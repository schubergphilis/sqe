//! Pre-parser for Hive-style `ALTER TABLE ... ADD/DROP PARTITION` on
//! external tables.
//!
//! ```sql
//! ALTER TABLE lake.sales.orders ADD IF NOT EXISTS PARTITION (dt = '2026-01-02') LOCATION 's3://raw/orders/dt=2026-01-02/'
//! ALTER TABLE lake.sales.orders DROP PARTITION (dt = '2026-01-02')
//! ALTER TABLE lake.sales.orders DROP PARTITION (dt = '2026-01-02') PURGE
//! ```
//!
//! sqlparser-rs 0.62 parses the bare `ADD/DROP PARTITION (k = v)` forms into
//! `AlterTableOperation::{AddPartitions, DropPartitions}`, but two things it
//! cannot express matter here:
//!
//! 1. `LOCATION '<url>'` after a partition spec. sqlparser's ADD loop stops
//!    at the first non-`PARTITION` token, so the LOCATION form dies with
//!    `Expected: end of statement, found: LOCATION`. The location is the
//!    whole point of an out-of-prefix partition entry (design D1), so this
//!    module pre-parses the whole statement instead of post-processing the
//!    sqlparser AST.
//! 2. `PURGE` after DROP PARTITION. Same parse failure. It must be
//!    *recognized* so the coordinator can reject it naming cycle 4, rather
//!    than surfacing a baffling parser error.
//!
//! Tokenization goes through sqlparser's own tokenizer, so quoted
//! identifiers, string escapes, and comments behave exactly as they would
//! if sqlparser had parsed the statement.

use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use sqe_core::{Result, SqeError};

/// A parsed `ALTER TABLE ... ADD/DROP PARTITION` statement ready for the
/// coordinator to translate into manifest index maintenance.
#[derive(Debug, Clone, PartialEq)]
pub struct HivePartitionDdl {
    /// The raw (possibly dotted/quoted) table reference.
    pub table: String,
    pub action: HivePartitionAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HivePartitionAction {
    Add {
        if_not_exists: bool,
        partitions: Vec<HivePartitionSpec>,
    },
    Drop {
        if_exists: bool,
        partitions: Vec<HivePartitionSpec>,
        /// `DROP PARTITION ... PURGE`. Never executed: the coordinator
        /// rejects it naming cycle 4, because cycle 2 never touches data.
        /// Parsed here so the rejection is precise instead of a parser
        /// error.
        purge: bool,
    },
}

/// One `PARTITION (k = 'v', ...)` group plus its optional `LOCATION`.
#[derive(Debug, Clone, PartialEq)]
pub struct HivePartitionSpec {
    /// (key, value) pairs in declaration order. Values are unquoted text;
    /// typing against the declared partition-key types happens at the
    /// manifest layer, which owns that rule.
    pub values: Vec<(String, String)>,
    pub location: Option<String>,
}

/// Try to parse one of the statements above. Returns `Ok(None)` when the
/// input does not look like one of them, so the classifier can fall through
/// to the next pre-parser (`PARTITION FIELD`, tags, masking policies, ...).
pub fn try_parse_hive_partition_ddl(sql: &str) -> Result<Option<HivePartitionDdl>> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("ALTER TABLE ") {
        return Ok(None);
    }

    // sqlparser's tokenizer emits Whitespace tokens between everything
    // significant; none of this grammar cares about spacing, so drop them
    // once here instead of skipping them at every step of the walk below.
    let tokens: Vec<Token> = Tokenizer::new(&GenericDialect {}, trimmed)
        .tokenize()
        .map_err(|e| SqeError::Execution(format!("failed to tokenize ALTER TABLE: {e}")))?
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();

    let mut i = 0usize;
    // Skip ALTER TABLE.
    for expect in ["ALTER", "TABLE"] {
        match tokens.get(i) {
            Some(Token::Word(w)) if w.value.eq_ignore_ascii_case(expect) => i += 1,
            _ => return Ok(None),
        }
    }

    // Table reference: every token up to ADD or DROP. Dotted refs arrive as
    // Word Period Word; quoted idents arrive as quoted Words. Reconstructed
    // with the token's own text so quoting survives for the handler.
    let table_start = i;
    while let Some(tok) = tokens.get(i) {
        match tok {
            Token::Word(w) if w.value.eq_ignore_ascii_case("ADD") => break,
            Token::Word(w) if w.value.eq_ignore_ascii_case("DROP") => break,
            _ => i += 1,
        }
    }
    if i == table_start {
        return Ok(None);
    }
    let table = tokens[table_start..i]
        .iter()
        .map(token_text)
        .collect::<Vec<_>>()
        .join("");

    let action = match tokens.get(i) {
        Some(Token::Word(w)) if w.value.eq_ignore_ascii_case("ADD") => {
            i += 1;
            let if_not_exists = strip_if_not_exists(&tokens, &mut i);
            let mut partitions = Vec::new();
            loop {
                // Only claim `PARTITION (`; `PARTITION FIELD` belongs to the
                // Iceberg partition-evolution pre-parser and must fall
                // through untouched.
                if !peek_keyword(&tokens, i, "PARTITION")
                    || !matches!(tokens.get(i + 1), Some(Token::LParen))
                {
                    break;
                }
                i += 1;
                let values = parse_spec_values(&tokens, &mut i)?;
                let location = if peek_keyword(&tokens, i, "LOCATION") {
                    i += 1;
                    match tokens.get(i) {
                        Some(Token::SingleQuotedString(s)) => {
                            i += 1;
                            Some(s.clone())
                        }
                        other => {
                            return Err(SqeError::Execution(format!(
                                "ADD PARTITION LOCATION must be a single-quoted string, got {:?}",
                                other.map(describe_token)
                            )))
                        }
                    }
                } else {
                    None
                };
                partitions.push(HivePartitionSpec { values, location });
                if matches!(tokens.get(i), Some(Token::Comma)) {
                    i += 1;
                } else {
                    break;
                }
            }
            if partitions.is_empty() {
                // Not ours: `ADD COLUMN`, `ADD CONSTRAINT`, ... falls back to
                // sqlparser and the existing dispatch.
                return Ok(None);
            }
            expect_end(&tokens, i)?;
            HivePartitionAction::Add {
                if_not_exists,
                partitions,
            }
        }
        Some(Token::Word(w)) if w.value.eq_ignore_ascii_case("DROP") => {
            i += 1;
            let if_exists = strip_if_exists(&tokens, &mut i);
            // Same FIELD guard as ADD: `DROP PARTITION FIELD <col>` is the
            // evolution parser's statement, not ours.
            if !peek_keyword(&tokens, i, "PARTITION")
                || !matches!(tokens.get(i + 1), Some(Token::LParen))
            {
                return Ok(None);
            }
            i += 1;
            let values = parse_spec_values(&tokens, &mut i)?;
            let purge = peek_keyword(&tokens, i, "PURGE");
            if purge {
                i += 1;
            }
            expect_end(&tokens, i)?;
            HivePartitionAction::Drop {
                if_exists,
                partitions: vec![HivePartitionSpec {
                    values,
                    location: None,
                }],
                purge,
            }
        }
        _ => return Ok(None),
    };

    Ok(Some(HivePartitionDdl { table, action }))
}

fn token_text(t: &Token) -> String {
    match t {
        Token::Period => ".".to_string(),
        Token::Word(w) => w.to_string(),
        other => other.to_string(),
    }
}

fn describe_token(t: &Token) -> String {
    token_text(t)
}

fn peek_keyword(tokens: &[Token], i: usize, kw: &str) -> bool {
    matches!(tokens.get(i), Some(Token::Word(w)) if w.value.eq_ignore_ascii_case(kw))
}

fn strip_if_not_exists(tokens: &[Token], i: &mut usize) -> bool {
    let hit = peek_keyword(tokens, *i, "IF")
        && peek_keyword(tokens, *i + 1, "NOT")
        && peek_keyword(tokens, *i + 2, "EXISTS");
    if hit {
        *i += 3;
    }
    hit
}

fn strip_if_exists(tokens: &[Token], i: &mut usize) -> bool {
    let hit = peek_keyword(tokens, *i, "IF") && peek_keyword(tokens, *i + 1, "EXISTS");
    if hit {
        *i += 2;
    }
    hit
}

/// `( k = v [, k = v] )` inside the parentheses after `PARTITION`.
fn parse_spec_values(tokens: &[Token], i: &mut usize) -> Result<Vec<(String, String)>> {
    match tokens.get(*i) {
        Some(Token::LParen) => *i += 1,
        other => {
            return Err(SqeError::Execution(format!(
                "expected `(` after PARTITION, got {:?}",
                other.map(describe_token)
            )))
        }
    }

    let mut values = Vec::new();
    loop {
        let key = match tokens.get(*i) {
            Some(Token::Word(w)) => {
                *i += 1;
                w.value.clone()
            }
            other => {
                return Err(SqeError::Execution(format!(
                    "expected a partition key name, got {:?}",
                    other.map(describe_token)
                )))
            }
        };
        match tokens.get(*i) {
            Some(Token::Eq) => *i += 1,
            other => {
                return Err(SqeError::Execution(format!(
                    "expected `=` after partition key `{key}`, got {:?}",
                    other.map(describe_token)
                )))
            }
        };
        let value = match tokens.get(*i) {
            Some(Token::SingleQuotedString(s)) | Some(Token::DoubleQuotedString(s)) => {
                let v = s.clone();
                *i += 1;
                v
            }
            Some(Token::Number(n, _)) => assemble_composite_value(tokens, i, n.clone()),
            Some(Token::Word(w)) => assemble_composite_value(tokens, i, w.value.clone()),
            // Negative number: `-` followed by digits.
            Some(Token::Minus) => {
                *i += 1;
                match tokens.get(*i) {
                    Some(Token::Number(n, _)) => {
                        assemble_composite_value(tokens, i, format!("-{n}"))
                    }
                    other => {
                        return Err(SqeError::Execution(format!(
                            "expected a number after `-`, got {:?}",
                            other.map(describe_token)
                        )))
                    }
                }
            }
            other => {
                return Err(SqeError::Execution(format!(
                    "expected a partition value for `{key}`, got {:?}",
                    other.map(describe_token)
                )))
            }
        };
        values.push((key, value));

        match tokens.get(*i) {
            Some(Token::Comma) => *i += 1,
            Some(Token::RParen) => {
                *i += 1;
                break;
            }
            other => {
                return Err(SqeError::Execution(format!(
                    "expected `,` or `)` in PARTITION spec, got {:?}",
                    other.map(describe_token)
                )))
            }
        }
    }
    Ok(values)
}

fn expect_end(tokens: &[Token], i: usize) -> Result<()> {
    match tokens.get(i) {
        None => Ok(()),
        Some(tok) => Err(SqeError::Execution(format!(
            "unexpected trailing token in ALTER TABLE ... PARTITION: {}",
            describe_token(tok)
        ))),
    }
}

/// Assemble an unquoted composite value. A bare date-like `2026-01-02`
/// tokenizes as Number Minus Number Minus Number, so after the first piece
/// keep joining `-`/`.`-separated number/word pieces. Only joins when a
/// piece actually follows, so grammar punctuation (`)` `,`) is never
/// swallowed.
fn assemble_composite_value(tokens: &[Token], i: &mut usize, first: String) -> String {
    let mut v = first;
    *i += 1; // consume the first piece
    loop {
        let join = match tokens.get(*i) {
            Some(Token::Minus) => '-',
            Some(Token::Period) => '.',
            _ => break,
        };
        let piece = match tokens.get(*i + 1) {
            Some(Token::Number(n, _)) => n.clone(),
            Some(Token::Word(w)) => w.value.clone(),
            _ => break,
        };
        v.push(join);
        v.push_str(&piece);
        *i += 2;
    }
    v
}
