//! The JSON path subset Trino's `json_extract`, `json_extract_scalar` and
//! `json_size` accept: `$`, then any run of `.key`, `[n]`, `["key"]` and
//! `['key']`, for example `$.items[0].name` or `$["a.b"][1]`.
//!
//! A path that does not start with `$` is read as if it were `$.` followed
//! by it (`items.0.name`), and a dotted numeric segment indexes an array
//! (`$.items.0`): both are what SQE accepted before brackets were parsed,
//! kept so existing queries do not change. A path that does not parse, or
//! that names a missing key or index, yields `None` (SQL NULL).

use serde_json::Value;

/// One step of a parsed path.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// `.key`: an object member, or an array index when the key is numeric.
    Dotted(String),
    /// `["key"]` / `['key']`: an object member only.
    Member(String),
    /// `[n]`: an array element only.
    Index(usize),
}

/// The value `path` names inside `value`, or `None`.
pub(crate) fn navigate<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for step in parse(path)? {
        current = match step {
            Step::Dotted(key) => match current {
                Value::Object(obj) => obj.get(&key)?,
                Value::Array(arr) => arr.get(key.parse::<usize>().ok()?)?,
                _ => return None,
            },
            Step::Member(key) => current.as_object()?.get(&key)?,
            Step::Index(idx) => current.as_array()?.get(idx)?,
        };
    }
    Some(current)
}

/// The steps of `path`, or `None` when it does not parse.
fn parse(path: &str) -> Option<Vec<Step>> {
    let path = path.trim();
    let rest = match path.strip_prefix('$') {
        Some(rest) => rest,
        // Legacy form without `$`: `a.b` reads as `$.a.b`.
        None if path.is_empty() => "",
        None => return parse(&format!("$.{path}")),
    };
    let chars: Vec<char> = rest.chars().collect();
    let mut steps = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '.' => {
                let start = i + 1;
                let mut end = start;
                while end < chars.len() && chars[end] != '.' && chars[end] != '[' {
                    end += 1;
                }
                let key: String = chars[start..end].iter().collect();
                let key = key.trim();
                // `$.` alone, or `a..b`: skip the empty segment, as before.
                if !key.is_empty() {
                    steps.push(Step::Dotted(key.to_string()));
                }
                i = end;
            }
            '[' => {
                let close = chars[i..].iter().position(|&c| c == ']')? + i;
                let inner: String = chars[i + 1..close].iter().collect();
                let inner = inner.trim();
                let quoted = inner.len() >= 2
                    && ((inner.starts_with('"') && inner.ends_with('"'))
                        || (inner.starts_with('\'') && inner.ends_with('\'')));
                if quoted {
                    steps.push(Step::Member(inner[1..inner.len() - 1].to_string()));
                } else {
                    steps.push(Step::Index(inner.parse().ok()?));
                }
                i = close + 1;
            }
            c if c.is_whitespace() => i += 1,
            _ => return None,
        }
    }
    Some(steps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn at(doc: &Value, path: &str) -> Option<Value> {
        navigate(doc, path).cloned()
    }

    #[test]
    fn root_and_dotted_members() {
        let doc = json!({"a": {"b": 1}});
        assert_eq!(at(&doc, "$"), Some(doc.clone()));
        assert_eq!(at(&doc, "$.a.b"), Some(json!(1)));
        assert_eq!(at(&doc, "$.a.missing"), None);
    }

    #[test]
    fn bracket_index_after_member() {
        let doc = json!({"items": [{"name": "x"}, {"name": "y"}]});
        assert_eq!(at(&doc, "$.items[0].name"), Some(json!("x")));
        assert_eq!(at(&doc, "$.items[1].name"), Some(json!("y")));
        assert_eq!(at(&doc, "$.items[2].name"), None);
    }

    #[test]
    fn bracket_index_at_root_and_nested() {
        let doc = json!([[1, 2], [3, 4]]);
        assert_eq!(at(&doc, "$[1][0]"), Some(json!(3)));
        assert_eq!(at(&doc, "$[ 0 ]"), Some(json!([1, 2])));
    }

    #[test]
    fn quoted_member_keeps_dots_and_spaces() {
        let doc = json!({"a.b": {"c d": true}});
        assert_eq!(at(&doc, r#"$["a.b"]["c d"]"#), Some(json!(true)));
        assert_eq!(at(&doc, "$['a.b']['c d']"), Some(json!(true)));
    }

    #[test]
    fn index_on_object_and_member_on_array_are_null() {
        let doc = json!({"o": {"0": 1}, "a": [1]});
        assert_eq!(at(&doc, "$.o[0]"), None);
        assert_eq!(at(&doc, r#"$.a["0"]"#), None);
    }

    #[test]
    fn legacy_forms_still_resolve() {
        let doc = json!({"items": [{"name": "x"}], "o": {"0": "zero"}});
        assert_eq!(at(&doc, "$.items.0.name"), Some(json!("x")));
        assert_eq!(at(&doc, "items.0.name"), Some(json!("x")));
        assert_eq!(at(&doc, "$.o.0"), Some(json!("zero")));
        assert_eq!(at(&doc, ""), Some(doc.clone()));
        assert_eq!(at(&doc, "$."), Some(doc.clone()));
    }

    #[test]
    fn malformed_paths_are_null() {
        let doc = json!({"a": [1]});
        assert_eq!(at(&doc, "$.a[0"), None);
        assert_eq!(at(&doc, "$.a[x]"), None);
        assert_eq!(at(&doc, "$.a[-1]"), None);
        assert_eq!(at(&doc, "$a"), None);
    }
}
