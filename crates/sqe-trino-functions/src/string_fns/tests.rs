use arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;

use super::*;

/// Run `sql`; the first column's Arrow type and every row rendered as text
/// (`NULL` for a null).
async fn run(sql: &str) -> (DataType, Vec<String>) {
    let ctx = crate::duckdb_test_ctx();
    crate::register_trino_functions(&ctx);
    crate::register_extended_trino_functions(&ctx);
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let dt = batches[0].column(0).data_type().clone();
    let mut rows = Vec::new();
    for b in &batches {
        let col = b.column(0);
        for i in 0..col.len() {
            rows.push(if col.is_null(i) {
                "NULL".to_string()
            } else {
                array_value_to_string(col, i).unwrap()
            });
        }
    }
    (dt, rows)
}

async fn one(sql: &str) -> String {
    let (_, rows) = run(sql).await;
    assert_eq!(rows.len(), 1, "{sql}");
    rows.into_iter().next().unwrap()
}

async fn err(sql: &str) -> String {
    let ctx = crate::duckdb_test_ctx();
    crate::register_trino_functions(&ctx);
    crate::register_extended_trino_functions(&ctx);
    let res = match ctx.sql(sql).await {
        Ok(df) => df.collect().await.map(|_| ()),
        Err(e) => Err(e),
    };
    res.expect_err(sql).to_string()
}

fn is_list_of(dt: &DataType, item: &DataType) -> bool {
    matches!(dt, DataType::List(f) if f.data_type() == item)
}

#[tokio::test]
async fn concat_arrays_and_strings() {
    let (dt, rows) = run("SELECT concat(make_array(1,2), make_array(3))").await;
    assert!(is_list_of(&dt, &DataType::Int64), "{dt:?}");
    assert_eq!(rows, ["[1, 2, 3]"]);
    assert_eq!(one("SELECT concat('ab', 'cd')").await, "abcd");
    let (_, rows) = run(
        "SELECT concat(a, make_array(9)) FROM (VALUES (make_array(1)), (make_array(2,3))) t(a)",
    )
    .await;
    assert_eq!(rows, ["[1, 9]", "[2, 3, 9]"]);
}

#[tokio::test]
async fn concat_ws_joins_strings_and_arrays() {
    assert_eq!(one("SELECT concat_ws('-', 'a', 'b')").await, "a-b");
    let (dt, rows) = run("SELECT concat_ws(',', make_array('a', NULL, 'b'))").await;
    assert_eq!(dt, DataType::Utf8);
    assert_eq!(rows, ["a,b"]);
    assert_eq!(
        one("SELECT concat_ws(',', make_array('a','b'))").await,
        "a,b"
    );
    // NULL separator or NULL array gives NULL.
    assert_eq!(
        one("SELECT concat_ws(CAST(NULL AS VARCHAR), make_array('a'))").await,
        "NULL"
    );
    let (_, rows) = run(
        "SELECT concat_ws(sep, a) FROM (VALUES (',', make_array('x','y')), ('|', make_array('z'))) t(sep, a)",
    )
    .await;
    assert_eq!(rows, ["x,y", "z"]);
}

#[tokio::test]
async fn reverse_arrays_and_strings() {
    let (dt, rows) = run("SELECT reverse(make_array(1,2,3))").await;
    assert!(is_list_of(&dt, &DataType::Int64), "{dt:?}");
    assert_eq!(rows, ["[3, 2, 1]"]);
    assert_eq!(one("SELECT reverse('abc')").await, "cba");
    let (_, rows) = run("SELECT reverse(s) FROM (VALUES ('ab'), (NULL), ('xyz')) t(s)").await;
    assert_eq!(rows, ["ba", "NULL", "zyx"]);
}

#[tokio::test]
async fn repeat_builds_an_array_of_any_type() {
    let (dt, rows) = run("SELECT repeat('ab', 3)").await;
    assert!(
        matches!(&dt, DataType::List(f) if matches!(f.data_type(), DataType::Utf8 | DataType::Utf8View)),
        "{dt:?}"
    );
    assert_eq!(rows, ["[ab, ab, ab]"]);
    let (dt, rows) = run("SELECT repeat(7, 2)").await;
    assert!(is_list_of(&dt, &DataType::Int64), "{dt:?}");
    assert_eq!(rows, ["[7, 7]"]);
    let (_, rows) = run("SELECT repeat(x, n) FROM (VALUES (1, 2), (3, 1)) t(x, n)").await;
    assert_eq!(rows, ["[1, 1]", "[3]"]);
    assert_eq!(one("SELECT repeat(1, NULL)").await, "NULL");
    let over = crate::scalar_fns::MAX_GENERATED_SIZE + 1;
    for sql in [
        format!("SELECT repeat(1, {over})"),
        format!("SELECT repeat('ab', {})", over / 2 + 1),
    ] {
        assert!(err(&sql).await.contains("exceeds the limit"), "{sql}");
    }
}

#[tokio::test]
async fn replace_two_argument_form_removes() {
    let (dt, rows) = run("SELECT replace('abcabc', 'b')").await;
    assert_eq!(dt, DataType::Utf8);
    assert_eq!(rows, ["acac"]);
    // The 3-argument form is the built-in.
    assert_eq!(one("SELECT replace('abcabc', 'b', 'X')").await, "aXcaXc");
    let (_, rows) = run("SELECT replace(s, 'a') FROM (VALUES ('aXa'), (NULL), ('b')) t(s)").await;
    assert_eq!(rows, ["X", "NULL", "b"]);
}

#[tokio::test]
async fn split_part_past_the_end_is_null() {
    let (dt, rows) = run("SELECT split_part('a,b,c', ',', 2)").await;
    assert_eq!(dt, DataType::Utf8);
    assert_eq!(rows, ["b"]);
    assert_eq!(one("SELECT split_part('a,b,c', ',', 4)").await, "NULL");
    assert_eq!(one("SELECT split_part('a,b,c', ',', NULL)").await, "NULL");
    assert!(err("SELECT split_part('a,b', ',', 0)")
        .await
        .contains("greater than zero"));
    assert!(err("SELECT split_part('a,b', ',', -1)")
        .await
        .contains("greater than zero"));
    let (_, rows) =
        run("SELECT split_part(s, ',', 2) FROM (VALUES ('a,b'), ('a'), (NULL)) t(s)").await;
    assert_eq!(rows, ["b", "NULL", "NULL"]);
}

#[tokio::test]
async fn strpos_family_returns_bigint() {
    let (dt, rows) = run("SELECT strpos('hello', 'l')").await;
    assert_eq!(dt, DataType::Int64);
    assert_eq!(rows, ["3"]);
    assert_eq!(one("SELECT strpos('hello', 'z')").await, "0");
    assert_eq!(one("SELECT strpos('héllo', 'l')").await, "3");
    // 3-argument form: the N-th occurrence, negative counts from the end.
    let (dt, rows) = run("SELECT strpos('hello', 'l', 2)").await;
    assert_eq!(dt, DataType::Int64);
    assert_eq!(rows, ["4"]);
    assert_eq!(one("SELECT strpos('hello', 'l', -1)").await, "4");
    assert_eq!(one("SELECT strpos('hello', 'l', -2)").await, "3");
    assert_eq!(one("SELECT strpos('hello', 'l', 3)").await, "0");
    assert_eq!(one("SELECT strpos('hello', 'l', -3)").await, "0");
    assert!(err("SELECT strpos('hello', 'l', 0)")
        .await
        .contains("instance"));
    assert_eq!(one("SELECT strpos(NULL, 'l')").await, "NULL");
    // Aliases.
    for f in ["index", "instr", "position"] {
        let (dt, rows) = run(&format!("SELECT {f}('abc', 'b')")).await;
        assert_eq!(dt, DataType::Int64, "{f}");
        assert_eq!(rows, ["2"], "{f}");
    }
    let (_, rows) = run("SELECT strpos(s, 'l') FROM (VALUES ('hello'), (NULL), ('xl')) t(s)").await;
    assert_eq!(rows, ["3", "NULL", "2"]);
}

#[tokio::test]
async fn length_and_levenshtein_return_bigint() {
    for f in ["length", "char_length", "character_length"] {
        let (dt, rows) = run(&format!("SELECT {f}('héllo')")).await;
        assert_eq!(dt, DataType::Int64, "{f}");
        assert_eq!(rows, ["5"], "{f}");
    }
    let (_, rows) = run("SELECT length(s) FROM (VALUES ('ab'), (NULL), ('')) t(s)").await;
    assert_eq!(rows, ["2", "NULL", "0"]);
    let (dt, rows) = run("SELECT levenshtein_distance('kitten', 'sitting')").await;
    assert_eq!(dt, DataType::Int64);
    assert_eq!(rows, ["3"]);
    let (_, rows) = run(
        "SELECT levenshtein_distance(a, b) FROM (VALUES ('a', 'b'), ('abc', 'abc'), (NULL, 'x')) t(a, b)",
    )
    .await;
    assert_eq!(rows, ["1", "0", "NULL"]);
}

#[tokio::test]
async fn hamming_distance_is_bigint() {
    let (dt, rows) = run("SELECT hamming_distance('karolin', 'kathrin')").await;
    assert_eq!(dt, DataType::Int64);
    assert_eq!(rows, ["3"]);
}

#[tokio::test]
async fn soundex_is_standard_american() {
    let (dt, rows) = run("SELECT soundex('Robert')").await;
    assert_eq!(dt, DataType::Utf8);
    assert_eq!(rows, ["R163"]);
    assert_eq!(one("SELECT soundex('Tymczak')").await, "T522");
    assert_eq!(one("SELECT soundex('Pfister')").await, "P236");
    assert_eq!(one("SELECT soundex('Ashcraft')").await, "A261");
    assert_eq!(one("SELECT soundex('A')").await, "A000");
    assert_eq!(one("SELECT soundex('')").await, "");
    let (_, rows) = run("SELECT soundex(s) FROM (VALUES ('Rupert'), (NULL)) t(s)").await;
    assert_eq!(rows, ["R163", "NULL"]);
}

#[tokio::test]
async fn normalize_defaults_to_nfc() {
    let (dt, rows) = run("SELECT length(normalize('e\u{301}'))").await;
    assert_eq!(dt, DataType::Int64);
    assert_eq!(rows, ["1"]);
    let (dt, rows) = run("SELECT length(normalize('\u{e9}', 'NFD'))").await;
    assert_eq!(dt, DataType::Int64);
    assert_eq!(rows, ["2"]);
    assert_eq!(one("SELECT normalize('\u{fb01}', 'NFKC')").await, "fi");
    assert!(err("SELECT normalize('a', 'NOPE')").await.contains("NFC"));
    assert_eq!(one("SELECT normalize(CAST(NULL AS VARCHAR))").await, "NULL");
    let (_, rows) = run("SELECT normalize(s) FROM (VALUES ('abc'), (NULL)) t(s)").await;
    assert_eq!(rows, ["abc", "NULL"]);
}

#[tokio::test]
async fn human_readable_seconds_rounds_and_pluralises() {
    let (dt, rows) = run("SELECT human_readable_seconds(96)").await;
    assert_eq!(dt, DataType::Utf8);
    assert_eq!(rows, ["1 minute, 36 seconds"]);
    assert_eq!(
        one("SELECT human_readable_seconds(3762)").await,
        "1 hour, 2 minutes, 42 seconds"
    );
    assert_eq!(
        one("SELECT human_readable_seconds(56363463)").await,
        "93 weeks, 1 day, 8 hours, 31 minutes, 3 seconds"
    );
    assert_eq!(one("SELECT human_readable_seconds(0)").await, "0 seconds");
    assert_eq!(one("SELECT human_readable_seconds(1)").await, "1 second");
    assert_eq!(one("SELECT human_readable_seconds(59.6)").await, "1 minute");
    assert_eq!(one("SELECT human_readable_seconds(NULL)").await, "NULL");
    let (_, rows) =
        run("SELECT human_readable_seconds(x) FROM (VALUES (60.0), (NULL), (3600.0)) t(x)").await;
    assert_eq!(rows, ["1 minute", "NULL", "1 hour"]);
}

#[tokio::test]
async fn luhn_check_validates_digit_strings() {
    let (dt, rows) = run("SELECT luhn_check('79927398713')").await;
    assert_eq!(dt, DataType::Boolean);
    assert_eq!(rows, ["true"]);
    assert_eq!(one("SELECT luhn_check('79927398710')").await, "false");
    assert_eq!(one("SELECT luhn_check('7992 7398')").await, "NULL");
    assert_eq!(one("SELECT luhn_check(NULL)").await, "NULL");
    let (_, rows) =
        run("SELECT luhn_check(s) FROM (VALUES ('4539578763621486'), ('1234'), (NULL)) t(s)").await;
    assert_eq!(rows, ["true", "false", "NULL"]);
}

#[tokio::test]
async fn format_number_uses_unit_suffixes() {
    let (dt, rows) = run("SELECT format_number(1234)").await;
    assert_eq!(dt, DataType::Utf8);
    assert_eq!(rows, ["1.23K"]);
    assert_eq!(one("SELECT format_number(123)").await, "123");
    assert_eq!(one("SELECT format_number(1000000)").await, "1M");
    assert_eq!(one("SELECT format_number(123456)").await, "123K");
    assert_eq!(one("SELECT format_number(12345678)").await, "12.35M");
    assert_eq!(one("SELECT format_number(999999)").await, "1M");
    assert_eq!(one("SELECT format_number(1234.5)").await, "1.23K");
    assert_eq!(one("SELECT format_number(NULL)").await, "NULL");
    let (_, rows) = run("SELECT format_number(x) FROM (VALUES (1500), (NULL), (7)) t(x)").await;
    assert_eq!(rows, ["1.5K", "NULL", "7"]);
}

#[tokio::test]
async fn parse_data_size_is_decimal_38_0() {
    let (dt, rows) = run("SELECT parse_data_size('2.3MB')").await;
    assert_eq!(dt, DataType::Decimal128(38, 0));
    assert_eq!(rows, ["2411724"]);
    assert_eq!(one("SELECT parse_data_size('1kB')").await, "1024");
    assert_eq!(one("SELECT parse_data_size('7B')").await, "7");
    assert_eq!(
        one("SELECT parse_data_size('1YB')").await,
        (1u128 << 80).to_string()
    );
    assert_eq!(one("SELECT parse_data_size(NULL)").await, "NULL");
    assert!(err("SELECT parse_data_size('12 parsecs')")
        .await
        .contains("Invalid data size"));
    assert!(err("SELECT parse_data_size('MB')")
        .await
        .contains("Invalid"));
    let (dt, rows) =
        run("SELECT parse_data_size(s) FROM (VALUES ('1GB'), (NULL), ('2kB')) t(s)").await;
    assert_eq!(dt, DataType::Decimal128(38, 0));
    assert_eq!(rows, ["1073741824", "NULL", "2048"]);
}

#[tokio::test]
async fn translate_is_the_builtin() {
    // Same semantics as Trino: characters are mapped one to one, extras dropped.
    assert_eq!(one("SELECT translate('abcd', 'ac', 'z')").await, "zbd");
}

#[tokio::test]
async fn sql_position_syntax_still_plans() {
    // `position(x IN y)` is planned by DataFusion's own expr planner straight to
    // the built-in strpos, so it keeps its Int32 type: known gap.
    let (_, rows) = run("SELECT position('b' IN 'abc')").await;
    assert_eq!(rows, ["2"]);
}

#[test]
fn data_size_arithmetic_has_no_float_error() {
    assert_eq!(parse_data_size_of("0.1kB").unwrap(), 102);
    assert_eq!(parse_data_size_of("1.5GB").unwrap(), 1_610_612_736);
    assert!(parse_data_size_of("99999999999999999999999YB").is_err());
}

#[tokio::test]
async fn concat_with_an_element_prepends_or_appends() {
    let (dt, rows) = run("SELECT concat(7, make_array(1, 2))").await;
    assert!(is_list_of(&dt, &DataType::Int64), "{dt:?}");
    assert_eq!(rows, ["[7, 1, 2]"]);
    assert_eq!(one("SELECT concat(make_array(1), 2)").await, "[1, 2]");
    assert_eq!(
        one("SELECT concat(make_array(1), 2, make_array(3, 4), 5)").await,
        "[1, 2, 3, 4, 5]"
    );
    assert_eq!(one("SELECT concat(make_array('a'), 'b')").await, "[a, b]");
    // A NULL array gives NULL; a typed NULL element is kept as an element.
    assert_eq!(
        one("SELECT concat(make_array(1), CAST(NULL AS BIGINT[]))").await,
        "NULL"
    );
    assert_eq!(
        one("SELECT cardinality(concat(make_array(1), CAST(NULL AS BIGINT)))").await,
        "2"
    );
    let (_, rows) = run(
        "SELECT concat(a, x) FROM (VALUES (make_array(1), 8), (make_array(2, 3), 9), (CAST(NULL AS BIGINT[]), 1)) t(a, x)",
    )
    .await;
    assert_eq!(rows, ["[1, 8]", "[2, 3, 9]", "NULL"]);
    assert!(err("SELECT concat(make_array(1), 'x')")
        .await
        .contains("concat"));
}

#[tokio::test]
async fn title_case_capitalises_each_word() {
    let (dt, rows) = run("SELECT title_case('hello wORLD-foo_bar 2nd')").await;
    assert_eq!(dt, DataType::Utf8);
    assert_eq!(rows, ["Hello World-Foo_Bar 2nd"]);
    assert_eq!(one("SELECT title_case('')").await, "");
    assert_eq!(one("SELECT title_case('élan vital')").await, "Élan Vital");
    let (_, rows) = run("SELECT title_case(s) FROM (VALUES ('a b'), (NULL)) t(s)").await;
    assert_eq!(rows, ["A B", "NULL"]);
}

#[tokio::test]
async fn char2hexint_is_utf16be_hex() {
    let (dt, rows) = run("SELECT char2hexint('abc')").await;
    assert_eq!(dt, DataType::Utf8);
    assert_eq!(rows, ["006100620063"]);
    assert_eq!(one("SELECT char2hexint('ü')").await, "00FC");
    // Outside the BMP: a surrogate pair.
    assert_eq!(one("SELECT char2hexint('😀')").await, "D83DDE00");
    assert_eq!(one("SELECT char2hexint('')").await, "");
    let (_, rows) = run("SELECT char2hexint(s) FROM (VALUES ('a'), (NULL)) t(s)").await;
    assert_eq!(rows, ["0061", "NULL"]);
}

#[tokio::test]
async fn position_and_extract_return_bigint() {
    let (dt, rows) = run("SELECT position('b' IN 'abc')").await;
    assert_eq!(dt, DataType::Int64);
    assert_eq!(rows, ["2"]);
    assert_eq!(one("SELECT position('z' IN 'abc')").await, "0");
    let (_, rows) = run("SELECT position('a' IN s) FROM (VALUES ('xa'), (NULL)) t(s)").await;
    assert_eq!(rows, ["2", "NULL"]);
    for field in [
        "YEAR",
        "QUARTER",
        "MONTH",
        "WEEK",
        "DAY",
        "DOW",
        "DOY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "MILLISECOND",
    ] {
        let (dt, _) = run(&format!(
            "SELECT extract({field} FROM TIMESTAMP '2024-03-05 06:07:08.123')"
        ))
        .await;
        assert_eq!(dt, DataType::Int64, "{field}");
    }
    assert_eq!(
        one("SELECT extract(YEAR FROM DATE '2024-03-05')").await,
        "2024"
    );
    assert_eq!(
        one("SELECT extract(SECOND FROM TIMESTAMP '2024-03-05 06:07:08.123')").await,
        "8"
    );
    let (_, rows) = run(
        "SELECT extract(MONTH FROM d) FROM (VALUES (DATE '2024-03-05'), (CAST(NULL AS DATE))) t(d)",
    )
    .await;
    assert_eq!(rows, ["3", "NULL"]);
}
