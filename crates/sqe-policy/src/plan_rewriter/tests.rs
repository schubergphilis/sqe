use super::*;
use sqe_core::config::MaskPrecedence;

// ── summarise_policies (audit summary) tests ──────────────────────────────

#[test]
fn summary_counts_masks_filters_and_restrictions() {
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    policy
        .column_masks
        .insert("ssn".to_string(), MaskType::Hash);
    policy
        .column_masks
        .insert("salary".to_string(), MaskType::Nullify);
    policy.restricted_columns.push("notes".to_string());
    let mut table_policies = HashMap::new();
    table_policies.insert("sales.orders".to_string(), policy);

    let s = summarise_policies(&table_policies);
    assert_eq!(s.row_filters_applied, 1);
    assert_eq!(
        s.columns_masked,
        vec!["salary".to_string(), "ssn".to_string()]
    );
    assert_eq!(s.columns_restricted, vec!["notes".to_string()]);
    assert!(!s.denied, "a real row filter is not a deny");
    // Review 610: lineage needs the scan each mask applied to, and a
    // restricted column counts as masked (its value is NULL, not copied).
    assert_eq!(
        s.masked_fields,
        vec![
            ("sales.orders".to_string(), "notes".to_string()),
            ("sales.orders".to_string(), "salary".to_string()),
            ("sales.orders".to_string(), "ssn".to_string()),
        ]
    );
}

#[test]
fn summary_marks_denied_on_deny_all_filter() {
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(lit(false));
    let mut table_policies = HashMap::new();
    table_policies.insert("sales.orders".to_string(), policy);

    let s = summarise_policies(&table_policies);
    assert!(s.denied, "lit(false) deny-all must set denied=true");
    assert_eq!(
        s.row_filters_applied, 0,
        "the deny-all sentinel must not be counted as a real row filter"
    );
}

#[test]
fn summary_default_when_no_policies() {
    let table_policies: HashMap<String, ResolvedPolicy> = HashMap::new();
    assert_eq!(
        summarise_policies(&table_policies),
        PolicySummary::default()
    );
}

// ── merge_tag_masks precedence tests ──────────────────────────────────────

fn make_col_tags(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
    pairs
        .iter()
        .map(|(col, tags)| {
            (
                col.to_string(),
                tags.iter().map(|t| t.to_string()).collect(),
            )
        })
        .collect()
}

fn make_tag_masks(pairs: &[(&str, MaskType)]) -> HashMap<String, TagMaskSpec> {
    pairs
        .iter()
        .map(|(t, m)| (t.to_string(), TagMaskSpec::Ready(m.clone())))
        .collect()
}

fn no_unmappable() -> HashSet<String> {
    HashSet::new()
}

fn default_identity() -> SessionIdentity {
    SessionIdentity {
        username: "test_user".to_string(),
        roles: vec![],
        database: None,
        schema: None,
    }
}

#[test]
fn merge_tag_masks_applies_tag_mask_to_unmasked_column() {
    let mut policy = ResolvedPolicy::default();
    let col_tags = make_col_tags(&[("email", &["PII"])]);
    let tag_masks = make_tag_masks(&[("PII", MaskType::Nullify)]);
    merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Tag,
    );
    assert!(
        matches!(policy.column_masks.get("email"), Some(MaskType::Nullify)),
        "tag mask must be applied when no resource mask exists"
    );
}

#[test]
fn merge_tag_masks_resource_mask_wins_under_resource_precedence() {
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("email".to_string(), MaskType::Hash);
    let col_tags = make_col_tags(&[("email", &["PII"])]);
    let tag_masks = make_tag_masks(&[("PII", MaskType::Nullify)]);
    merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Resource,
    );
    assert!(
        matches!(policy.column_masks.get("email"), Some(MaskType::Hash)),
        "under Resource precedence the named-column rule must survive the tag rule"
    );
}

/// The default, and the reason this knob exists: Hive and Spark/Kyuubi let
/// the tag rule win, so SQE must too or the same Ranger policy set renders
/// different values in the two engines.
#[test]
fn merge_tag_masks_tag_mask_wins_under_tag_precedence() {
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("email".to_string(), MaskType::Hash);
    let col_tags = make_col_tags(&[("email", &["PII"])]);
    let tag_masks = make_tag_masks(&[("PII", MaskType::Nullify)]);
    merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Tag,
    );
    assert!(
        matches!(policy.column_masks.get("email"), Some(MaskType::Nullify)),
        "under Tag precedence the tag rule must overwrite the resource mask"
    );
}

/// A malformed CUSTOM tag template is a defect in the TAG rule. It must not
/// cost the column the resource mask that is working fine, which `Tag`
/// precedence makes reachable for the first time.
#[test]
fn merge_tag_masks_broken_custom_template_keeps_the_resource_mask() {
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("ssn".to_string(), MaskType::Nullify);
    let col_tags = make_col_tags(&[("ssn", &["PII"])]);
    let mut tag_masks = HashMap::new();
    tag_masks.insert(
        "PII".to_string(),
        TagMaskSpec::Custom("this is not( a valid expression".to_string()),
    );
    merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Tag,
    );
    assert!(
        matches!(policy.column_masks.get("ssn"), Some(MaskType::Nullify)),
        "the working resource mask must survive a broken tag template"
    );
    assert!(
        !policy.restricted_columns.contains(&"ssn".to_string()),
        "a broken tag template must not restrict an already-masked column"
    );
}

/// With no resource mask to fall back on, the same broken template must still
/// fail closed rather than return the column raw.
#[test]
fn merge_tag_masks_broken_custom_template_restricts_an_unmasked_column() {
    let mut policy = ResolvedPolicy::default();
    let col_tags = make_col_tags(&[("ssn", &["PII"])]);
    let mut tag_masks = HashMap::new();
    tag_masks.insert(
        "PII".to_string(),
        TagMaskSpec::Custom("this is not( a valid expression".to_string()),
    );
    merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Tag,
    );
    assert!(
        policy.restricted_columns.contains(&"ssn".to_string()),
        "an unprotected column with a broken tag mask must be restricted"
    );
}

/// Whichever way precedence points, the column must never come back raw and
/// must never be dropped instead of masked. Restriction is strictly worse
/// than a working mask, and raw is a leak.
#[test]
fn merge_tag_masks_never_leaves_a_contested_column_raw_or_restricted() {
    for precedence in [MaskPrecedence::Tag, MaskPrecedence::Resource] {
        let mut policy = ResolvedPolicy::default();
        policy
            .column_masks
            .insert("email".to_string(), MaskType::Hash);
        let col_tags = make_col_tags(&[("email", &["PII"])]);
        // The tag is unmappable: no spec for it, and it is flagged.
        let mut unmappable = HashSet::new();
        unmappable.insert("PII".to_string());
        merge_tag_masks(
            &mut policy,
            &col_tags,
            &make_tag_masks(&[]),
            vec![],
            &unmappable,
            &default_identity(),
            precedence,
        );
        assert!(
            policy.column_masks.contains_key("email"),
            "{precedence:?}: an unmappable tag must not strip the resource mask"
        );
        assert!(
            !policy.restricted_columns.contains(&"email".to_string()),
            "{precedence:?}: a column with a working resource mask must not be restricted"
        );
    }
}

#[test]
fn merge_tag_masks_restricted_column_stays_restricted() {
    let mut policy = ResolvedPolicy::default();
    policy.restricted_columns.push("ssn".to_string());
    let col_tags = make_col_tags(&[("ssn", &["PII"])]);
    let tag_masks = make_tag_masks(&[("PII", MaskType::Nullify)]);
    merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Tag,
    );
    // No mask added; restricted stays restricted.
    assert!(
        !policy.column_masks.contains_key("ssn"),
        "restricted column must not gain a mask from tags"
    );
    assert!(
        policy.restricted_columns.contains(&"ssn".to_string()),
        "column must remain in restricted_columns"
    );
}

/// Merge `tag_filters` over `resource_filters` for a table with one tagged
/// column, returning the conflicts and the resulting row filters.
fn merge_row_filters(
    resource_filters: Vec<Expr>,
    tag_filters: Vec<Expr>,
    precedence: MaskPrecedence,
) -> (Vec<PolicyConflict>, Vec<Expr>) {
    let mut policy = ResolvedPolicy {
        row_filters: resource_filters,
        ..Default::default()
    };
    let col_tags = make_col_tags(&[("region", &["RESTRICTED"])]);
    let conflicts = merge_tag_masks(
        &mut policy,
        &col_tags,
        &HashMap::new(),
        tag_filters,
        &no_unmappable(),
        &default_identity(),
        precedence,
    );
    (conflicts, policy.row_filters)
}

fn eu() -> Expr {
    datafusion::logical_expr::col("region").eq(lit("EU"))
}

fn id_over_one() -> Expr {
    datafusion::logical_expr::col("id").gt(lit(1))
}

const ALL_PRECEDENCES: [MaskPrecedence; 3] = [
    MaskPrecedence::Tag,
    MaskPrecedence::Resource,
    MaskPrecedence::Refuse,
];

/// TA-13: a tag row filter and a resource row filter are refused, not
/// ANDed, whatever the mask precedence.
#[test]
fn a_tag_and_a_resource_row_filter_conflict_under_every_precedence() {
    for precedence in ALL_PRECEDENCES {
        let (conflicts, filters) = merge_row_filters(vec![id_over_one()], vec![eu()], precedence);
        assert_eq!(conflicts, vec![PolicyConflict::RowFilter], "{precedence:?}");
        // Both stay in the policy: a caller that ignores the conflict
        // still reads the most restrictive combination.
        assert_eq!(filters, vec![id_over_one(), eu()], "{precedence:?}");
    }
}

#[test]
fn two_tag_row_filters_conflict_under_every_precedence() {
    for precedence in ALL_PRECEDENCES {
        let (conflicts, _) = merge_row_filters(vec![], vec![eu(), id_over_one()], precedence);
        assert_eq!(conflicts, vec![PolicyConflict::RowFilter], "{precedence:?}");
    }
}

#[test]
fn the_same_row_filter_from_two_sources_is_one_filter() {
    for precedence in ALL_PRECEDENCES {
        let (conflicts, filters) = merge_row_filters(vec![eu()], vec![eu(), eu()], precedence);
        assert!(conflicts.is_empty(), "{precedence:?}: {conflicts:?}");
        assert_eq!(filters, vec![eu()], "{precedence:?}");
    }
}

#[test]
fn a_tag_row_filter_alone_applies() {
    for precedence in ALL_PRECEDENCES {
        let (conflicts, filters) = merge_row_filters(vec![], vec![eu()], precedence);
        assert!(conflicts.is_empty(), "{precedence:?}: {conflicts:?}");
        assert_eq!(filters, vec![eu()], "{precedence:?}");
    }
}

/// The deny-all sentinel is not a filter a policy chose: it never makes a
/// conflict, and it is kept.
#[test]
fn the_deny_all_sentinel_is_not_counted() {
    for precedence in ALL_PRECEDENCES {
        let (conflicts, filters) = merge_row_filters(vec![lit(false)], vec![eu()], precedence);
        assert!(conflicts.is_empty(), "{precedence:?}: {conflicts:?}");
        assert_eq!(filters, vec![lit(false), eu()], "{precedence:?}");
    }
}

/// Outside TA-13: resource filters with no tag filter still AND.
#[test]
fn two_resource_row_filters_without_a_tag_filter_still_and() {
    for precedence in ALL_PRECEDENCES {
        let (conflicts, filters) = merge_row_filters(vec![id_over_one(), eu()], vec![], precedence);
        assert!(conflicts.is_empty(), "{precedence:?}: {conflicts:?}");
        assert_eq!(filters, vec![id_over_one(), eu()], "{precedence:?}");
    }
}

#[test]
fn merge_tag_masks_first_matching_tag_wins() {
    // Column has two tags; only the second has a mask.
    let mut policy = ResolvedPolicy::default();
    let col_tags = make_col_tags(&[("salary", &["INTERNAL", "PII"])]);
    // Only PII has a mask.
    let tag_masks = make_tag_masks(&[("PII", MaskType::Hash)]);
    merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Tag,
    );
    assert!(
        matches!(policy.column_masks.get("salary"), Some(MaskType::Hash)),
        "the only tag with a mask supplies it"
    );
}

/// Runs `merge_tag_masks` on column `c` carrying `tags`, with `masks` as the
/// tag masks and an optional resource mask. Returns the policy and conflicts.
fn merge_one_column(
    tags: &[&str],
    masks: &[(&str, MaskType)],
    resource: Option<MaskType>,
    precedence: MaskPrecedence,
) -> (ResolvedPolicy, Vec<PolicyConflict>) {
    let mut policy = ResolvedPolicy::default();
    if let Some(m) = resource {
        policy.column_masks.insert("c".to_string(), m);
    }
    let conflicts = merge_tag_masks(
        &mut policy,
        &make_col_tags(&[("c", tags)]),
        &make_tag_masks(masks),
        vec![],
        &no_unmappable(),
        &default_identity(),
        precedence,
    );
    (policy, conflicts)
}

fn partial(show_first: u32, show_last: u32) -> MaskType {
    MaskType::PartialMask {
        show_first,
        show_last,
        upper: 'X',
        lower: 'x',
        digit: '#',
    }
}

const TAG_AND_RESOURCE: [MaskPrecedence; 2] = [MaskPrecedence::Tag, MaskPrecedence::Resource];

fn assert_joined(tags: &[&str], masks: &[(&str, MaskType)], expected: MaskType) {
    for precedence in TAG_AND_RESOURCE {
        let (policy, conflicts) = merge_one_column(tags, masks, None, precedence);
        assert!(
            conflicts.is_empty(),
            "{precedence:?} {tags:?}: {conflicts:?}"
        );
        assert_eq!(
            policy.column_masks.get("c"),
            Some(&expected),
            "{precedence:?} {tags:?}"
        );
        assert!(!policy.is_restricted("c"), "{precedence:?} {tags:?}");
    }
}

fn assert_conflict(tags: &[&str], masks: &[(&str, MaskType)]) {
    for precedence in TAG_AND_RESOURCE {
        let (policy, conflicts) = merge_one_column(tags, masks, None, precedence);
        assert_eq!(
            conflicts,
            vec![PolicyConflict::Mask("c".to_string())],
            "{precedence:?} {tags:?}"
        );
        assert!(policy.is_restricted("c"), "{precedence:?} {tags:?}");
        assert!(policy.mask_for("c").is_none(), "{precedence:?} {tags:?}");
    }
}

#[test]
fn merge_tag_masks_most_restrictive_nullify_beats_a_weaker_tag() {
    let masks = [("pii", MaskType::Nullify), ("weak", partial(0, 4))];
    assert_joined(&["pii", "weak"], &masks, MaskType::Nullify);
    assert_joined(&["weak", "pii"], &masks, MaskType::Nullify);
}

#[test]
fn merge_tag_masks_most_restrictive_narrower_partial_mask_wins() {
    let masks = [("a", partial(0, 2)), ("b", partial(0, 4))];
    assert_joined(&["a", "b"], &masks, partial(0, 2));
    assert_joined(&["b", "a"], &masks, partial(0, 2));
}

#[test]
fn merge_tag_masks_most_restrictive_a_partial_mask_that_reveals_a_class_joins_classless() {
    // Narrower, but keeps the character class visible where the other does
    // not: the meet shows two characters and no class.
    let classless = MaskType::PartialMask {
        show_first: 0,
        show_last: 4,
        upper: '*',
        lower: '*',
        digit: '*',
    };
    let meet = MaskType::PartialMask {
        show_first: 0,
        show_last: 2,
        upper: '*',
        lower: '*',
        digit: '*',
    };
    let masks = [("a", partial(0, 2)), ("b", classless.clone())];
    assert_joined(&["a", "b"], &masks, meet.clone());
    assert_joined(&["b", "a"], &masks, meet);
    // The class-less mask is at least as restrictive when it is also narrower.
    let narrower_classless = MaskType::PartialMask {
        show_first: 0,
        show_last: 2,
        upper: '*',
        lower: '*',
        digit: '*',
    };
    let masks = [("a", partial(0, 4)), ("b", narrower_classless.clone())];
    assert_joined(&["a", "b"], &masks, narrower_classless.clone());
    assert_joined(&["b", "a"], &masks, narrower_classless);
}

#[test]
fn merge_tag_masks_most_restrictive_incomparable_partial_masks_join_to_the_meet() {
    let masks = [("a", partial(4, 0)), ("b", partial(0, 4))];
    assert_joined(&["a", "b"], &masks, partial(0, 0));
    assert_joined(&["b", "a"], &masks, partial(0, 0));
}

/// Ranger `MASK` (classes visible, nothing shown) and `MASK_SHOW_LAST_4` (last
/// four shown, no class) on one column join to a mask that shows neither.
#[test]
fn merge_tag_masks_most_restrictive_ranger_mask_and_show_last_4_join() {
    let ranger_mask = MaskType::PartialMask {
        show_first: 0,
        show_last: 0,
        upper: 'X',
        lower: 'x',
        digit: 'n',
    };
    let show_last_4 = MaskType::PartialMask {
        show_first: 0,
        show_last: 4,
        upper: 'x',
        lower: 'x',
        digit: 'x',
    };
    let meet = MaskType::PartialMask {
        show_first: 0,
        show_last: 0,
        upper: 'x',
        lower: 'x',
        digit: 'x',
    };
    let masks = [("pii", ranger_mask), ("card", show_last_4)];
    assert_joined(&["pii", "card"], &masks, meet.clone());
    assert_joined(&["card", "pii"], &masks, meet);
}

#[test]
fn merge_tag_masks_most_restrictive_a_mask_below_an_incomparable_pair_wins_in_any_order() {
    let masks = [
        ("a", partial(4, 0)),
        ("b", partial(0, 4)),
        ("c", partial(0, 0)),
    ];
    for order in [
        ["a", "b", "c"],
        ["a", "c", "b"],
        ["b", "a", "c"],
        ["b", "c", "a"],
        ["c", "a", "b"],
        ["c", "b", "a"],
    ] {
        assert_joined(&order, &masks, partial(0, 0));
    }
}

#[test]
fn merge_tag_masks_most_restrictive_two_different_customs_conflict() {
    let mut policy = ResolvedPolicy::default();
    let mut masks = HashMap::new();
    masks.insert("x".to_string(), TagMaskSpec::Custom("upper({col})".into()));
    masks.insert("y".to_string(), TagMaskSpec::Custom("lower({col})".into()));
    for order in [["x", "y"], ["y", "x"]] {
        for precedence in TAG_AND_RESOURCE {
            policy.column_masks.clear();
            policy.restricted_columns.clear();
            let conflicts = merge_tag_masks(
                &mut policy,
                &make_col_tags(&[("c", &order)]),
                &masks,
                vec![],
                &no_unmappable(),
                &default_identity(),
                precedence,
            );
            assert_eq!(conflicts, vec![PolicyConflict::Mask("c".to_string())]);
            assert!(policy.is_restricted("c"));
        }
    }
}

#[test]
fn merge_tag_masks_most_restrictive_custom_with_nullify_is_nullify() {
    let mut policy = ResolvedPolicy::default();
    let mut masks = make_tag_masks(&[("n", MaskType::Nullify)]);
    masks.insert("x".to_string(), TagMaskSpec::Custom("upper({col})".into()));
    for order in [["x", "n"], ["n", "x"]] {
        policy.column_masks.clear();
        let conflicts = merge_tag_masks(
            &mut policy,
            &make_col_tags(&[("c", &order)]),
            &masks,
            vec![],
            &no_unmappable(),
            &default_identity(),
            MaskPrecedence::Tag,
        );
        assert!(conflicts.is_empty());
        assert_eq!(policy.column_masks.get("c"), Some(&MaskType::Nullify));
    }
}

#[test]
fn merge_tag_masks_most_restrictive_a_broken_custom_applies_no_other_tag_mask() {
    let mut policy = ResolvedPolicy::default();
    let mut masks = make_tag_masks(&[("h", MaskType::Hash)]);
    masks.insert("bad".to_string(), TagMaskSpec::Custom("((( {col}".into()));
    let conflicts = merge_tag_masks(
        &mut policy,
        &make_col_tags(&[("c", &["h", "bad"])]),
        &masks,
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Tag,
    );
    assert!(conflicts.is_empty());
    assert!(policy.is_restricted("c"));
    assert!(policy.mask_for("c").is_none());
}

#[test]
fn merge_tag_masks_most_restrictive_hash_against_other_masks() {
    let masks = [("h", MaskType::Hash), ("p", partial(0, 4))];
    assert_conflict(&["h", "p"], &masks);
    assert_conflict(&["p", "h"], &masks);
    let masks = [("h", MaskType::Hash), ("d", MaskType::DateShowYear)];
    assert_conflict(&["h", "d"], &masks);
    let masks = [("h", MaskType::Hash), ("r", MaskType::Redact("***".into()))];
    assert_joined(&["h", "r"], &masks, MaskType::Redact("***".into()));
    assert_joined(&["r", "h"], &masks, MaskType::Redact("***".into()));
}

#[test]
fn merge_tag_masks_most_restrictive_two_redactions_are_nullify() {
    let masks = [
        ("a", MaskType::Redact("***".into())),
        ("b", MaskType::Redact("xxx".into())),
    ];
    assert_joined(&["a", "b"], &masks, MaskType::Nullify);
    assert_joined(&["b", "a"], &masks, MaskType::Nullify);
    // The same redaction twice is one mask.
    let masks = [
        ("a", MaskType::Redact("***".into())),
        ("b", MaskType::Redact("***".into())),
    ];
    assert_joined(&["a", "b"], &masks, MaskType::Redact("***".into()));
}

#[test]
fn merge_tag_masks_most_restrictive_a_tag_without_a_mask_adds_nothing() {
    let masks = [("m", MaskType::Hash)];
    assert_joined(&["none", "m"], &masks, MaskType::Hash);
    assert_joined(&["m", "none"], &masks, MaskType::Hash);
}

#[test]
fn merge_tag_masks_most_restrictive_is_independent_of_tag_order() {
    let masks = [
        ("p", partial(0, 4)),
        ("n", MaskType::Nullify),
        ("h", MaskType::Hash),
    ];
    for order in [
        ["p", "n", "h"],
        ["p", "h", "n"],
        ["n", "p", "h"],
        ["n", "h", "p"],
        ["h", "p", "n"],
        ["h", "n", "p"],
    ] {
        assert_joined(&order, &masks, MaskType::Nullify);
    }
    // Redact absorbs a pair that cannot be joined in every order.
    let masks = [
        ("p", partial(0, 4)),
        ("r", MaskType::Redact("*".into())),
        ("h", MaskType::Hash),
    ];
    for order in [
        ["p", "r", "h"],
        ["p", "h", "r"],
        ["r", "p", "h"],
        ["r", "h", "p"],
        ["h", "p", "r"],
        ["h", "r", "p"],
    ] {
        assert_joined(&order, &masks, MaskType::Redact("*".into()));
    }
}

#[test]
fn merge_tag_masks_most_restrictive_tag_winner_replaces_a_resource_mask_under_tag() {
    let masks = [("a", partial(0, 4)), ("b", MaskType::Nullify)];
    let (policy, conflicts) = merge_one_column(
        &["a", "b"],
        &masks,
        Some(MaskType::Hash),
        MaskPrecedence::Tag,
    );
    assert!(conflicts.is_empty());
    assert_eq!(policy.column_masks.get("c"), Some(&MaskType::Nullify));
    // Under `Resource` the resource mask stands and the tags are skipped.
    let (policy, conflicts) = merge_one_column(
        &["a", "b"],
        &masks,
        Some(MaskType::Hash),
        MaskPrecedence::Resource,
    );
    assert!(conflicts.is_empty());
    assert_eq!(policy.column_masks.get("c"), Some(&MaskType::Hash));
}

#[test]
fn merge_tag_masks_empty_col_tags_is_noop() {
    let mut policy = ResolvedPolicy::default();
    merge_tag_masks(
        &mut policy,
        &HashMap::new(),
        &HashMap::new(),
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Tag,
    );
    assert!(policy.column_masks.is_empty());
    assert!(policy.row_filters.is_empty());
}

// ── unmappable-tag fail-closed tests (security regression) ────────────────

#[test]
fn merge_tag_masks_unmappable_tag_restricts_unmasked_column() {
    // A column whose only protection is an unmappable (unsupported type)
    // tag mask must be RESTRICTED, not returned raw.
    let mut policy = ResolvedPolicy::default();
    let col_tags = make_col_tags(&[("ssn", &["PII"])]);
    let tag_masks: HashMap<String, TagMaskSpec> = HashMap::new(); // PII produced no mask (unmappable)
    let unmappable: HashSet<String> = ["PII".to_string()].into_iter().collect();
    merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &unmappable,
        &default_identity(),
        MaskPrecedence::Tag,
    );
    assert!(
        policy.restricted_columns.contains(&"ssn".to_string()),
        "column with unmappable tag mask must be restricted (fail-closed)"
    );
    assert!(
        !policy.column_masks.contains_key("ssn"),
        "restricted column must not also carry a mask"
    );
}

#[test]
fn merge_tag_masks_unmappable_tag_resource_mask_wins_not_restricted() {
    // Same unmappable tag, but the column already has a resource mask. The
    // resource mask is more specific and sufficient — the column must NOT
    // be restricted.
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("ssn".to_string(), MaskType::Hash);
    let col_tags = make_col_tags(&[("ssn", &["PII"])]);
    let tag_masks: HashMap<String, TagMaskSpec> = HashMap::new();
    let unmappable: HashSet<String> = ["PII".to_string()].into_iter().collect();
    merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &unmappable,
        &default_identity(),
        MaskPrecedence::Tag,
    );
    assert!(
        matches!(policy.column_masks.get("ssn"), Some(MaskType::Hash)),
        "resource mask must win over unmappable-tag restriction"
    );
    assert!(
        !policy.restricted_columns.contains(&"ssn".to_string()),
        "column with a resource mask must NOT be restricted by an unmappable tag"
    );
}

// ── CUSTOM tag mask tests ──────────────────────────────────────────────────

#[test]
fn merge_tag_masks_custom_template_applied_to_column() {
    // A CUSTOM TagMaskSpec with a valid {col} template must produce a
    // MaskType::Custom expression for the column — NOT restrict it.
    let mut policy = ResolvedPolicy::default();
    let col_tags = make_col_tags(&[("email", &["PII"])]);
    let mut tag_masks: HashMap<String, TagMaskSpec> = HashMap::new();
    tag_masks.insert(
        "PII".to_string(),
        TagMaskSpec::Custom("concat('***', {col})".to_string()),
    );
    merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Tag,
    );
    assert!(
        matches!(policy.column_masks.get("email"), Some(MaskType::Custom(_))),
        "CUSTOM tag mask with valid expression must produce MaskType::Custom"
    );
    assert!(
        !policy.restricted_columns.contains(&"email".to_string()),
        "column with a valid CUSTOM mask must NOT be restricted"
    );
}

#[test]
fn merge_tag_masks_refuse_same_custom_mask_from_two_tags_is_one_mask() {
    let mut policy = ResolvedPolicy::default();
    let col_tags = make_col_tags(&[("email", &["PII", "CONTACT"]), ("ssn", &["PII", "GOV"])]);
    let mut tag_masks: HashMap<String, TagMaskSpec> = HashMap::new();
    let template = TagMaskSpec::Custom("concat('***', {col})".to_string());
    tag_masks.insert("PII".to_string(), template.clone());
    tag_masks.insert("CONTACT".to_string(), template);
    tag_masks.insert("GOV".to_string(), TagMaskSpec::Ready(MaskType::Nullify));
    let conflicts = merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Refuse,
    );
    assert_eq!(conflicts, vec![PolicyConflict::Mask("ssn".to_string())]);
    assert!(matches!(
        policy.column_masks.get("email"),
        Some(MaskType::Custom(_))
    ));
    assert!(
        policy.is_restricted("ssn"),
        "a contested column fails closed"
    );
    assert!(!policy.column_masks.contains_key("ssn"));
}

#[test]
fn merge_tag_masks_refuse_keeps_the_fail_closed_rules() {
    let mut policy = ResolvedPolicy::default();
    let col_tags = make_col_tags(&[("email", &["BROKEN"]), ("ssn", &["SECRET"])]);
    let mut tag_masks: HashMap<String, TagMaskSpec> = HashMap::new();
    tag_masks.insert(
        "BROKEN".to_string(),
        TagMaskSpec::Custom("concat('***', {col}".to_string()),
    );
    let unmappable: HashSet<String> = ["SECRET".to_string()].into();
    let conflicts = merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![lit(false)],
        &unmappable,
        &default_identity(),
        MaskPrecedence::Refuse,
    );
    assert!(conflicts.is_empty(), "{conflicts:?}");
    assert!(policy.is_restricted("email"), "a broken template restricts");
    assert!(policy.is_restricted("ssn"), "an unmappable tag restricts");
    assert!(policy_denies_all(&policy), "the deny-all sentinel is kept");
}

#[test]
fn merge_tag_masks_custom_template_bad_expr_restricts_column() {
    // A CUSTOM TagMaskSpec whose expression cannot be parsed must RESTRICT
    // the column (fail-closed), not leave it unmasked.
    let mut policy = ResolvedPolicy::default();
    let col_tags = make_col_tags(&[("ssn", &["PII"])]);
    let mut tag_masks: HashMap<String, TagMaskSpec> = HashMap::new();
    // Invalid SQL expression — parser will reject it.
    tag_masks.insert(
        "PII".to_string(),
        TagMaskSpec::Custom("!!!INVALID SQL!!!".to_string()),
    );
    merge_tag_masks(
        &mut policy,
        &col_tags,
        &tag_masks,
        vec![],
        &no_unmappable(),
        &default_identity(),
        MaskPrecedence::Tag,
    );
    assert!(
        policy.restricted_columns.contains(&"ssn".to_string()),
        "column with unparseable CUSTOM tag mask must be restricted (fail-closed)"
    );
    assert!(
        !policy.column_masks.contains_key("ssn"),
        "restricted column must not also carry a mask"
    );
}

#[test]
fn test_apply_mask_nullify_utf8() {
    let expr = apply_mask("ssn", &DataType::Utf8, &MaskType::Nullify, None);
    match expr {
        Expr::Literal(ScalarValue::Utf8(None), _) => {}
        other => panic!("Expected Utf8 NULL literal, got: {other:?}"),
    }
}

#[test]
fn test_apply_mask_nullify_int64_produces_typed_null() {
    let expr = apply_mask("customer_id", &DataType::Int64, &MaskType::Nullify, None);
    match expr {
        Expr::Literal(ScalarValue::Int64(None), _) => {}
        other => panic!("Expected Int64 NULL literal, got: {other:?}"),
    }
}

#[test]
fn test_apply_mask_nullify_decimal_produces_typed_null() {
    let dt = DataType::Decimal128(18, 2);
    let expr = apply_mask("salary", &dt, &MaskType::Nullify, None);
    match expr {
        Expr::Literal(ScalarValue::Decimal128(None, 18, 2), _) => {}
        other => panic!("Expected Decimal128(None,18,2), got: {other:?}"),
    }
}

#[test]
fn test_apply_mask_nullify_timestamp_produces_typed_null() {
    let dt = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None);
    let expr = apply_mask("ts", &dt, &MaskType::Nullify, None);
    match expr {
        Expr::Literal(ScalarValue::TimestampMicrosecond(None, None), _) => {}
        other => panic!("Expected TimestampMicrosecond NULL, got: {other:?}"),
    }
}

#[test]
fn test_apply_mask_redact_utf8_returns_literal() {
    let expr = apply_mask(
        "ssn",
        &DataType::Utf8,
        &MaskType::Redact("***".to_string()),
        None,
    );
    assert!(matches!(expr, Expr::Literal(ScalarValue::Utf8(Some(_)), _)));
}

#[test]
fn test_apply_mask_redact_int_wraps_in_cast() {
    let expr = apply_mask(
        "id",
        &DataType::Int64,
        &MaskType::Redact("0".to_string()),
        None,
    );
    assert!(
        matches!(expr, Expr::Cast(_)),
        "Expected Cast for Redact on Int64, got: {expr:?}"
    );
}

/// Iceberg scans read strings as Utf8View by default. PartialMask used to
/// handle only Utf8/LargeUtf8 and replaced a Utf8View column with NULL,
/// so the mask silently showed nothing instead of the unmasked tail.
#[tokio::test]
async fn test_apply_mask_partial_on_utf8_view_masks_and_keeps_the_type() {
    use datafusion::arrow::array::{Array, AsArray, StringViewArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "ssn",
        DataType::Utf8View,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringViewArray::from(vec![
            Some("111-11-1111"),
            None,
        ]))],
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();

    let mask = MaskType::PartialMask {
        show_first: 0,
        show_last: 4,
        upper: 'x',
        lower: 'x',
        digit: 'x',
    };
    let expr = apply_mask("ssn", &DataType::Utf8View, &mask, None).alias("ssn");
    let out = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![expr])
        .unwrap()
        .collect()
        .await
        .unwrap();
    let ssn = out[0].column(0);
    assert_eq!(ssn.data_type(), &DataType::Utf8View);
    let ssn = ssn.as_string_view();
    assert_eq!(ssn.value(0), "xxx-xx-1111");
    assert!(ssn.is_null(1));
}

#[test]
fn test_apply_mask_hash_utf8_produces_scalar_function() {
    let expr = apply_mask("email", &DataType::Utf8, &MaskType::Hash, None);
    assert!(
        matches!(expr, Expr::ScalarFunction(_)),
        "Expected ScalarFunction for Hash mask, got: {expr:?}"
    );
}

/// Regression for #37: when a mask key is configured the Hash branch
/// must still produce a scalar function, but the registered UDF
/// carries the key so it runs HMAC-SHA256 at execution time. We can't
/// reach into the boxed ScalarFunction from the LogicalPlan layer, so
/// the assertion focuses on the structural invariant (still a
/// function call wrapped in a Cast for non-string types) plus the
/// keyed-vs-unkeyed branch in the underlying UDF (covered in
/// `sha256_udf::tests`).
#[test]
fn test_apply_mask_hash_with_key_still_produces_scalar_function() {
    let key = Some(Arc::new(b"deployment-key".to_vec()));
    let utf8_expr = apply_mask("email", &DataType::Utf8, &MaskType::Hash, key.clone());
    assert!(matches!(utf8_expr, Expr::ScalarFunction(_)));

    let int_expr = apply_mask("id", &DataType::Int64, &MaskType::Hash, key);
    assert!(matches!(int_expr, Expr::Cast(_)));
}

// ── resolve_policy_key (issue #205) ──────────────────────────
// The Iceberg key keeps the FULL dotted namespace ("ns1.ns2", not just
// "ns2"); a bare table falls back to "default". `hive_catalogs` is
// empty for all of these, so the Hive branch never fires.

/// A `HiveCatalogs` whose only member is `name`, built through the same
/// public API `ATTACH` will use, not by poking the private field.
fn hive_only(name: &str) -> HiveCatalogs {
    let hc = HiveCatalogs::default();
    hc.insert(name);
    hc
}

#[test]
fn resolve_policy_key_bare_table_uses_default_namespace() {
    let r = datafusion::common::TableReference::bare("employees");
    assert_eq!(
        resolve_policy_key(&r, &HiveCatalogs::default()),
        Some(("default".to_string(), "employees".to_string()))
    );
}

#[test]
fn resolve_policy_key_two_part_uses_schema_as_namespace() {
    let r = datafusion::common::TableReference::partial("hr", "employees");
    assert_eq!(
        resolve_policy_key(&r, &HiveCatalogs::default()),
        Some(("hr".to_string(), "employees".to_string()))
    );
}

#[test]
fn resolve_policy_key_three_part_uses_schema_as_namespace() {
    let r = datafusion::common::TableReference::full("cat", "hr", "employees");
    assert_eq!(
        resolve_policy_key(&r, &HiveCatalogs::default()),
        Some(("hr".to_string(), "employees".to_string()))
    );
}

#[test]
fn resolve_policy_key_keeps_the_whole_multilevel_namespace() {
    // cat.ns1.ns2.employees -> schema "ns1.ns2", kept whole.
    //
    // This replaces a test that pinned the last component ("ns2"). That was a
    // collision: `sales` and `a.b.sales` both resolved to the Ranger database
    // `sales`, so a policy written for one fired on the other.
    let r = datafusion::common::TableReference::full("cat", "ns1.ns2", "employees");
    assert_eq!(
        resolve_policy_key(&r, &HiveCatalogs::default()),
        Some(("ns1.ns2".to_string(), "employees".to_string()))
    );
}

/// 201: the read path (TableReference) and the ident path (DML,
/// information_schema) must agree on every shape, including
/// multi-level namespaces and Hive roots.
#[test]
fn ident_and_table_ref_keys_agree() {
    use datafusion::common::TableReference;
    let cats = HiveCatalogs::default();
    let ns = |parts: &[&str]| parts.iter().map(|p| p.to_string()).collect::<Vec<_>>();
    assert_eq!(
        resolve_policy_key(&TableReference::partial("a.b", "t"), &cats),
        resolve_policy_key_for_ident(None, &ns(&["a", "b"]), "t", &cats)
    );
    assert_eq!(
        resolve_policy_key(&TableReference::full("cat", "a.b", "t"), &cats),
        resolve_policy_key_for_ident(Some("cat"), &ns(&["a", "b"]), "t", &cats)
    );
    assert_eq!(
        resolve_policy_key(&TableReference::bare("t"), &cats),
        resolve_policy_key_for_ident(None, &[], "t", &cats)
    );
    assert_eq!(
        resolve_policy_key_for_ident(None, &ns(&["a", "b"]), "t", &cats),
        Some(("a.b".to_string(), "t".to_string())),
        "the whole namespace is the key, never its last component"
    );
    assert_eq!(resolve_policy_key_for_ident(None, &[], "", &cats), None);
}

#[test]
fn namespaces_sharing_a_last_component_no_longer_collide() {
    // The point of keeping the whole namespace. Before, both of these produced
    // the key `sales`.
    let shallow = datafusion::common::TableReference::full("cat", "sales", "orders");
    let deep = datafusion::common::TableReference::full("cat", "a.b.sales", "orders");
    let shallow_key = resolve_policy_key(&shallow, &HiveCatalogs::default()).expect("shallow");
    let deep_key = resolve_policy_key(&deep, &HiveCatalogs::default()).expect("deep");
    assert_ne!(
        shallow_key, deep_key,
        "two distinct Iceberg namespaces must not resolve to the same policy key"
    );
    assert_eq!(shallow_key.0, "sales");
    assert_eq!(deep_key.0, "a.b.sales");
}

#[test]
fn resolve_policy_key_empty_table_fails_closed() {
    // An empty table component cannot be confidently keyed; the caller
    // treats None as deny-all rather than passthrough.
    let r = datafusion::common::TableReference::bare("");
    assert_eq!(resolve_policy_key(&r, &HiveCatalogs::default()), None);
}

// ── resolve_policy_key: Hive routing (Task 7) ─────────────────
// A Hive table's key must carry its warehouse-root name, never collide
// with a same-named Iceberg table, and leave the Iceberg branch alone.

#[test]
fn hive_table_key_carries_the_root_name() {
    let r = datafusion::common::TableReference::full("lake", "sales", "orders");
    assert_eq!(
        resolve_policy_key(&r, &hive_only("lake")),
        Some(("lake.sales".to_string(), "orders".to_string()))
    );
}

#[test]
fn an_iceberg_key_is_unchanged_by_the_hive_branch() {
    // Regression guard: every Ranger policy already written against the
    // hive_database convention keys on the bare namespace. The Hive branch
    // must not touch that, whether or not some OTHER catalog is Hive-routed.
    let r = datafusion::common::TableReference::full("iceberg", "hr", "employees");
    assert_eq!(
        resolve_policy_key(&r, &hive_only("lake")),
        Some(("hr".to_string(), "employees".to_string()))
    );
    assert_eq!(
        resolve_policy_key(&r, &HiveCatalogs::default()),
        Some(("hr".to_string(), "employees".to_string()))
    );
}

#[test]
fn an_iceberg_grant_cannot_reach_a_hive_table_of_the_same_name() {
    let iceberg = datafusion::common::TableReference::full("iceberg", "hr", "employees");
    let hive = datafusion::common::TableReference::full("lake", "hr", "employees");
    let cats = hive_only("lake");
    assert_ne!(
        resolve_policy_key(&iceberg, &cats),
        resolve_policy_key(&hive, &cats),
        "same-named tables in different catalog kinds must not share a policy key"
    );
}

/// The residual gap the security review found and `attach_configured_hive_roots`
/// (`runtime_catalog.rs`) now refuses at the door, not here: this
/// function itself only matches `TableReference::Full`, so a two-part
/// reference (`table_ref.catalog() == None`) ALWAYS takes the Iceberg
/// branch, even when `hive_catalogs` names a root that happens to equal
/// what the caller's session default catalog resolved from. Every other
/// test above uses `TableReference::full`; this one pins the
/// non-`Full` case explicitly, so a future refactor that "helpfully"
/// tries to promote the reference here (which the review deliberately
/// deferred: a late change to a security-critical path, versus a cheap,
/// fail-closed refusal at attach time) does not silently reopen the
/// escalation this key decides. The Hive root name intentionally
/// matches a real Iceberg namespace here to make the collision
/// concrete, not just type-shaped.
#[test]
fn a_two_part_reference_never_takes_the_hive_branch_even_if_the_name_matches() {
    let partial = datafusion::common::TableReference::partial("lake", "orders");
    let bare = datafusion::common::TableReference::bare("orders");
    let cats = hive_only("lake");

    assert_eq!(
        resolve_policy_key(&partial, &cats),
        Some(("lake".to_string(), "orders".to_string())),
        "a Partial reference must resolve as an ordinary Iceberg namespace named 'lake', \
         not the Hive root 'lake'"
    );
    assert_eq!(
        resolve_policy_key(&bare, &cats),
        Some(("default".to_string(), "orders".to_string())),
        "a Bare reference has no catalog component to match against at all"
    );
}

#[test]
fn a_root_attached_after_the_rewriter_was_built_still_gets_the_hive_key() {
    // The regression guard for the staleness path. The enforcer is built
    // once process-wide; ATTACH happens later. If the set were
    // snapshotted, this key would come back as ("sales", "orders") and
    // the table would inherit any grant written for an Iceberg namespace
    // named `sales`.
    let cats = HiveCatalogs::default();
    let r = datafusion::common::TableReference::full("lake", "sales", "orders");
    assert_eq!(
        resolve_policy_key(&r, &cats),
        Some(("sales".to_string(), "orders".to_string())),
        "before ATTACH the root is unknown, so the Iceberg branch is correct"
    );
    cats.insert("lake");
    assert_eq!(
        resolve_policy_key(&r, &cats),
        Some(("lake.sales".to_string(), "orders".to_string())),
        "after ATTACH the same reference must key under the root"
    );
}

// ── catalog-scoped keys ([policy.ranger] catalog_scoped) ──────────

#[test]
fn catalog_scoped_key_carries_the_catalog() {
    use datafusion::common::TableReference;
    let cats = HiveCatalogs::default();
    let a = TableReference::full("ws_a", "finance", "orders");
    let b = TableReference::full("ws_b", "finance", "orders");
    assert_eq!(
        resolve_policy_key_scoped(&a, &cats, true),
        Some(("ws_a.finance".to_string(), "orders".to_string()))
    );
    assert_eq!(
        resolve_policy_key_scoped(&b, &cats, true),
        Some(("ws_b.finance".to_string(), "orders".to_string()))
    );
    // Off: both catalogs collapse onto one key, which is the leak.
    assert_eq!(
        resolve_policy_key_scoped(&a, &cats, false),
        resolve_policy_key_scoped(&b, &cats, false)
    );
    assert_eq!(
        resolve_policy_key(&a, &cats),
        resolve_policy_key_scoped(&a, &cats, false)
    );
    // Multi-level namespaces stay whole behind the catalog.
    assert_eq!(
        resolve_policy_key_for_ident_scoped(
            Some("ws_a"),
            &["a".to_string(), "b".to_string()],
            "t",
            &cats,
            true
        ),
        Some(("ws_a.a.b".to_string(), "t".to_string()))
    );
}

#[test]
fn catalog_scoped_key_without_a_catalog_fails_closed() {
    use datafusion::common::TableReference;
    let cats = HiveCatalogs::default();
    assert_eq!(
        resolve_policy_key_scoped(&TableReference::partial("finance", "orders"), &cats, true),
        None
    );
    assert_eq!(
        resolve_policy_key_scoped(&TableReference::bare("orders"), &cats, true),
        None
    );
    // A reserved virtual relation holds no user rows; it keeps its key.
    assert_eq!(
        resolve_policy_key_scoped(
            &TableReference::partial("information_schema", "tables"),
            &cats,
            true
        ),
        Some(("information_schema".to_string(), "tables".to_string()))
    );
}

#[test]
fn catalog_scoped_leaves_the_hive_key_alone() {
    use datafusion::common::TableReference;
    let r = TableReference::full("lake", "sales", "orders");
    assert_eq!(
        resolve_policy_key_scoped(&r, &hive_only("lake"), true),
        Some(("lake.sales".to_string(), "orders".to_string())),
        "a Hive key is root.database already, not lake.lake.sales"
    );
}

/// Test store that keys on the catalog, like a `catalog_scoped` Ranger.
struct CatalogScopedStore(crate::policy_store::InMemoryPolicyStore);

#[async_trait]
impl PolicyStore for CatalogScopedStore {
    fn catalog_scoped(&self) -> bool {
        true
    }
    async fn resolve(
        &self,
        user: &SessionUser,
        table_name: &str,
        namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        self.0.resolve(user, table_name, namespace).await
    }
}

/// Two catalogs with the same `finance.orders`: a mask written for
/// `ws_a.finance` masks `ws_a.finance.orders` and not `ws_b.finance.orders`,
/// and a catalog-less scan is denied rather than keyed without a catalog.
#[tokio::test]
async fn catalog_scoped_store_masks_only_the_named_catalog() {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::TableReference;
    use datafusion::logical_expr::builder::table_scan;

    let inner = crate::policy_store::InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("ssn".to_string(), MaskType::Nullify);
    inner
        .add_table_policy("ws_a.finance", "orders", policy)
        .await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(CatalogScopedStore(inner)));
    let user = diag_user(&[]);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("ssn", DataType::Utf8, true),
    ]);
    let scan_of = |r: TableReference| table_scan(Some(r), &schema, None).unwrap().build().unwrap();

    let (_, a) = rewriter
        .evaluate(
            &user,
            &crate::TableScope::default(),
            scan_of(TableReference::full("ws_a", "finance", "orders")),
        )
        .await
        .unwrap();
    assert_eq!(
        a.columns_masked,
        vec!["ssn".to_string()],
        "named catalog is masked"
    );
    assert!(!a.denied);

    let (_, b) = rewriter
        .evaluate(
            &user,
            &crate::TableScope::default(),
            scan_of(TableReference::full("ws_b", "finance", "orders")),
        )
        .await
        .unwrap();
    assert!(
        b.columns_masked.is_empty(),
        "another catalog's same-named table is not"
    );

    let (_, none) = rewriter
        .evaluate(
            &user,
            &crate::TableScope::default(),
            scan_of(TableReference::partial("finance", "orders")),
        )
        .await
        .unwrap();
    assert!(
        none.denied,
        "no catalog to key on: deny, never the bare namespace"
    );
}

/// `table_files('finance', 'orders', 'ws_b')` run from catalog `ws_a`
/// reads `ws_b`, so a `catalog_scoped` store checks `ws_b.finance`, not
/// the session's `ws_a.finance`. Keyed on the session's catalog, a
/// caller denied `ws_b.finance.orders` read its metadata from `ws_a`.
#[tokio::test]
async fn catalog_scoped_metadata_tvf_is_checked_in_the_catalog_it_names() {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;

    fn names_ws_b(p: &dyn TableProvider) -> Option<(Option<String>, String, String)> {
        (p as &dyn std::any::Any)
            .downcast_ref::<EmptyTable>()
            .map(|_| (Some("ws_b".into()), "finance".into(), "orders".into()))
    }
    fn names_none(p: &dyn TableProvider) -> Option<(Option<String>, String, String)> {
        names_ws_b(p).map(|(_, ns, t)| (None, ns, t))
    }

    let inner = crate::policy_store::InMemoryPolicyStore::new();
    let mut deny = ResolvedPolicy::default();
    deny.row_filters.push(lit(false));
    inner.add_table_policy("ws_b.finance", "orders", deny).await;
    let store = Arc::new(CatalogScopedStore(inner));
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
    let plan = LogicalPlanBuilder::scan(
        "table_files",
        provider_as_source(Arc::new(EmptyTable::new(schema))),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let scope = |c: &str| crate::TableScope {
        catalog: Some(c.to_string()),
        schema: Some("public".to_string()),
    };
    let user = diag_user(&[]);

    let err = PolicyPlanRewriter::new(store.clone())
        .with_metadata_target(names_ws_b)
        .evaluate(&user, &scope("ws_a"), plan.clone())
        .await
        .unwrap_err();
    assert_eq!(err.error_code(), sqe_core::SqeErrorCode::TableNotFound);

    let rewriter = PolicyPlanRewriter::new(store).with_metadata_target(names_none);
    assert!(
        rewriter
            .evaluate(&user, &scope("ws_a"), plan.clone())
            .await
            .is_ok(),
        "no catalog named: the session's ws_a, which has no policy"
    );
    assert!(
        rewriter
            .evaluate(&user, &scope("ws_b"), plan)
            .await
            .is_err(),
        "no catalog named, session in ws_b: denied"
    );
}

// ── is_reserved_virtual_ref ──────────────────────────────────
// Reserved virtual relations are skipped from the tag lookup so an
// unknown tag state never denies information_schema / system queries.
// The skip must be tight: a real Iceberg ref must never match.

#[test]
fn reserved_ref_matches_bare_information_schema() {
    let r = datafusion::common::TableReference::partial("information_schema", "tables");
    assert!(is_reserved_virtual_ref(&r));
}

#[test]
fn reserved_ref_matches_qualified_information_schema() {
    let r = datafusion::common::TableReference::full("cat", "information_schema", "columns");
    assert!(is_reserved_virtual_ref(&r));
}

#[test]
fn reserved_ref_matches_system_and_datafusion_catalogs() {
    let sys = datafusion::common::TableReference::full("system", "runtime", "queries");
    assert!(is_reserved_virtual_ref(&sys));
    let df = datafusion::common::TableReference::full("datafusion", "info", "version");
    assert!(is_reserved_virtual_ref(&df));
    for schema in ["metadata", "jdbc", "RUNTIME"] {
        let r = datafusion::common::TableReference::full("system", schema, "t");
        assert!(is_reserved_virtual_ref(&r), "system.{schema}.t");
    }
}

#[test]
fn reserved_ref_does_not_match_polaris_system_schemas() {
    // #901: a Polaris catalog named `system` serves every schema the
    // virtual part does not reserve; those tables are tag-governed.
    let r = datafusion::common::TableReference::full("system", "access", "audit");
    assert!(!is_reserved_virtual_ref(&r));
    // A quoted `"SYSTEM"` is a different catalog, never the virtual one.
    let r = datafusion::common::TableReference::full("SYSTEM", "runtime", "queries");
    assert!(!is_reserved_virtual_ref(&r));
}

#[test]
fn reserved_ref_does_not_match_real_iceberg_table() {
    // A normal user table must be tag-governed.
    let r = datafusion::common::TableReference::full("cat", "ns1.ns2", "employees");
    assert!(!is_reserved_virtual_ref(&r));
    // Bare and two-part user tables too.
    assert!(!is_reserved_virtual_ref(
        &datafusion::common::TableReference::bare("employees")
    ));
    assert!(!is_reserved_virtual_ref(
        &datafusion::common::TableReference::partial("hr", "employees")
    ));
}

#[test]
fn reserved_ref_does_not_match_namespace_ending_in_runtime() {
    // The fix keys on reserved CATALOG names, not on the last namespace
    // component, so a user namespace ending in "runtime" is NOT a bypass.
    let r = datafusion::common::TableReference::full("cat", "app.runtime", "events");
    assert!(!is_reserved_virtual_ref(&r));
}

#[test]
fn reserved_ref_does_not_match_pg_catalog() {
    // SQE does not register pg_catalog as a virtual provider, so a user
    // namespace named pg_catalog must still be tag-governed.
    let r = datafusion::common::TableReference::partial("pg_catalog", "pg_class");
    assert!(!is_reserved_virtual_ref(&r));
}

#[test]
fn test_apply_mask_custom_returns_provided_expr() {
    let custom_expr = datafusion::logical_expr::lit("REDACTED");
    let result = apply_mask(
        "secret",
        &DataType::Utf8,
        &MaskType::Custom(custom_expr.clone()),
        None,
    );
    assert_eq!(
        format!("{result:?}"),
        format!("{custom_expr:?}"),
        "Custom mask should return the provided expression unchanged"
    );
}

#[tokio::test]
async fn custom_mask_referencing_sibling_resolves_end_to_end() {
    use crate::policy_store::InMemoryPolicyStore;
    use crate::session_udf::SessionIdentity;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::TableReference;
    use datafusion::logical_expr::builder::table_scan;
    use sqe_core::SessionUser;

    let schema = Schema::new(vec![
        Field::new("salary", DataType::Utf8, true),
        Field::new("department", DataType::Utf8, true),
    ]);
    let scan = table_scan(
        Some(TableReference::partial("hr", "employees")),
        &schema,
        None,
    )
    .unwrap()
    .build()
    .unwrap();

    let identity = SessionIdentity {
        username: "bob".to_string(),
        roles: vec![],
        database: Some("db".to_string()),
        schema: Some("hr".to_string()),
    };
    let mask_expr = parse_sql_predicate(
        "CASE WHEN department = 'HR' THEN salary ELSE '0' END",
        &identity,
    )
    .unwrap();

    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("salary".to_string(), MaskType::Custom(mask_expr));

    let store = InMemoryPolicyStore::new();
    store.add_table_policy("hr", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let user = SessionUser {
        username: "bob".to_string(),
        roles: vec![],
        subject: None,
        email: None,
        groups: vec![],
    };

    let (rewritten, _summary) = rewriter
        .evaluate(&user, &crate::TableScope::default(), scan)
        .await
        .expect("rewrite with sibling-referencing CUSTOM mask must succeed");

    let rendered = format!("{}", rewritten.display_indent());
    assert!(
        rendered.contains("department"),
        "rewritten plan must reference the sibling column, got:\n{rendered}"
    );
}

/// A view body is optimized when it is planned, so its WHERE arrives as
/// pushed-down `TableScan.filters`. Those run on raw values at file,
/// row-group and row level, so a predicate on a masked or restricted column
/// must leave the scan and be evaluated above the mask instead.
#[tokio::test]
async fn scan_filters_on_governed_columns_move_above_the_mask() {
    use crate::policy_store::InMemoryPolicyStore;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::tree_node::TreeNode;
    use datafusion::common::TableReference;
    use datafusion::logical_expr::builder::table_scan_with_filters;
    use sqe_core::SessionUser;

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("salary", DataType::Int64, true),
        Field::new("ssn", DataType::Utf8, true),
    ]);
    // The salary predicate's column is not even projected (an Exact
    // pushdown can drop it from the projection).
    let scan = table_scan_with_filters(
        Some(TableReference::partial("hr", "employees")),
        &schema,
        Some(vec![0, 2]),
        vec![
            col("salary").gt_eq(lit(100_000i64)),
            col("ssn").eq(lit("x")),
            col("id").gt(lit(0i64)),
        ],
    )
    .unwrap()
    .build()
    .unwrap();

    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("salary".to_string(), MaskType::Nullify);
    policy.restricted_columns.push("ssn".to_string());
    let store = InMemoryPolicyStore::new();
    store.add_table_policy("hr", "employees", policy).await;
    let user = SessionUser {
        username: "bob".to_string(),
        roles: vec![],
        subject: None,
        email: None,
        groups: vec![],
    };

    let (rewritten, _) = PolicyPlanRewriter::new(Arc::new(store))
        .evaluate(&user, &crate::TableScope::default(), scan)
        .await
        .unwrap();

    let mut scan_filters = Vec::new();
    rewritten
        .apply(|n| {
            if let LogicalPlan::TableScan(s) = n {
                scan_filters.extend(s.filters.iter().map(|f| f.to_string()));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
    assert_eq!(
        scan_filters.len(),
        1,
        "only the ungoverned predicate may stay on the scan: {scan_filters:?}"
    );
    assert!(scan_filters[0].contains("id"), "{scan_filters:?}");
    let rendered = format!("{}", rewritten.display_indent());
    assert!(
        rendered.contains("salary >= Int64(100000)") && rendered.contains("ssn = Utf8(\"x\")"),
        "the moved predicates must still filter, above the mask:\n{rendered}"
    );
    assert_eq!(
        rewritten.schema().fields().len(),
        2,
        "the caller's columns are unchanged"
    );
}

/// #750: a metadata TVF over a table the caller is denied all access to is
/// refused by `evaluate` itself (so CTAS/INSERT/EXPLAIN are covered too),
/// including when the TVF sits inside a subquery.
#[tokio::test]
async fn metadata_tvf_over_a_denied_table_is_refused_inside_a_subquery() {
    use crate::policy_store::InMemoryPolicyStore;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::exists;
    use sqe_core::SessionUser;

    // Stand-in for `table_files('sales','orders')`: an EmptyTable that the
    // injected lookup maps to its target.
    fn target(p: &dyn TableProvider) -> Option<(Option<String>, String, String)> {
        (p as &dyn std::any::Any)
            .downcast_ref::<EmptyTable>()
            .map(|_| (None, "sales".to_string(), "orders".to_string()))
    }
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
    let tvf = LogicalPlanBuilder::scan(
        "table_files",
        provider_as_source(Arc::new(EmptyTable::new(Arc::clone(&schema)))),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let plan = datafusion::logical_expr::builder::table_scan(Some("t"), &schema, None)
        .unwrap()
        .filter(exists(Arc::new(tvf)))
        .unwrap()
        .build()
        .unwrap();
    let user = SessionUser {
        username: "analyst".to_string(),
        roles: vec![],
        subject: None,
        email: None,
        groups: vec![],
    };

    let store = Arc::new(InMemoryPolicyStore::new());
    let rewriter = PolicyPlanRewriter::new(store.clone()).with_metadata_target(target);
    assert!(
        rewriter
            .evaluate(&user, &crate::TableScope::default(), plan.clone())
            .await
            .is_ok(),
        "no policy on the target: passes"
    );

    let mut deny = ResolvedPolicy::default();
    deny.row_filters.push(lit(false));
    store.add_table_policy("sales", "orders", deny).await;
    let err = rewriter
        .evaluate(&user, &crate::TableScope::default(), plan)
        .await
        .unwrap_err();
    assert_eq!(err.error_code(), sqe_core::SqeErrorCode::TableNotFound);
    assert!(
        err.to_string().contains("table not found: sales.orders"),
        "{err}"
    );
}

/// A file table function's scan (`read_csv(...)`) has no Iceberg tags, so
/// an unknown tag state does not deny it. Recognised by provider type:
/// any other scan named `read_csv()` is still denied, as is one sharing
/// that name with a file scan in the same plan.
#[tokio::test]
async fn file_table_scan_is_not_denied_for_unknown_tag_state() {
    use crate::policy_store::InMemoryPolicyStore;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::{provider_as_source, MemTable, ViewTable};
    use sqe_core::SessionUser;

    struct UnknownTags;
    impl TagSource for UnknownTags {
        fn column_tags(
            &self,
            _c: Option<&str>,
            _n: &[String],
            _t: &str,
        ) -> Option<HashMap<String, Vec<String>>> {
            None
        }
    }
    // Stand-in for the file table provider: an EmptyTable.
    fn file_table(p: &dyn TableProvider) -> bool {
        (p as &dyn std::any::Any)
            .downcast_ref::<EmptyTable>()
            .is_some()
    }

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
    let scan = |name: &str, provider: Arc<dyn TableProvider>| {
        LogicalPlanBuilder::scan(name, provider_as_source(provider), None)
            .unwrap()
            .build()
            .unwrap()
    };
    let file = || scan("read_csv()", Arc::new(EmptyTable::new(Arc::clone(&schema))));
    let mem = |name: &str| {
        scan(
            name,
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap()),
        )
    };
    let view = scan(
        "read_csv()",
        Arc::new(ViewTable::new(
            datafusion::logical_expr::builder::table_scan(Some("t"), &schema, None)
                .unwrap()
                .build()
                .unwrap(),
            None,
        )),
    );
    let same_name_union = LogicalPlanBuilder::from(file())
        .union(mem("read_csv()"))
        .unwrap()
        .build()
        .unwrap();

    let user = SessionUser {
        username: "analyst".to_string(),
        roles: vec![],
        subject: None,
        email: None,
        groups: vec![],
    };
    let scope = crate::TableScope {
        catalog: Some("ws".to_string()),
        schema: Some("default".to_string()),
    };
    let rewriter = PolicyPlanRewriter::new(Arc::new(InMemoryPolicyStore::new()))
        .with_tag_source(Arc::new(UnknownTags))
        .with_file_table(file_table);
    // Denied either by a deny-all row filter (the un-inlined view) or by
    // the unknown-tag-state refusal (#840).
    let denied = |plan: LogicalPlan| {
        let rewriter = &rewriter;
        let scope = &scope;
        let user = &user;
        async move {
            match rewriter.evaluate(user, scope, plan).await {
                Ok((_, summary)) => summary.denied,
                Err(e) => {
                    assert!(e.to_string().contains("tag state unknown"), "{e}");
                    true
                }
            }
        }
    };

    assert!(
        !denied(file()).await,
        "a file scan has no tags to be unsure of"
    );
    assert!(
        denied(mem("t")).await,
        "a table on a tag cache miss fails closed"
    );
    assert!(
        denied(mem("read_csv()")).await,
        "the scan name grants nothing"
    );
    assert!(
        denied(view).await,
        "a view named read_csv() is still denied"
    );
    assert!(
        denied(same_name_union).await,
        "a table sharing the name with a file scan is governed as a table"
    );
}

// ── resolve_effective_policy (SHOW EFFECTIVE POLICY diagnostic) ───────
// The diagnostic must report the SAME resolution the rewriter applies:
// resource policy from store.resolve, plus tag-merged masks/filters.

fn diag_user(roles: &[&str]) -> sqe_core::session::SessionUser {
    sqe_core::session::SessionUser {
        username: "alice".to_string(),
        roles: roles.iter().map(|r| r.to_string()).collect(),
        subject: None,
        email: None,
        groups: vec![],
    }
}

#[tokio::test]
async fn resolve_effective_policy_surfaces_resource_masks_and_filters() {
    let store = crate::policy_store::InMemoryPolicyStore::new();
    let mut p = ResolvedPolicy::default();
    p.column_masks.insert("ssn".to_string(), MaskType::Hash);
    p.row_filters.push(col("region").eq(lit("EU")));
    p.restricted_columns.push("notes".to_string());
    store.add_table_policy("hr", "employees", p).await;

    let got = resolve_effective_policy(
        &store,
        &diag_user(&[]),
        None,
        "employees",
        "hr",
        &HashMap::new(),
        MaskPrecedence::Tag,
    )
    .await;

    assert!(matches!(got.column_masks.get("ssn"), Some(MaskType::Hash)));
    assert_eq!(got.row_filters.len(), 1);
    assert_eq!(got.restricted_columns, vec!["notes".to_string()]);
}

#[tokio::test]
async fn resolve_effective_policy_no_policy_is_empty_passthrough() {
    let store = crate::policy_store::InMemoryPolicyStore::new();
    let got = resolve_effective_policy(
        &store,
        &diag_user(&[]),
        None,
        "orders",
        "sales",
        &HashMap::new(),
        MaskPrecedence::Tag,
    )
    .await;
    assert!(got.column_masks.is_empty());
    assert!(got.row_filters.is_empty());
    assert!(got.restricted_columns.is_empty());
}

#[tokio::test]
async fn resolve_effective_policy_matches_rewriter_key_via_namespace() {
    // The store keys on "{namespace}.{table}"; the caller must pass the
    // last namespace component (as resolve_policy_key derives), so a policy
    // registered under "ns2" is found when the table is cat.ns1.ns2.t.
    let store = crate::policy_store::InMemoryPolicyStore::new();
    let mut p = ResolvedPolicy::default();
    p.column_masks
        .insert("email".to_string(), MaskType::Nullify);
    store.add_table_policy("ns2", "t", p).await;

    let got = resolve_effective_policy(
        &store,
        &diag_user(&[]),
        None,
        "t",
        "ns2",
        &HashMap::new(),
        MaskPrecedence::Tag,
    )
    .await;
    assert!(matches!(
        got.column_masks.get("email"),
        Some(MaskType::Nullify)
    ));
}
