//! The fold and the files it writes, over a bundle made here.

use std::fs::File;
use std::process::Command;

use arrow::array::{
    Array, BooleanArray, Int32Array, Int64Array, StringArray, TimestampMillisecondArray,
};
use mwrev::{BundleWriter, RevisionHeader};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use wikidata_refs::fold::{Collected, FoldError, PageFold};

const A: &str = "Q6$11111111-1111-1111-1111-111111111111";

fn header(page: u64, ns: i64, rev: u64, hidden: bool) -> RevisionHeader {
    RevisionHeader {
        page_id: page,
        ns,
        rev_id: rev,
        parent_rev_id: None,
        timestamp: format!("2020-01-01T00:00:{:02}Z", rev % 60),
        user_id: 1,
        text_hidden: hidden,
    }
}

fn url_ref(url: &str) -> String {
    format!(
        r#"{{"hash":"ignored","snaks":{{"P854":[{{"snaktype":"value","property":"P854","hash":"x","datavalue":{{"value":"{url}","type":"string"}}}}],"P248":[{{"snaktype":"value","property":"P248","datavalue":{{"value":{{"entity-type":"item","numeric-id":5,"id":"Q5"}},"type":"wikibase-entityid"}}}},{{"snaktype":"somevalue","property":"P248"}}]}},"snaks-order":["P854","P248"]}}"#
    )
}

/// An item as Wikibase stores a revision: no data types beside the snaks.
fn item(id: &str, guid: &str, refs: &[String]) -> String {
    format!(
        r#"{{"type":"item","id":"{id}","labels":{{"en":{{"language":"en","value":"six"}}}},"descriptions":[],"aliases":[],"claims":{{"P31":[{{"mainsnak":{{"snaktype":"value","property":"P31","hash":"y","datavalue":{{"value":{{"entity-type":"item","numeric-id":5,"id":"Q5"}},"type":"wikibase-entityid"}}}},"type":"statement","id":"{guid}","rank":"normal","references":[{}]}}]}},"sitelinks":[]}}"#,
        refs.join(",")
    )
}

/// The revisions of the test bundle, in order.
fn revisions() -> Vec<(RevisionHeader, String)> {
    let one = url_ref("https://a.example/");
    let two = url_ref("https://b.example/");
    vec![
        // Page 10, Q6: no reference, one, hidden, legacy, two, one, redirect.
        (header(10, 0, 100, false), item("Q6", A, &[])),
        (
            header(10, 0, 101, false),
            item("Q6", A, std::slice::from_ref(&one)),
        ),
        (header(10, 0, 102, true), String::new()),
        (
            header(10, 0, 103, false),
            r#"{"label":{"en":"six"},"entity":["item",6],"claims":[]}"#.to_string(),
        ),
        (
            header(10, 0, 104, false),
            item("Q6", &A.to_lowercase(), &[one.clone(), two.clone()]),
        ),
        (
            header(10, 0, 105, false),
            item("Q6", A, &[two.clone(), one.clone()]),
        ),
        (
            header(10, 0, 106, false),
            item("Q6", A, std::slice::from_ref(&two)),
        ),
        (header(10, 0, 107, false), "not json".to_string()),
        (
            header(10, 0, 108, false),
            r#"{"entity":"Q6","redirect":"Q7"}"#.to_string(),
        ),
        // A talk page: not an entity namespace.
        (header(11, 1, 200, false), "hello".to_string()),
        // Page 12, Q8: the reference Q6 had, new to this page.
        (
            header(12, 0, 300, false),
            item("Q8", "Q8$22222222-2222-2222-2222-222222222222", &[one]),
        ),
    ]
}

#[test]
fn the_fold_dates_each_reference_and_lets_unreadable_revisions_stand() {
    let mut fold = PageFold::new(Some(vec![0, 120]));
    let mut out = Collected::default();
    for (h, text) in revisions() {
        fold.revision(&h, &text, &mut out).unwrap();
    }
    let got: Vec<(u64, bool, &str)> = out
        .events
        .iter()
        .map(|e| (e.revision_id, e.added, e.entity_id.as_str()))
        .collect();
    assert_eq!(
        got,
        [
            (101, true, "Q6"),  // a.example arrives
            (104, true, "Q6"),  // b.example arrives; a.example was never away (102, 103 stand)
            (106, false, "Q6"), // a.example leaves; 105 only reordered
            (108, false, "Q6"), // the redirect takes b.example
            (300, true, "Q8"),
        ]
    );
    let a = &out.events[0];
    assert_eq!(a.statement_id, A);
    assert_eq!(a.statement_property, "P31");
    assert_eq!(a.timestamp, "2020-01-01T00:00:41Z");
    // The same content has the same hash on both pages, and leaves under the hash it came with.
    assert_eq!(out.events[2].reference_hash, a.reference_hash);
    assert_eq!(out.events[4].reference_hash, a.reference_hash);
    assert_eq!(out.events[2].statement_property, "P31");
    assert_eq!(a.reference_hash.len(), 40);
    assert_ne!(a.reference_hash, "ignored");

    // Snaks once per page per reference: a (3 snaks) and b (3) on page 10, a again on page 12.
    assert_eq!(out.snaks.len(), 9);
    let first: Vec<_> = out.snaks[..3]
        .iter()
        .map(|s| {
            (
                s.snak_order,
                s.property.as_str(),
                s.snaktype,
                s.value_type.as_deref(),
                s.value.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        first,
        [
            (
                0,
                "P854",
                "value",
                Some("string"),
                Some("https://a.example/")
            ),
            (1, "P248", "value", Some("wikibase-entityid"), Some("Q5")),
            (2, "P248", "somevalue", None, None),
        ]
    );

    let c = fold.finish();
    assert_eq!(
        (
            c.entities,
            c.redirects,
            c.other_namespace,
            c.hidden,
            c.empty,
            c.legacy,
            c.unparsed
        ),
        (6, 1, 1, 1, 0, 1, 1)
    );
    assert_eq!((c.added, c.removed, c.snaks), (3, 2, 9));
    assert_eq!(c.unparsed_samples[0].0, 107);
}

#[test]
fn a_part_of_a_split_page_starts_from_a_baseline() {
    let one = url_ref("https://a.example/");
    let two = url_ref("https://b.example/");
    let mut fold = PageFold::new(None);
    let mut out = Collected::default();
    // The page's first revision here has a parent, and is hidden; the state it starts
    // from is the first one read.
    let mut first = header(10, 0, 500, true);
    first.parent_rev_id = Some(499);
    fold.revision(&first, "", &mut out).unwrap();
    let mut second = header(10, 0, 501, false);
    second.parent_rev_id = Some(500);
    fold.revision(
        &second,
        &item("Q6", A, std::slice::from_ref(&one)),
        &mut out,
    )
    .unwrap();
    let mut third = header(10, 0, 502, false);
    third.parent_rev_id = Some(501);
    fold.revision(&third, &item("Q6", A, &[one, two]), &mut out)
        .unwrap();
    let got: Vec<_> = out
        .events
        .iter()
        .map(|e| (e.revision_id, e.added, e.baseline))
        .collect();
    assert_eq!(got, [(501, true, true), (502, true, false)]);
    assert_eq!(fold.finish().baseline, 1);
}

#[test]
fn a_bundle_not_grouped_by_page_is_refused() {
    let mut fold = PageFold::new(None);
    let mut out = Collected::default();
    fold.revision(&header(1, 0, 1, false), &item("Q1", "Q1$a", &[]), &mut out)
        .unwrap();
    fold.revision(&header(2, 0, 2, false), &item("Q2", "Q2$a", &[]), &mut out)
        .unwrap();
    let e = fold
        .revision(&header(1, 0, 3, false), &item("Q1", "Q1$a", &[]), &mut out)
        .unwrap_err();
    assert!(matches!(
        e,
        FoldError::NotGrouped {
            page_id: 1,
            revision_id: 3
        }
    ));
}

#[test]
fn the_binary_writes_both_datasets_from_a_bundle() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = dir.path().join("wikidatawiki-test-p1p12.mwrev.zst");
    let mut w = BundleWriter::create(&bundle).unwrap();
    for (h, text) in revisions() {
        w.write_revision(&h, &text).unwrap();
    }
    w.finish().unwrap();
    let out = dir.path().join("staging");

    for _ in 0..2 {
        // Twice: a re-run replaces the bundle's files.
        let run = Command::new(env!("CARGO_BIN_EXE_wikidata-refs"))
            .arg("extract")
            .arg(&bundle)
            .arg("-o")
            .arg(&out)
            .output()
            .unwrap();
        assert!(
            run.status.success(),
            "{}",
            String::from_utf8_lossy(&run.stderr)
        );
        let summary: serde_json::Value = serde_json::from_slice(&run.stdout).unwrap();
        assert_eq!(summary["references"]["added"], 3);
        assert_eq!(summary["revisions"]["hidden"], 1);
        assert_eq!(summary["revisions"]["legacy"], 1);
    }

    let events = out.join("wikidata_reference_events/wikidatawiki-test-p1p12.parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&events).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<_> = reader.map(Result::unwrap).collect();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
    let b = &batches[0];
    assert_eq!(b.schema(), wikidata_refs::sink::events_schema());
    let col = |i: usize| b.column(i).clone();
    assert_eq!(
        col(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        10
    );
    assert_eq!(
        col(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(4),
        "Q8"
    );
    assert_eq!(
        col(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        101
    );
    // 2020-01-01T00:00:41Z
    assert_eq!(
        col(3)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0),
        1_577_836_841_000
    );
    assert!(
        !col(7)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(2)
    );

    let snaks = out.join("wikidata_reference_snaks/wikidatawiki-test-p1p12.parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&snaks).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<_> = reader.map(Result::unwrap).collect();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 9);
    assert_eq!(batches[0].schema(), wikidata_refs::sink::snaks_schema());
    assert!(batches[0].column(6).is_null(2), "somevalue has no value");
    assert!(
        !out.join("wikidata_reference_events/wikidatawiki-test-p1p12.parquet.tmp")
            .exists()
    );
}

/// `tests/schema.json` is the copy of the two staging schemas that
/// `packages/schema/tests/test_wikidata_refs_schema_parity.py` holds `wcd_schema.staging` to.
#[test]
fn the_schemas_are_the_ones_wcd_schema_defines() {
    let want: serde_json::Value = serde_json::from_str(include_str!("schema.json")).unwrap();
    for (name, schema) in [
        (
            wikidata_refs::sink::EVENTS,
            wikidata_refs::sink::events_schema(),
        ),
        (
            wikidata_refs::sink::SNAKS,
            wikidata_refs::sink::snaks_schema(),
        ),
    ] {
        let got: Vec<serde_json::Value> = schema
            .fields()
            .iter()
            .map(|f| {
                let ty = match f.data_type() {
                    arrow::datatypes::DataType::Int32 => "int32",
                    arrow::datatypes::DataType::Int64 => "int64",
                    arrow::datatypes::DataType::Utf8 => "string",
                    arrow::datatypes::DataType::Boolean => "bool",
                    // Stored in milliseconds, as pyarrow stores a `timestamp[s]` column.
                    arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Millisecond,
                        None,
                    ) => "timestamp[s]",
                    other => panic!("{other}"),
                };
                serde_json::json!([f.name(), ty])
            })
            .collect();
        assert_eq!(serde_json::Value::Array(got), want[name], "{name}");
    }
}
