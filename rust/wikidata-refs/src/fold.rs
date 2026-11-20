//! The fold over one bundle's revisions.

use std::collections::{BTreeSet, HashMap, HashSet};

use mwrev::RevisionHeader;
use scatter_wikibase_changeset::refs::{ReferenceKey, ReferenceUses};
use scatter_wikibase_model::entity::Entity;
use scatter_wikibase_model::hash::Hasher;
use scatter_wikibase_model::statement::{Reference, SnakKind};
use scatter_wikibase_model::value::DataValue;
use serde_json::Value;

/// A reference arriving on a statement, or leaving it, in one revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// The entity's page.
    pub page_id: u64,
    /// The entity, as the revision names it (`Q42`).
    pub entity_id: String,
    /// The revision that made the change.
    pub revision_id: u64,
    /// The revision's timestamp, as the bundle wrote it.
    pub timestamp: String,
    /// The statement: its GUID with the subject in canonical case.
    pub statement_id: String,
    /// The property of the statement's main snak.
    pub statement_property: String,
    /// Wikibase's reference hash, recomputed from the snaks.
    pub reference_hash: String,
    /// Added (`true`) or removed (`false`).
    pub added: bool,
    /// The reference was already there when this bundle's part of the page's history
    /// began: the page's first revision here has a parent, so its history starts in
    /// another bundle (the exports split very large pages by revision range), and what
    /// that revision holds is where this part starts from, not what the revision added.
    /// Always an addition. Whoever joins the parts sets these rows against the state
    /// the earlier part ended in.
    pub baseline: bool,
}

/// One snak of a reference, written the first time a page shows the reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnakRow {
    /// The page the reference was seen on.
    pub page_id: u64,
    /// The reference.
    pub reference_hash: String,
    /// The snak's position in the reference, from 0.
    pub snak_order: u32,
    /// The snak's property.
    pub property: String,
    /// `value`, `somevalue` or `novalue`.
    pub snaktype: &'static str,
    /// The data value's type (`string`, `wikibase-entityid`, `time`, …); absent without a value.
    pub value_type: Option<String>,
    /// The value: a string as it is, an entity value as its ID, anything else as the
    /// JSON of its `value`. Absent without a value.
    pub value: Option<String>,
}

/// Where the fold's rows go.
pub trait Sink {
    /// One reference added or removed.
    fn event(&mut self, event: Event) -> Result<(), SinkError>;
    /// One snak of a reference new to its page.
    fn snak(&mut self, snak: SnakRow) -> Result<(), SinkError>;
}

/// A sink could not take a row.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SinkError(pub String);

/// Rows kept in memory, for tests and small inputs.
#[derive(Debug, Default)]
pub struct Collected {
    /// The events, in order.
    pub events: Vec<Event>,
    /// The snaks, in order.
    pub snaks: Vec<SnakRow>,
}

impl Sink for Collected {
    fn event(&mut self, event: Event) -> Result<(), SinkError> {
        self.events.push(event);
        Ok(())
    }
    fn snak(&mut self, snak: SnakRow) -> Result<(), SinkError> {
        self.snaks.push(snak);
        Ok(())
    }
}

/// What the fold made of the revisions it was given.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counts {
    /// Revisions read as an entity.
    pub entities: u64,
    /// Revisions that are redirects: the entity has no reference from there on.
    pub redirects: u64,
    /// Revisions in a namespace the fold was not asked for.
    pub other_namespace: u64,
    /// Revisions whose text the wiki has hidden; the state before them stands.
    pub hidden: u64,
    /// Revisions with no text that are not marked hidden; the state before them stands.
    pub empty: u64,
    /// Revisions in Wikibase's serialization of before 2014-2015 (`"entity"` and no
    /// `"type"`), which this reader does not read; the state before them stands.
    pub legacy: u64,
    /// Revisions that are not JSON or not an entity; the state before them stands.
    pub unparsed: u64,
    /// The first few unparsed revisions, with the reason.
    pub unparsed_samples: Vec<(u64, String)>,
    /// References added, baseline rows included.
    pub added: u64,
    /// Of those, the ones a part of a split page started from.
    pub baseline: u64,
    /// References removed.
    pub removed: u64,
    /// Snak rows written.
    pub snaks: u64,
}

impl Counts {
    /// Adds another bundle's counts.
    pub fn merge(&mut self, other: Counts) {
        self.entities += other.entities;
        self.redirects += other.redirects;
        self.other_namespace += other.other_namespace;
        self.hidden += other.hidden;
        self.empty += other.empty;
        self.legacy += other.legacy;
        self.unparsed += other.unparsed;
        self.added += other.added;
        self.baseline += other.baseline;
        self.removed += other.removed;
        self.snaks += other.snaks;
        for s in other.unparsed_samples {
            if self.unparsed_samples.len() < MAX_SAMPLES {
                self.unparsed_samples.push(s);
            }
        }
    }
}

const MAX_SAMPLES: usize = 10;

/// The fold could not go on.
#[derive(Debug, thiserror::Error)]
pub enum FoldError {
    /// A page's revisions are not together: the bundle is not one `RevisionChest build`
    /// wrote from a dump (a `sync` bundle is in time order), so a revision's
    /// predecessor is not the one before it.
    #[error(
        "page {page_id} comes back at revision {revision_id} after other pages: the bundle is not grouped by page"
    )]
    NotGrouped {
        /// The page.
        page_id: u64,
        /// The revision at which it came back.
        revision_id: u64,
    },
    /// The sink failed.
    #[error(transparent)]
    Sink(#[from] SinkError),
}

/// The fold: feed it a bundle's revisions in order.
pub struct PageFold {
    namespaces: Option<Vec<i64>>,
    hasher: Hasher,
    page_id: Option<u64>,
    done: HashSet<u64>,
    entity_id: String,
    /// The previous state's references.
    keys: BTreeSet<ReferenceKey>,
    /// The main property of each statement that has had a reference on this page.
    properties: HashMap<String, String>,
    /// The references whose snaks this page has written.
    written: HashSet<String>,
    /// The page's history began before this bundle and no state of it has been read yet.
    baseline: bool,
    counts: Counts,
}

impl PageFold {
    /// A fold over the given namespaces; `None` takes every namespace.
    #[must_use]
    pub fn new(namespaces: Option<Vec<i64>>) -> Self {
        Self {
            namespaces,
            hasher: Hasher::local(),
            page_id: None,
            done: HashSet::new(),
            entity_id: String::new(),
            keys: BTreeSet::new(),
            properties: HashMap::new(),
            written: HashSet::new(),
            baseline: false,
            counts: Counts::default(),
        }
    }

    /// The counts so far.
    #[must_use]
    pub fn counts(&self) -> &Counts {
        &self.counts
    }

    /// Ends the fold and returns its counts.
    #[must_use]
    pub fn finish(self) -> Counts {
        self.counts
    }

    /// One revision, in bundle order.
    pub fn revision(
        &mut self,
        header: &RevisionHeader,
        text: &str,
        sink: &mut impl Sink,
    ) -> Result<(), FoldError> {
        if self
            .namespaces
            .as_ref()
            .is_some_and(|ns| !ns.contains(&header.ns))
        {
            self.counts.other_namespace += 1;
            return Ok(());
        }
        if self.page_id != Some(header.page_id) {
            if let Some(previous) = self.page_id {
                self.done.insert(previous);
            }
            if self.done.contains(&header.page_id) {
                return Err(FoldError::NotGrouped {
                    page_id: header.page_id,
                    revision_id: header.rev_id,
                });
            }
            self.page_id = Some(header.page_id);
            self.entity_id.clear();
            self.keys.clear();
            self.properties.clear();
            self.written.clear();
            self.baseline = header.parent_rev_id.is_some();
        }
        if header.text_hidden {
            self.counts.hidden += 1;
            return Ok(());
        }
        if text.trim().is_empty() {
            self.counts.empty += 1;
            return Ok(());
        }
        let value: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                self.unparsed(header.rev_id, &e.to_string());
                return Ok(());
            }
        };
        if value.get("redirect").is_some() {
            self.counts.redirects += 1;
            if self.entity_id.is_empty()
                && let Some(id) = value.get("entity").and_then(Value::as_str)
            {
                self.entity_id = id.to_string();
            }
            return self.emit(header, None, sink);
        }
        if value.get("type").is_none() && value.get("entity").is_some() {
            self.counts.legacy += 1;
            return Ok(());
        }
        match Entity::from_value(value) {
            Ok(parsed) => {
                self.counts.entities += 1;
                self.entity_id = parsed.entity.id.as_str().to_string();
                self.emit(header, Some(&parsed.entity), sink)
            }
            Err(e) => {
                self.unparsed(header.rev_id, &e.to_string());
                Ok(())
            }
        }
    }

    fn unparsed(&mut self, revision_id: u64, why: &str) {
        self.counts.unparsed += 1;
        if self.counts.unparsed_samples.len() < MAX_SAMPLES {
            let why: String = why.chars().take(200).collect();
            self.counts.unparsed_samples.push((revision_id, why));
        }
    }

    fn emit(
        &mut self,
        header: &RevisionHeader,
        entity: Option<&Entity>,
        sink: &mut impl Sink,
    ) -> Result<(), FoldError> {
        let uses = entity
            .map(|e| ReferenceUses::of(e, &self.hasher))
            .unwrap_or_default();
        for key in uses.removed_since(&self.keys) {
            self.counts.removed += 1;
            sink.event(Event {
                page_id: header.page_id,
                entity_id: self.entity_id.clone(),
                revision_id: header.rev_id,
                timestamp: header.timestamp.clone(),
                statement_id: key.statement.clone(),
                statement_property: self
                    .properties
                    .get(&key.statement)
                    .cloned()
                    .unwrap_or_default(),
                reference_hash: key.hash.clone(),
                added: false,
                baseline: false,
            })?;
        }
        for (key, used) in uses.added_since(&self.keys) {
            self.counts.added += 1;
            self.counts.baseline += u64::from(self.baseline);
            let property = used.property.as_str();
            if !self.properties.contains_key(&key.statement) {
                self.properties
                    .insert(key.statement.clone(), property.to_string());
            }
            sink.event(Event {
                page_id: header.page_id,
                entity_id: self.entity_id.clone(),
                revision_id: header.rev_id,
                timestamp: header.timestamp.clone(),
                statement_id: key.statement.clone(),
                statement_property: property.to_string(),
                reference_hash: key.hash.clone(),
                added: true,
                baseline: self.baseline,
            })?;
            if self.written.insert(key.hash.clone()) {
                for row in snak_rows(header.page_id, &key.hash, used.reference) {
                    self.counts.snaks += 1;
                    sink.snak(row)?;
                }
            }
        }
        self.keys = uses.keys();
        self.baseline = false;
        Ok(())
    }
}

/// A reference's snaks as rows, in the reference's order.
fn snak_rows(page_id: u64, hash: &str, reference: &Reference) -> Vec<SnakRow> {
    reference
        .snaks
        .values()
        .flatten()
        .zip(0u32..)
        .map(|(snak, snak_order)| {
            let (value_type, value) = match &snak.kind {
                SnakKind::Value(v) => (Some(v.value_type_name().to_string()), Some(value_text(v))),
                SnakKind::SomeValue | SnakKind::NoValue => (None, None),
            };
            SnakRow {
                page_id,
                reference_hash: hash.to_string(),
                snak_order,
                property: snak.property.as_str().to_string(),
                snaktype: snak.kind.snaktype(),
                value_type,
                value,
            }
        })
        .collect()
}

fn value_text(value: &DataValue) -> String {
    match value {
        DataValue::String(s) => s.clone(),
        other => {
            if let Some(id) = other.entity_id() {
                return id.as_str().to_string();
            }
            match serde_json::to_value(other) {
                Ok(Value::Object(mut wire)) => wire
                    .remove("value")
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                _ => String::new(),
            }
        }
    }
}
