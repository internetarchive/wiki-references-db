//! Stage 1 for a Wikibase wiki (Wikidata): what the citation extractor is for a
//! Wikipedia, for entities.
//!
//! A RevisionChest bundle holds every revision of each entity page, a page's revisions
//! together and in order. [`fold::PageFold`] reads them one at a time, keeps only the
//! keys of the previous revision's references, and emits a [`fold::Event`] for every
//! reference a revision added to a statement or removed from one, with the reference's
//! snaks ([`fold::SnakRow`]) the first time a page shows that reference. [`sink`] writes
//! both as staging Parquet, one file of each per bundle.
//!
//! The reading of an entity is `scatter-wikibase-model`'s and the diff is
//! `scatter_wikibase_changeset::refs`: a reference is identified by its statement and by
//! Wikibase's reference hash recomputed from its snaks.

#![forbid(unsafe_code)]

pub mod fold;
pub mod sink;
