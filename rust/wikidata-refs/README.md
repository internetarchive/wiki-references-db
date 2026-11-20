# wikidata-refs

Stage 1 for a Wikibase wiki. Where the citation extractor reads an article's
wikitext, this reads an entity's JSON: it folds each entity's revisions, in the
order a RevisionChest bundle holds them, into one row for every reference a
revision **added to a statement or removed from one**. That is the history a
Wikidata citations database is built from: when a reference URL or a "stated
in" arrived on an item, and when it left.

```
RevisionChest build -d dumps/ -o bundles/ --namespace 0,120 --parquet bundles/meta.parquet
wikidata-refs extract -d bundles/ -o staging/ --jobs 8
```

Per bundle it writes `staging/wikidata_reference_events/<bundle>.parquet` and
`staging/wikidata_reference_snaks/<bundle>.parquet`, each through a `.tmp` file
renamed when complete, so a re-run replaces a bundle's files whole. The counts
are printed as JSON. The columns are defined in `wcd_schema.staging`
(`WIKIBASE_SCHEMAS`), with a copy in `tests/schema.json`.

## What a reference is

A reference is identified by **where it sits and what it says**:

- the statement, by its GUID with the subject in canonical case (a legacy
  lowercase GUID and its uppercase form are one statement);
- the content, by Wikibase's reference hash **recomputed from the snaks**, not
  the hash the revision carries. The same snaks hash the same in every revision
  and on every entity.

So reordering references or their snaks is no event, editing one snak of a
reference is a removal and an addition, and a statement that is removed takes
its references with it. The reading of an entity and the diff are
`scatter-wikibase-model` and `scatter_wikibase_changeset::refs`
(scatterbase/triplespace).

## Revisions that are not an entity

| Revision | Effect | Count |
|---|---|---|
| Text hidden by the wiki (`text_hidden=1` on the bundle header) | none: the state before it stands | `hidden` |
| A redirect (`{"entity": …, "redirect": …}`) | every reference the entity had is removed | `redirects` |
| Wikibase's serialization of before 2014-2015 (`"entity"` and no `"type"`) | none; **not read yet** | `legacy` |
| No text, or not JSON, or not an entity | none | `empty`, `unparsed` (first ten listed) |

Until legacy revisions are read, a reference that was added in one is dated to
the first later revision in the current serialization.

## Bundles it accepts

Bundles that `RevisionChest build` wrote from a history dump: a page's
revisions together and in order. A bundle in which a page comes back after
other pages (a `sync` bundle, which is in time order) is refused.

The exports split very large pages by revision range over several files, so a
page's history can start in one bundle and continue in another. The fold holds
no state between bundles. When a page's first revision in a bundle has a
parent, the references of the first state read are written with
`baseline = true`: they are what this part starts from, not what that revision
added. Joining the parts (setting each baseline against the state the earlier
part ended in) is left to the stage that consolidates these files, which does
not exist yet.

## Building

```
cargo build --release      # target/release/wikidata-refs
cargo test
```

`mwrev` comes from crates.io (0.1, which has `RevisionHeader::text_hidden`),
not from the RevisionChest submodule; the two scatter crates are git
dependencies on scatterbase/triplespace `main`, at the commit `Cargo.lock`
pins (`cargo update -p scatter-wikibase-changeset -p scatter-wikibase-model`
moves it).
