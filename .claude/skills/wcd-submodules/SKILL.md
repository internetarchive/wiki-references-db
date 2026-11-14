---
name: wcd-submodules
description: Edit the WCD git submodules (packages/extractor, packages/normalizer, rust/revisionchest) safely, from branch to commit to pointer bump, and keep them independently usable.
---

# Working in the submodules

| Path | Upstream | Package / binary |
|---|---|---|
| `packages/extractor` | internetarchive/wiki-references-extractor | `refs_extractor` (per-wiki config in `wikis.yaml`) |
| `packages/normalizer` | internetarchive/wiki-references-normalizer | `citation_normalizer` |
| `rust/revisionchest` | internetarchive/RevisionChest | Cargo workspace: `mwdump`, `mwrev`, `revisionchest-meta`, the `RevisionChest` binary |

Each submodule tracks a local `v3` branch. Each is its own repository, used
outside this monorepo too.

## Rules

* **No imports back into the monorepo.** The extractor and normalizer must not
  import `wcd_schema`, `wcd_pipeline` or anything else here; RevisionChest
  must build and test on its own. Contracts are copied and checked by tests,
  not shared: the Stage-0 DDL copy in
  `crates/revisionchest-meta/tests/schema.sql` is kept in step by
  `packages/schema/tests/test_revisionchest_ddl_parity.py`.
* The normalizer feeds `normalized_sha1`. Any change to its output changes
  hashes and means a full rebuild. Say so in both commit messages.
* The extractor decides what counts as a reference and its `citation_type`.
  Self-closing `<ref name=…/>` comes out as a reuse event, not a reference.

## Workflow

```
git submodule update --init --recursive        # after any pull
cd packages/extractor
git status && git switch v3                     # don't commit on a detached HEAD
# … edit, then run the submodule's own tests:
uv run pytest packages/extractor/tests -q       # from the repo root (the workspace includes them)
git commit -am "…"                              # inside the submodule
cd ../..
git add packages/extractor
git commit -m "extractor: <what changed, and why the pointer moved>"
```

For RevisionChest, run what CI runs, inside `rust/revisionchest`:

```
cargo build --release --workspace
cargo test --workspace            # REVISIONCHEST_TEST_DATABASE_URL=… for the Postgres sink test
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

then the Python Postgres tier with `REVISIONCHEST_BIN=$PWD/rust/revisionchest/target/release/RevisionChest`
so the Stage-0 golden is regenerated from the binary and compared. The mwdump
fixture `fixture.xml` must stay byte-exact (don't let an editor reformat it).

## Before pushing

CI checks out with `submodules: recursive`, so every submodule commit that the
superproject points at must be reachable on that submodule's remote. Push the
submodule branch first, then the superproject. At the time of writing, the
extractor's and RevisionChest's `v3` branches exist only locally. **Ask the
owner before pushing anything to an internetarchive remote.**

`git diff --submodule=log` shows which submodule commits a superproject
change brings in; use it when writing the pointer-bump commit message.
