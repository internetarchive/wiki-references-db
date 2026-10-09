# UVA Wikipedia Citations Database — reference documentation

Copied verbatim from scatter-llc/UVAWikipediaCitationsDatabase @ 29f3ae319d54c753843f3e0b54a1a85c32e872b0
(the commit whose logic is vendored in `packages/semantic`). These describe
the UVA pipeline as it was — DuckDB tables, stage scripts, Rivanna/Slurm
operation — and are kept as the reference for the ported code; the unified
design is in `docs/design/`.

| File | What it documents |
|---|---|
| README.md | project overview, pipeline stages 1–6, frontend, design decisions |
| pipeline.md, data_model.md | the DuckDB pipeline and its tables |
| extract.md | the UVA extractor (ref parsing half → `wcd_semantic.content`) |
| normalizers.md | ref-tag and template normalizers (→ `wcd_semantic.reftag`, `.template`) |
| citation_matching.md | the four matching stages (→ `wcd_semantic.match_*`, `.merge_types`) |
| citation_history.md | page_source_summary / page_revision_stats builds (reference for Stages 7d and 8) |

To regenerate `fixtures/golden/semantic/expected.json`, check the UVA
repository out at the commit above and run
`uv run --with orjson --with zstandard python packages/semantic/tests/gen_golden.py <checkout>/src`.
