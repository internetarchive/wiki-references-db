"""wcd_semantic — the semantic layer of the Wikipedia Citations Database.

Pure functions, no I/O: classify citation content, extract bibliographic
fields and identifiers, group citations into sources within a page and
across pages, and score sources. Vendored from the UVA Wikipedia Citations
Database project (see NOTICE); wcd_pipeline wires these to Parquet/Postgres.

    content       which kind of citation a ref's inner content is; bullet-block split; footnote filter
    reftag        ref-tag normalizer   (Stage 4, part_type = ref_tag)
    template      template normalizer  (Stage 4, part_type = template)
    signals       per-revision structural flags (Stage 1)
    match_reftag  per-page ref-tag source matching  (Stage 6a)
    match_template per-page template source matching (Stage 6b)
    merge_types   per-page cross-type merge          (Stage 6c)
    match_global  cross-page matching                (Stage 6d)
    scoring       reliability score + verdict        (Stage 7f)
    profile       page source-profile classification (Stage 7h)
"""
