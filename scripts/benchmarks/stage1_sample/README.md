# Stage 1 sample benchmark

Estimates the CPU a full Stage 1 (`wcd extract`) run will need from a
stratified sample of pages, before committing a machine to it. On
enwiki-20261001 it estimated ~1,200 core-hours; the full run took 1,186
(implementation plan §19).

1. **Choose the sample.** Group pages by the total size of their history into
   strata (§19 used s3–s9, by order of magnitude) and draw pages from each.
   Record each stratum's population in `strata.csv`
   (`stratum,pages,revs,bytes`).
2. **Cut the sample's page histories out of the bundles** with
   `cut_pages.py BUNDLE RANGES.tsv OUT_DIR`. A page's revisions are contiguous
   in its RevisionChest bundle, so a byte range (from the Stage-0 metadata
   offsets) copies the whole history. `RANGES.tsv` rows are
   `group, page_id, first_offset, end_offset`. Recompress each group's parts
   into one `.mwrev.zst` per sample bundle, and list each bundle's revisions
   and text bytes in `bundle-sizes.tsv` (`name, revisions, bytes`).
3. **Extract each sample bundle, timed:** `run_one.sh BUNDLE RESULTS_DIR`.
4. **Extrapolate:** from the folder with `strata.csv` and `bundle-sizes.tsv`,
   run `estimate.py RESULTS_DIR`. CPU per revision (small strata) or per byte
   of text (large strata) is scaled to each stratum's population, with a
   bootstrap 90% interval and the peak RSS per worker.
