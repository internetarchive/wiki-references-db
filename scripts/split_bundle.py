#!/usr/bin/env python3
"""Split one ``.mwrev.zst`` bundle into N bundles on page boundaries
(``wcd_pipeline.split_bundle``; ``wcd setup`` uses it on its own).

    uv run python scripts/split_bundle.py fixture.xml.mwrev.zst -n 4 -o shards/
    # → shards/fixture.xml-0.mwrev.zst … fixture.xml-3.mwrev.zst
"""
from wcd_pipeline.split_bundle import main, piece_paths, split  # noqa: F401

if __name__ == "__main__":
    raise SystemExit(main())
