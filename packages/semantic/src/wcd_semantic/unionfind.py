"""Union-find with best-edge tracking.

Vendored from UVAWikipediaCitationsDatabase (scatter-llc, commit 29f3ae319d54),
``src/citation_matching/*.py``: the UnionFind class, identical in all four matcher modules. The DuckDB loading/writing half of that module is
not carried over; wcd_pipeline supplies the inputs from Parquet instead.
Logic is kept verbatim so the golden tests in ``tests/`` can hold it to the
upstream behaviour; see NOTICE for the provenance rules.
"""
from __future__ import annotations


class UnionFind:
    def __init__(self, keys):
        self.parent = {k: k for k in keys}
        self.rank = {k: 0 for k in keys}
        self.edge_info = {}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x, y, match_type, confidence):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1
        current = self.edge_info.get(rx, (match_type, 0))
        if confidence > current[1]:
            self.edge_info[rx] = (match_type, confidence)

    def components(self):
        groups = {}
        for k in self.parent:
            root = self.find(k)
            groups.setdefault(root, []).append(k)
        return groups
