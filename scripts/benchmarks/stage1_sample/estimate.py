"""Extrapolate Stage 1 CPU from the stratified sample. Usage: estimate.py RUN_DIR"""
import csv, random, sys
run = sys.argv[1]
res = {r[0]: (float(r[1]), float(r[2]) + float(r[3]), int(r[4])) for r in csv.reader(open(f"{run}/results.tsv"), delimiter="\t")}
size = {r[0]: (int(r[1]), int(r[2])) for r in csv.reader(open("bundle-sizes.tsv"), delimiter="\t")}
pop = {r[0]: (int(float(r[1])), int(float(r[2])), int(float(r[3]))) for r in list(csv.reader(open("strata.csv")))[1:]}
by = {}
for name, (wall, cpu, rss) in res.items():
    by.setdefault(name.split("-")[0], []).append((cpu, *size[name], rss))
random.seed(1)
B = 2000
def est(rows, key, popval):
    return sum(r[0] for r in rows) / sum(r[key] for r in rows) * popval / 3600
tot, boots = 0.0, [0.0] * B
print("stratum bundles   cpu-s  MB/cpu-s revs/cpu-s |  pop TB          revs |  core-h (basis)  90% CI")
for s in sorted(by):
    rows = by[s]; _, p_revs, p_bytes = pop[s]
    cpu = sum(r[0] for r in rows); revs = sum(r[1] for r in rows); text = sum(r[2] for r in rows)
    key, pv, basis = (1, p_revs, "revs") if s in ("s3", "s4") else (2, p_bytes, "bytes")
    e = est(rows, key, pv)
    bs = [est([random.choice(rows) for _ in rows], key, pv) for _ in range(B)]
    boots = [a + b for a, b in zip(boots, bs)]
    bs.sort(); tot += e
    print(f"{s:7s} {len(rows):5d} {cpu:9.0f} {text/1e6/cpu:9.2f} {revs/cpu:9.0f} | {p_bytes/1e12:7.3f} {p_revs:13,d} | {e:8,.0f} ({basis})  {bs[B//20]:,.0f}-{bs[-B//20]:,.0f}")
boots.sort()
print(f"TOTAL: {tot:,.0f} core-hours (90% CI {boots[B//20]:,.0f}-{boots[-B//20]:,.0f}); on 28 cores {tot/28:,.0f} h = {tot/28/24:.1f} days")
print(f"peak RSS per worker {max(r[3] for rows in by.values() for r in rows)/1024:.0f} MB")
