"""Check WCD's first-added revisions against Wikipedia's text: the URL (IABot's pattern,
any scheme) should be in WCD's first revision and absent from the revision before it.

    verify_against_wikipedia.py LOOKUP.json CLASSIFIED.json OUT.json [n_random]   (CLASSIFIED.json: classify_agreement.py output)
"""
import json
import random
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import iabot_get_times_added_port as ia  # noqa: E402

links50 = json.load(open(sys.argv[1]))
an = json.load(open(sys.argv[2]))
n_random = int(sys.argv[4]) if len(sys.argv) > 4 else 40
by_page = {a["page_id"]: a for a in links50}
picks = [(r["page_id"], r["url"], "iabot_later") for r in an["residual"] if r["class"] == "iabot_later"]
pool = [(a["page_id"], l["url"], "random") for a in links50 for l in a["links"]]
random.Random(7).shuffle(pool)
picks += pool[:n_random]
c = ia.Client()
out = []
for page_id, url, why in picks:
    link = next(l for l in by_page[page_id]["links"] if l["url"] == url)
    rev = link["first_rev"]
    d = c.get({"action": "query", "prop": "revisions", "format": "json", "revids": rev,
               "rvprop": "ids|content", "rvslots": "*"}, "verify")
    r0 = next(iter(d["query"]["pages"].values()))["revisions"][0]
    parent = r0.get("parentid", 0)
    at = ia.url_pattern(url).search(ia.main_text(r0) or "") is not None
    before = None
    if parent:
        d2 = c.get({"action": "query", "prop": "revisions", "format": "json", "revids": parent,
                    "rvprop": "ids|content", "rvslots": "*"}, "verify")
        r1 = next(iter(d2["query"]["pages"].values()))["revisions"][0]
        before = ia.url_pattern(url).search(ia.main_text(r1) or "") is not None
    rec = {"why": why, "page_id": page_id, "title": by_page[page_id]["title"], "url": url, "first_rev": rev,
           "parent": parent, "present_at_first": at, "present_before": before}
    out.append(rec)
    print(f"{why:12} {str(rec['title'])[:28]:28} rev {rev} present {at} before {before} | {url[:70]}", flush=True)
json.dump(out, open(sys.argv[3], "w"), indent=1)
ok = sum(1 for r in out if r["present_at_first"] and not r["present_before"])
print("confirmed", ok, "of", len(out))
