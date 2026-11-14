"""For each 'iabot_earlier' link: the wikitext of the revision IABot dated it to, and
the text around each match of IABot's pattern, to see what the pattern matched.

    explain_disagreements.py CLASSIFIED.json OUT.json   (CLASSIFIED.json: classify_agreement.py output)
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import iabot_get_times_added_port as ia  # noqa: E402

d = json.load(open(sys.argv[1]))
out = []
c = ia.Client()
for r in d["residual"]:
    if r["class"] != "iabot_earlier":
        continue
    data = c.get({"action": "query", "prop": "revisions", "format": "json", "pageids": r["page_id"],
                  "rvprop": "ids|timestamp|content", "rvslots": "*", "rvlimit": 1, "rvdir": "older",
                  "rvstart": r["iabot"]}, "diag")
    rev = next(iter(data["query"]["pages"].values()))["revisions"][0]
    text = ia.main_text(rev) or ""
    pat = ia.url_pattern(r["url"])
    ctx = []
    for m in pat.finditer(text):
        a, b = max(0, m.start() - 60), min(len(text), m.end() + 60)
        ctx.append(text[a:m.start()] + "[[" + text[m.start():m.end()] + "]]" + text[m.end():b])
    nxt = text[ctx and pat.search(text).end() or 0: (pat.search(text).end() + 1) if pat.search(text) else 0]
    out.append({"title": r["title"], "url": r["url"], "iabot": r["iabot"], "wcd_first": r["wcd_first"],
                "revid": rev["revid"], "n_matches": len(ctx), "contexts": ctx[:3]})
    print("=" * 100)
    print(r["title"], "|", r["url"], "| iabot", r["iabot"], "rev", rev["revid"], "| wcd", r["wcd_first"],
          "| matches", len(ctx))
    for x in ctx[:2]:
        print("   ...", x.replace("\n", " ")[:300])
json.dump(out, open(sys.argv[2], "w"), indent=1)
