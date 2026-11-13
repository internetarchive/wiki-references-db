"""How many of the URLs in an article's wikitext does WCD know as current links?

    wikitext_coverage.py LOOKUP.json OUT.json [--visible]

For each article of LOOKUP.json (lookup.py's output): the wikitext of the dump's latest revision; every http(s) URL in it
(a plain regex: scheme to the first space, ], [, <, >, ", |, {, }, and trailing
punctuation stripped; protocol-relative //host URLs inside [ ] links too); compared
with WCD's current links, scheme-insensitively. URLs inside a web.archive.org
archive URL count as part of that archive URL only. --visible first removes HTML
comments (<!-- ... -->): on the enwiki sample, every URL WCD did not return was
inside one.
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import iabot_get_times_added_port as ia  # noqa: E402

URL_RE = re.compile(r"(?:https?:)?//[^\s\]\[<>\"|{}]+", re.I)
SCHEME = re.compile(r"^(?:https?:)?//", re.I)


def key(u: str) -> str:
    return SCHEME.sub("", u).rstrip(".,;:)'")


def text_urls(text: str) -> set[str]:
    out = set()
    for m in URL_RE.finditer(text):
        u = m.group(0)
        if u.startswith("//") and (m.start() == 0 or text[m.start() - 1] != "["):
            continue                          # bare // only counts inside [ ] external links
        # an archive URL contains its original after /web/<ts>/: keep the whole archive URL only
        out.add(key(u))
    return {u for u in out if "." in u.split("/")[0]}


def main():
    visible = "--visible" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--visible"]
    data = json.load(open(args[0]))
    c = ia.Client()
    res = []
    for a in data:
        d = c.get({"action": "query", "prop": "revisions", "format": "json", "revids": a["latest_rev_id"],
                   "rvprop": "content", "rvslots": "*"}, "coverage")
        text = ia.main_text(next(iter(d["query"]["pages"].values()))["revisions"][0]) or ""
        if visible:
            text = re.sub(r"<!--.*?(-->|$)", "", text, flags=re.S)
        in_text = text_urls(text)
        wcd = {key(l["url"]) for l in a["links"]}
        missing = sorted(u for u in in_text if u not in wcd and not any(w.startswith(u) or u.startswith(w) for w in wcd))
        partial = sorted(u for u in in_text if u not in wcd and u not in missing)
        rec = {"page_id": a["page_id"], "title": a["title"], "text_urls": len(in_text), "wcd_links": len(wcd),
               "exact": len(in_text & wcd), "prefix_only": len(partial), "missing": len(missing),
               "missing_examples": missing[:8], "wcd_not_in_text": len(wcd - in_text)}
        res.append(rec)
        print(f"{str(a['title'])[:34]:34} text {len(in_text):>4} wcd {len(wcd):>4} exact {rec['exact']:>4} "
              f"prefix {len(partial):>3} missing {len(missing):>3}  {missing[:2]}", flush=True)
    json.dump(res, open(args[1], "w"), indent=1)
    tot = {k: sum(r[k] for r in res) for k in ("text_urls", "exact", "prefix_only", "missing", "wcd_not_in_text")}
    print(tot)


if __name__ == "__main__":
    main()
