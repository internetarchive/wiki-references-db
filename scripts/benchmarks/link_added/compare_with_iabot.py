"""Run the IABot getTimesAdded port on the sample's articles and compare with WCD.

    compare_with_iabot.py LOOKUP.json OUT.json MAX_LINKS [page_id ...]   (LOOKUP.json: lookup.py output)

For each article with 1..max_links current links (or the listed page ids): all its
current URLs (as WCD stores them) go to one getTimesAdded call, as IABot does on a
first visit. Each result is compared with WCD's first-added and current-run
timestamps. For a URL IABot finds nothing for, one extra request (not counted in
IABot's cost) checks whether its pattern matches the latest revision's text at all,
i.e. whether the URL string differs between WCD and the wikitext.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import iabot_get_times_added_port as ia  # noqa: E402


def norm_wcd(ts: str) -> str:
    return ts.replace(" ", "T")[:19] + "Z"


def latest_text(page_id: int, rev_id: int):
    c = ia.Client()
    d = c.get({"action": "query", "prop": "revisions", "format": "json", "rvprop": "content", "rvslots": "*",
               "revids": rev_id}, "diag")
    for page in d.get("query", {}).get("pages", {}).values():
        for rev in page.get("revisions", []):
            return ia.main_text(rev)
    return None


def main():
    data = json.load(open(sys.argv[1]))
    out_path, max_links = sys.argv[2], int(sys.argv[3])
    only = {int(x) for x in sys.argv[4:]}
    arts = [a for a in data if (a["page_id"] in only) if only] or \
           [a for a in data if 0 < a["n_links"] <= max_links]
    results = []
    for a in arts:
        urls = [l["url"] for l in a["links"]]
        r = ia.run_article(a["page_id"], a["latest_rev_id"], urls)
        links, lt = [], None
        for link, t in zip(a["links"], r["times"]):
            first, cur = norm_wcd(link["first_ts"]), norm_wcd(link["current_ts"])
            rec = {"url": link["url"], "iabot": t, "wcd_first": first, "wcd_current": cur, "n_runs": link["n_runs"]}
            if t is None:
                if lt is None:
                    lt = latest_text(a["page_id"], a["latest_rev_id"]) or ""
                rec["in_latest_text"] = ia.url_pattern(link["url"]).search(lt) is not None
                rec["verdict"] = "iabot_none" if rec["in_latest_text"] else "url_form_differs"
            elif t == first:
                rec["verdict"] = "match_first"
            elif t == cur:
                rec["verdict"] = "match_current_run"
            else:
                rec["verdict"] = "differs"
            links.append(rec)
        counts = {}
        for l in links:
            counts[l["verdict"]] = counts.get(l["verdict"], 0) + 1
        res = {"page_id": a["page_id"], "title": a["title"], "stratum": a["stratum"],
               "revision_count": a["revision_count"], "n_links": a["n_links"], "seconds": r["seconds"],
               "requests": r["requests"], "bot_requests": r["bot_requests"], "bytes": r["bytes"], "by_phase": r["by_phase"],
               "history_len": r["history_len"], "bot_history_requests": r["bot_history_requests"],
               "wcd_cold": a["t_cold"], "wcd_warm": a["t_warm"], "verdicts": counts, "links": links}
        results.append(res)
        print(f"{a['page_id']:>9} {str(a['title'])[:34]:34} revs {a['revision_count']:>6} links {a['n_links']:>4} "
              f"iabot {r['seconds']:7.1f}s req {r['requests']:>4} (bot {r['bot_requests']:>3}) {r['bytes'] / 1e6:7.1f} MB | wcd cold "
              f"{a['t_cold']:.2f}s | {counts}", flush=True)
        json.dump(results, open(out_path, "w"), indent=1)
        time.sleep(1)


if __name__ == "__main__":
    main()
