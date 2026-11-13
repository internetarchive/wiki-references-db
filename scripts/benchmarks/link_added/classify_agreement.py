"""Classify IABot-port answers against WCD's (exact URL and scheme-insensitive).

    classify_agreement.py COMPARE.json ANY_SCHEME.json OUT.json   (compare_with_iabot.py and any_scheme.py outputs)
"""
import json
import sys
from collections import Counter


def z(ts):
    return ts.replace(" ", "T")[:19] + "Z" if ts else None


def main():
    cmp_ = json.load(open(sys.argv[1]))
    anys = json.load(open(sys.argv[2]))
    overall = Counter()
    rows = []
    for a in cmp_:
        any_p = anys.get(str(a["page_id"]), {})
        for l in a["links"]:
            ia = l["iabot"]
            av = any_p.get(l["url"])
            a_first, a_cur = (z(av[0]), z(av[1])) if av else (None, None)
            if ia is None:
                v = l.get("verdict")
            elif ia == l["wcd_first"]:
                v = "match_first"
            elif ia == a_first:
                v = "match_first_any_scheme"
            elif ia in (l["wcd_current"], a_cur):
                v = "match_current_run"
            elif ia < l["wcd_first"]:
                v = "iabot_earlier"
            else:
                v = "iabot_later"
            l["class"] = v
            overall[v] += 1
            if v in ("iabot_earlier", "iabot_later", "iabot_none", "url_form_differs"):
                rows.append({"page_id": a["page_id"], "title": a["title"], **l, "any_first": a_first,
                             "any_current": a_cur})
    json.dump({"overall": overall, "residual": rows}, open(sys.argv[3], "w"), indent=1)
    print(dict(overall), "residual", len(rows))


if __name__ == "__main__":
    main()
