# SPDX-License-Identifier: AGPL-3.0
#
# This file is a port of code from InternetArchiveBot, Copyright (c) 2015-2026
# Maximilian Doerr (Cyberpower678), Internet Archive, licensed under the GNU Affero
# General Public License v3.0 (https://www.gnu.org/licenses/agpl-3.0.txt). As a
# derivative work it is under the AGPL-3.0 too, unlike the rest of this repository
# (GPL-3.0). It is a benchmark aid and is not imported by any package.
"""InternetArchiveBot's APII::getTimesAdded() (API path), ported to Python.

Used by ``compare_with_iabot.py`` to time IABot's own way of finding when a link
was added, and to compare its answers with WCD's.

Source: internetarchive/internetarchivebot master, app/src/Core/APII.php
(getPageHistory, getTimesAdded), fetched 2026-11-13. The port keeps its logic as
written, including:

* history: action=query&prop=revisions&rvprop=ids|user|userid&rvlimit=max&rvdir=newer,
  following continuation (up to 50,000 revisions);
* the URL pattern: exact URL, scheme case-insensitive and http/https interchangeable;
* the binary sweep only when the history has >= 100 revisions: stages 2..16, one
  request per stage with every URL's needle revision (rvprop=timestamp|content|ids),
  needle moved by PHP round(range / 2^stage), stop when the *last* URL's range is
  <= 20 or its needle sits on its upper bound;
* URLs grouped by identical (lower, upper) range, one request per group
  (rvstartid/rvendid, rvprop=timestamp|content, rvlimit=max, no continuation); the
  first revision in the range whose text matches is the time added.

Deviations, both forced by running without a bot account: needle revids go in
batches of 50 (the anonymous limit; IABot's bot account may send 500 in one
request), and history pages come 500 at a time (bots get 5,000). The history is
cut at the dump's latest revision after it is fetched, so both methods see the
same revisions; and the page is addressed by pageid rather than title (titles may
have moved since the dump). Requests are sequential.
"""
import json
import math
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

API = "https://en.wikipedia.org/w/api.php"
UA = "WCD-benchmark/0.1 (Wikipedia Citations Database research; https://github.com/internetarchive/wiki-references-db)"
ANON_REVIDS = 50
# one bot request: up to 500 revisions with content, and MediaWiki's 8 MiB result cap
BOT_REVS = 500
MAX_RESULT = 8 * 1024 * 1024


def php_round(x: float) -> int:
    """PHP round(): half away from zero."""
    return int(math.floor(x + 0.5)) if x >= 0 else -int(math.floor(-x + 0.5))


class Client:
    def __init__(self):
        self.requests = 0
        self.bytes = 0
        self.bot_requests = 0
        self.by_phase = {}

    def get(self, params, phase):
        req = urllib.request.Request(API + "?" + urllib.parse.urlencode(params), headers={"User-Agent": UA})
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=120) as r:
                    body = r.read()
                break
            except Exception:  # noqa: BLE001 — transient network errors
                if attempt == 4:
                    raise
                time.sleep(2 ** attempt)
        self.requests += 1
        self.bytes += len(body)
        n, b = self.by_phase.get(phase, (0, 0))
        self.by_phase[phase] = (n + 1, b + len(body))
        return json.loads(body)


def url_pattern(url: str):
    m = re.match(r"^https?://", url, re.I)
    if m:
        return re.compile(r"(?i:https?)://" + re.escape(url[m.end():]))
    return re.compile(re.escape(url))


def main_text(revision):
    slots = revision.get("slots", {}).get("main", {})
    return slots.get("*")


def get_page_history(c: Client, page_id: int) -> list[dict]:
    out, resume = [], {}
    while len(out) < 50000:
        params = {"action": "query", "prop": "revisions", "format": "json", "rvdir": "newer",
                  "rvprop": "ids|user|userid", "rvlimit": "max", "pageids": page_id, **resume}
        data = c.get(params, "history")
        for page in data.get("query", {}).get("pages", {}).values():
            out += page.get("revisions", [])
        if "continue" in data:
            resume = data["continue"]
        else:
            break
    return out


def get_times_added(c: Client, page_id: int, urls: dict, history: list[dict]) -> dict:
    patterns = {tid: url_pattern(u) for tid, u in urls.items()}
    result = {tid: None for tid in urls}
    rng = len(history)
    if rng == 0:
        return result
    proc = {tid: {"upper": rng - 1, "lower": 0, "needle": php_round(rng / 2) - 1, "useQuery": -1} for tid in urls}
    if rng >= 100:
        for stage in range(2, 17):
            revs = {}
            for tid in urls:
                n = proc[tid]["needle"]
                if 0 <= n < rng:             # PHP reads null past the end; that URL is skipped this stage
                    revs[n] = history[n]["revid"]
            ids = list(revs.values())[:BOT_REVS]
            fetched = {}
            got = 0
            c.bot_requests += 1                # IABot sends these revids in one request
            for i in range(0, len(ids), ANON_REVIDS):
                if got >= MAX_RESULT:          # what one bot response could not have carried
                    break
                params = {"action": "query", "prop": "revisions", "format": "json",
                          "rvprop": "timestamp|content|ids", "rvslots": "*",
                          "revids": "|".join(map(str, ids[i:i + ANON_REVIDS]))}
                b0 = c.bytes
                data = c.get(params, "sweep")
                got += c.bytes - b0
                for page in data.get("query", {}).get("pages", {}).values():
                    for rev in page.get("revisions", []):
                        fetched[rev["revid"]] = rev
            for tid in urls:
                n = proc[tid]["needle"]
                rev = fetched.get(history[n]["revid"]) if 0 <= n < rng else None
                if rev is None:
                    continue
                text = main_text(rev)
                if text is None:
                    continue
                if patterns[tid].search(text) is None:
                    proc[tid]["lower"] = proc[tid]["needle"] + 1
                    proc[tid]["needle"] += php_round(rng / (2 ** stage))
                else:
                    proc[tid]["upper"] = proc[tid]["needle"]
                    proc[tid]["needle"] -= php_round(rng / (2 ** stage)) - 1
            last = proc[tid]                 # as written: the loop variable of the last URL
            if (last["upper"] - last["lower"] <= 20 or last["needle"] == last["upper"]
                    or last["lower"] + 1 == last["lower"]):
                break
    queries = []
    for tid, link in proc.items():
        for qi, q in enumerate(queries):
            if q["lower"] == link["lower"] and q["upper"] == link["upper"]:
                link["useQuery"] = qi
                break
        if link["useQuery"] == -1:
            queries.append({"lower": link["lower"], "upper": link["upper"]})
            link["useQuery"] = len(queries) - 1
    for qi, b in enumerate(queries):
        params = {"action": "query", "prop": "revisions", "format": "json", "rvdir": "newer",
                  "rvprop": "timestamp|content", "rvslots": "*", "rvlimit": "max",
                  "rvstartid": history[b["lower"]]["revid"], "rvendid": history[b["upper"]]["revid"],
                  "pageids": page_id}
        revisions = None
        got, cont = 0, {}
        c.bot_requests += 1                    # IABot: one request, no continuation
        while True:
            b0 = c.bytes
            data = c.get({**params, **cont}, "range")
            got += c.bytes - b0
            for page in data.get("query", {}).get("pages", {}).values():
                if page.get("revisions") is not None:
                    revisions = (revisions or []) + page["revisions"]
            # the anonymous limit is 50 revisions with content; follow continuation up to
            # what one bot response holds (500 revisions, 8 MiB)
            if "continue" not in data or len(revisions or []) >= BOT_REVS or got >= MAX_RESULT:
                break
            cont = data["continue"]
        for tid, tmp in proc.items():
            if tmp["useQuery"] != qi or revisions is None:
                continue
            t = None
            for rev in revisions:
                text = main_text(rev)
                if text is None or "timestamp" not in rev:
                    continue
                if patterns[tid].search(text):
                    t = rev["timestamp"]
                    break
            result[tid] = t
    return result


def run_article(page_id: int, latest_rev_id: int, urls: list[str]) -> dict:
    c = Client()
    t0 = time.perf_counter()
    full = get_page_history(c, page_id)
    c.bot_requests += max(1, math.ceil(len(full) / 5000))
    history = [r for r in full if r["revid"] <= latest_rev_id]
    times = get_times_added(c, page_id, dict(enumerate(urls)), history)
    dt = time.perf_counter() - t0
    return {"times": [times[i] for i in range(len(urls))], "seconds": round(dt, 2), "requests": c.requests,
            "bytes": c.bytes, "by_phase": c.by_phase, "history_len": len(history), "bot_requests": c.bot_requests,
            "bot_history_requests": max(1, math.ceil(len(history) / 5000))}


def iso_to_dt(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) if s else None
