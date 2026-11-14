# Link-addition benchmark

When was each link in an article added? InternetArchiveBot needs this date to
pick the archive snapshot closest to when a link was cited, and finds it by
binary-searching the article's history through the Wikipedia API. These scripts
time the same lookup from a WCD build, run IABot's own algorithm on the same
links, and check WCD's answers against Wikipedia's text. The enwiki results are
in the implementation plan, §25. The lookup is the prototype for the API
endpoint in SCA-388.

All scripts need the build's environment (`DATABASE_URL`, `PARQUET_DIR`); the
ones that query Wikipedia send sequential requests with a descriptive
User-Agent.

| Script | What it does |
|---|---|
| `pick_sample.sql` | Draws the 50-page sample into `sample.csv`: ten pages per band of revision count, spread by reference count, fixed seed |
| `lookup.py` | The WCD lookup and its timings (cold, warm, whole-bucket read); writes each page's current links with first-added and current-run revisions |
| `any_scheme.py` | The same lookup with `http`/`https` treated as one link, IABot's definition |
| `iabot_get_times_added_port.py` | IABot's `APII::getTimesAdded()` ported to Python; **AGPL-3.0**, see its header |
| `compare_with_iabot.py` | Runs the port on each article's links against live Wikipedia, timing it and recording requests and bytes |
| `classify_agreement.py` | Compares IABot's dates with WCD's (exact URL and any scheme, first addition and current run) |
| `explain_disagreements.py` | For links IABot dates earlier: what its pattern matched in that revision |
| `verify_against_wikipedia.py` | Checks WCD's first-added revision against Wikipedia's text: the link should be in it and not in its parent |
| `wikitext_coverage.py` | How many of the URLs in each article's wikitext WCD returns; `--visible` ignores HTML comments |

## Running it

```
psql "$DATABASE_URL" -f pick_sample.sql                                  # sample.csv
python lookup.py sample.csv lookup.json --drop-cache-cmd ./drop-cache.sh  # WCD timings
python any_scheme.py lookup.json any.json
python compare_with_iabot.py lookup.json iabot.json 150                  # live Wikipedia; articles with <= 150 links
python classify_agreement.py iabot.json any.json classified.json
python explain_disagreements.py classified.json explained.json
python verify_against_wikipedia.py lookup.json classified.json verified.json 40
python wikitext_coverage.py lookup.json coverage.json --visible
```

`--drop-cache-cmd` is any command that empties the OS page cache before a
page's cold lookup (for example `sync; echo 3 | sudo tee /proc/sys/vm/drop_caches`).
Postgres keeps its own buffer cache, as a running server would.

## Notes

* The lookup reads only the `citation_history` file whose dump page range
  holds the page; the file names carry the range. Reading the whole bucket (the
  API's current path) gave the same links on all 50 enwiki pages.
* The IABot port runs without a bot account, so it fetches revisions 50 at a
  time and follows continuation up to what one bot response holds (500
  revisions or 8 MiB). It reports both its actual requests and the requests the
  bot would make. Its timings include all network traffic; WCD's are the local
  lookup with no network.
* Large articles make IABot's search download hundreds of megabytes; the
  `MAX_LINKS` argument of `compare_with_iabot.py` keeps the load on Wikipedia
  reasonable.
