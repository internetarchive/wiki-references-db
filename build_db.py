import requests
import psycopg2
import json
import sys
from typing import List
from urllib.parse import urlparse
from syntax import normalize_wikitext, get_md5
from wikis import get_family
from refs_extractor.article import extract_references_from_page, get_current_timestamp
from credentials import *

def fetch_pages_from_db():
    conn = psycopg2.connect(
        dbname="pageset",
        user=DBUSER,
        password=DBPASS,
        host=HOST,
        port=PORT
    )
    cur = conn.cursor()
    cur.execute("SELECT title FROM articles")
    articles = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return articles

def process_pages(titles: List[str], domain="en.wikipedia.org", as_of=None):
    conn = psycopg2.connect(host=HOST, port=PORT, database=DB, user=DBUSER,
                            password=DBPASS)
    cur = conn.cursor()

    if as_of is None:
        as_of = get_current_timestamp()

    for title in titles:
        print("="*40)
        print(title)
        print("="*40)
        cur.execute("SELECT id FROM wikis WHERE domain = %s", (domain,))
        wiki_id = cur.fetchone()
        if not wiki_id:
            family = get_family(domain)
            cur.execute("INSERT INTO wikis (domain, family) VALUES (%s, %s) "
                        "RETURNING id", (domain, family))
            wiki_id = cur.fetchone()[0]
        else:
            wiki_id = wiki_id[0]

        page_id, revision_id, revision_timestamp, reference_details = extract_references_from_page(title, as_of=as_of)

        if page_id is None or revision_id is None or revision_timestamp is None:
            continue

        revision_timestamp = revision_timestamp.\
                                 replace("T", " ").\
                                 replace("Z", "")
        for wikitext in reference_details:
            reference_normalized = normalize_wikitext(wikitext)
            record_md5 = get_md5(domain, page_id, reference_normalized)
            reference_md5 = get_md5(reference_normalized)
            raw_md5 = get_md5(wikitext)

            cur.execute("INSERT INTO history ("
                            "record_md5, "
                            "revision_id, "
                            "revision_timestamp) "
                        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (record_md5, revision_id, revision_timestamp))

            cur.execute("INSERT INTO wikireferences ("
                            "wiki, "
                            "page_id, "
                            "reference_raw, "
                            "reference_normalized, "
                            "reference_md5, "
                            "reference_raw_md5, "
                            "record_md5, "
                            "latest_revision) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (record_md5) "
                        "DO UPDATE SET latest_revision = EXCLUDED.latest_revision",
                        (wiki_id, page_id, wikitext, reference_normalized, reference_md5, raw_md5, record_md5, revision_id))

            if reference_normalized == "<ref />":
                print(wikitext, end="\n\n")
            else:
                print(reference_normalized, end="\n\n")

        conn.commit()

    cur.close()
    conn.close()


if __name__ == '__main__':
        as_of = None
        if len(sys.argv) > 1:
            as_of = sys.argv[1]
        process_pages(fetch_pages_from_db(), as_of=as_of)
