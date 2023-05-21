import requests
import psycopg2
import json
from typing import List
from urllib.parse import urlparse
from syntax import normalize_wikitext, get_md5
from wikis import get_family
from credentials import *

def wikipedia_articles():
    S = requests.Session()

    URL = "https://en.wikipedia.org/w/api.php"

    SEARCHPAGE = ""

    while True:
        PARAMS = {
            "action": "query",
            "format": "json",
            "list": "allpages",
            "aplimit": "max",
            "apfilterredir": "nonredirects",
            "apcontinue": SEARCHPAGE
        }

        R = S.get(url=URL, params=PARAMS)
        DATA = R.json()

        if 'query' in DATA:
            for page in DATA['query']['allpages']:
                yield page['title']

        if 'continue' in DATA:
            SEARCHPAGE = DATA['continue']['apcontinue']
        else:
            break

def get_latest_revision(domain, page_id):
    url = f"https://{domain}/w/api.php"

    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "pageids": page_id,
        "rvprop": "ids|timestamp",
        "rvlimit": 1
    }

    response = requests.get(url, params=params)

    data = response.json()

    # handle errors and empty responses
    if 'error' in data:
        raise Exception(data['error'])
    if 'warnings' in data:
        print(data['warnings'])
    if 'query' in data:
        pages = data['query']['pages']
        for page in pages.values():
            if 'revisions' in page:
                rev = page['revisions'][0]  # only latest revision requested
                return rev['revid'], rev['timestamp']
    return None, None

def process_urls(urls: List[str]):
    conn = psycopg2.connect(host="192.168.7.36", database=DB, user=DBUSER,
                            password=DBPASS)
    cur = conn.cursor()

    for url in urls:
        print(url)
        domain = urlparse(url).netloc
        cur.execute("SELECT id FROM wikis WHERE domain = %s", (domain,))
        wiki_id = cur.fetchone()
        if not wiki_id:
            family = get_family(domain)
            cur.execute("INSERT INTO wikis (domain, family) VALUES (%s, %s) "
                        "RETURNING id", (domain, family))
            wiki_id = cur.fetchone()[0]
        else:
            wiki_id = wiki_id[0]

        params = {
            "url": url,
            "regex": "test",
            "refresh": "true"
        }

        request_url = "https://archive.org/services/context/iari/v2/statistics/all"
        request = requests.get(request_url, params=params)
        try:
            iari_resp = request.json()
            page_id = iari_resp["page_id"]
            reference_details = iari_resp.get("reference_details", [])
        except:
            continue

        for detail in reference_details:
            wikitext = detail.get("wikitext", "").replace("\\\"", "\"")
            type_ = detail.get("type")
            reference_role = {
                "general": 0,
                "named": 1,
                "footnote": 2,
                "content": 3
            }.get(type_, 0)

            reference_normalized = normalize_wikitext(wikitext)
            record_md5 = get_md5(domain, page_id, reference_normalized)
            reference_md5 = get_md5(reference_normalized)


            latest_revision, revision_timestamp = get_latest_revision(domain, page_id)
            revision_timestamp = revision_timestamp.\
                                     replace("T", " ").\
                                     replace("Z", "")

            cur.execute("INSERT INTO history (record_md5, revision_id, revision_timestamp) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (record_md5, latest_revision, revision_timestamp))

            cur.execute("INSERT INTO wikireferences (wiki, page_id, reference_role, reference_normalized, reference_md5, record_md5, latest_revision) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (record_md5) DO UPDATE SET latest_revision = EXCLUDED.latest_revision",
                        (wiki_id, page_id, reference_role, reference_normalized, reference_md5, record_md5, latest_revision))

            print(f"{record_md5}\t{reference_normalized}\n")

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':

    for article in wikipedia_articles():
        article = f"https://en.wikipedia.org/wiki/{article.replace(' ', '_')}"
        process_urls([article])
