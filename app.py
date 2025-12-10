from flask import Flask, request, jsonify, render_template, Response
from urllib.parse import urlparse, unquote
from refs_extractor.article import extract_references_from_page
from syntax import normalize_wikitext, get_sha1

app = Flask(__name__)

HTML_FORM = """
<!doctype html>
<title>Reference Lookup</title>
<h2>Wikipedia Reference Lookup</h2>
<form method="get">
  <label for="url">Wikipedia Article URL:</label><br />
  <input type="text" id="url" name="url" size="60" value="https://en.wikipedia.org/wiki/Siemens_scandal"><br><br>
  <label for="asof">As of (optional):</label><br />
  <input type="text" id="asof" name="asof" placeholder="YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ">
  <input type="checkbox" id="raw" name="raw" value="true">
  <label for="raw">Show raw references</label><br><br>
  <label for="output">Output format:</label>
  <select id="output" name="output">
    <option value="html">HTML Table</option>
    <option value="tsv">TSV (normalized only)</option>
    <option value="json">JSON</option>
  </select><br><br>
  <input type="submit" value="Submit">
</form>
"""


def _parse_url(url: str):
    parsed = urlparse(url)
    domain = parsed.netloc
    # Expect paths like /wiki/Title
    title = unquote(parsed.path.split("/wiki/")[-1]) if "/wiki/" in parsed.path else unquote(parsed.path.strip("/"))
    # Normalize spaces
    title = title.replace("_", " ")
    return domain, title


def _normalize_asof(asof: str | None) -> str | None:
    if not asof:
        return None
    asof = asof.strip()
    if not asof:
        return None
    # If only a date is provided, assume start of day UTC
    if len(asof) == 10 and asof.count("-") == 2:
        return f"{asof}T00:00:00Z"
    return asof


@app.route("/", methods=["GET"])
def reference_lookup():
    url = request.args.get("url")
    raw = request.args.get("raw", "false").lower() == "true"
    output = request.args.get("output", "html")
    asof = _normalize_asof(request.args.get("asof"))

    if not url:
        return HTML_FORM

    try:
        domain, title = _parse_url(url)
    except Exception:
        return Response("Invalid URL format", status=400)

    # Extract references from Wikipedia live API
    page_id, revision_id, revision_timestamp, references = extract_references_from_page(title, domain=domain, as_of=asof)
    if page_id is None:
        return Response("Article not found for the given time.", status=404)

    rows = []
    headers = []

    if raw:
        headers = [
            "reference_raw",
            "reference_raw_sha1",
            "record_sha1",
        ]
        seen = set()
        for reference_raw in references:
            reference_normalized = normalize_wikitext(reference_raw)
            record_sha1 = get_sha1(domain, page_id, reference_normalized)
            reference_raw_sha1 = get_sha1(reference_raw)
            key = (record_sha1, reference_raw_sha1)
            if key in seen:
                continue
            seen.add(key)
            rows.append((reference_raw, reference_raw_sha1, record_sha1))
    else:
        headers = [
            "reference_normalized",
            "reference_normalized_sha1",
            "record_sha1",
        ]
        seen = set()
        for reference_raw in references:
            reference_normalized = normalize_wikitext(reference_raw)
            record_sha1 = get_sha1(domain, page_id, reference_normalized)
            reference_normalized_sha1 = get_sha1(reference_normalized)
            key = record_sha1
            if key in seen:
                continue
            seen.add(key)
            rows.append((reference_normalized, reference_normalized_sha1, record_sha1))

    if output == "html":
        ref_column_names = {"reference_raw", "reference_normalized"}
        ref_column_index = next((i for i, h in enumerate(headers) if h in ref_column_names), None)

        tooltips = {
            "record_sha1": "SHA1 hash of the domain, page id, and normalized reference wikitext.",
            "reference_raw_sha1": "SHA1 hash of the reference wikitext as it appears in-article.",
            "reference_normalized_sha1": "SHA1 hash of the normalized reference wikitext.",
        }

        return render_template(
            "results.html",
            url=url,
            headers=headers,
            rows=rows,
            ref_column_index=ref_column_index,
            tooltips=tooltips
        )

    elif output == "tsv":
        if raw:
            return Response("TSV output not supported for raw references", status=400)
        tsv = "\t".join(headers) + "\n"
        tsv += "\n".join("\t".join(str(cell) for cell in row) for row in rows)
        return Response(tsv, mimetype="text/tab-separated-values")

    elif output == "json":
        result = [dict(zip(headers, row)) for row in rows]
        return jsonify(result)

    else:
        return Response("Unsupported output format", status=400)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=12121, debug=True)
