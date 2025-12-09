from flask import Flask, request, jsonify, render_template, Response
from sqlalchemy import create_engine, func, and_, select
from sqlalchemy.orm import Session
from models import WebResource, Document, Citation, CitationHistory, NormalizedCitation
from credentials import *
import datetime

app = Flask(__name__)
engine = create_engine(f'postgresql://{dbuser}:{dbpass}@{dbhost}:{dbport}/{dbname}')

HTML_FORM = """
<!doctype html>
<title>Reference Lookup</title>
<h2>Wikipedia Reference Lookup</h2>
<form method="get">
  <label for="url">Wikipedia Article URL:</label><br>
  <input type="text" id="url" name="url" size="60" value="https://en.wikipedia.org/wiki/Siemens_scandal"><br><br>
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

@app.route("/", methods=["GET"])
def reference_lookup():
    url = request.args.get("url")
    raw = request.args.get("raw", "false").lower() == "true"
    output = request.args.get("output", "html")

    if not url:
        return HTML_FORM

    with Session(engine) as session:
        if raw:
            stmt = (
                select(
                    Citation.reference_raw,
                    func.min(CitationHistory.revision_timestamp).label("earliest_revision_timestamp"),
                    func.max(CitationHistory.revision_timestamp).label("latest_revision_timestamp"),
                    Citation.reference_raw_sha1,
                    Citation.record_sha1
                )
                .select_from(WebResource)
                .join(Document, WebResource.instance_of_document == Document.id)
                .join(Citation, Citation.wiki_article_id == Document.numeric_page_id)
                .join(CitationHistory, and_(
                    Citation.record_sha1 == CitationHistory.record_sha1,
                    Citation.reference_raw_sha1 == CitationHistory.reference_raw_sha1
                ))
                .where(WebResource.url == url)
                .group_by(Citation.reference_raw, Citation.reference_raw_sha1, Citation.record_sha1)
                .order_by(func.max(CitationHistory.revision_timestamp).desc())
            )
            rows = session.execute(stmt).all()
            headers = [
                "reference_raw",
                "earliest_revision_timestamp",
                "latest_revision_timestamp",
                "reference_raw_sha1",
                "record_sha1"
            ]
        else:
            stmt = (
                select(
                    NormalizedCitation.reference_normalized,
                    func.min(CitationHistory.revision_timestamp).label("earliest_revision_timestamp"),
                    func.max(CitationHistory.revision_timestamp).label("latest_revision_timestamp"),
                    NormalizedCitation.reference_normalized_sha1,
                    NormalizedCitation.record_sha1
                )
                .select_from(WebResource)
                .join(Document, WebResource.instance_of_document == Document.id)
                .join(Citation, Citation.wiki_article_id == Document.numeric_page_id)
                .join(CitationHistory, and_(
                    Citation.record_sha1 == CitationHistory.record_sha1,
                    Citation.reference_raw_sha1 == CitationHistory.reference_raw_sha1
                ))
                .join(NormalizedCitation, and_(
                    NormalizedCitation.record_sha1 == Citation.record_sha1,
                    NormalizedCitation.reference_normalized_sha1 == Citation.reference_normalized_sha1
                ))
                .where(WebResource.url == url)
                .group_by(
                    NormalizedCitation.reference_normalized,
                    NormalizedCitation.reference_normalized_sha1,
                    NormalizedCitation.record_sha1
                )
                .order_by(func.max(CitationHistory.revision_timestamp).desc())
            )
            rows = session.execute(stmt).all()
            headers = [
                "reference_normalized",
                "earliest_revision_timestamp",
                "latest_revision_timestamp",
                "reference_normalized_sha1",
                "record_sha1"
            ]

    
        #rows = [row + (row[max_ts_idx] == max_timestamp,) for row in rows]

    if output == "html":
        ref_column_names = {"reference_raw", "reference_normalized"}
        ref_column_index = next((i for i, h in enumerate(headers) if h in ref_column_names), None)

        tooltips = {
            "record_sha1": "SHA1 hash of the domain, page title, and normalized reference wikitext.",
            "reference_raw_sha1": "SHA1 hash of the reference wikitext as it appears in-article.",
            "reference_normalized_sha1": "SHA1 hash of the normalized reference wikitext.",
            "currently_visible": "True if the citation is still present in the latest revision of the article."
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
