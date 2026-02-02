from flask import Flask, request, jsonify, render_template, Response
from sqlalchemy import create_engine, func, and_, select, case
from sqlalchemy.orm import Session
from models import WebResource, Document, Citation, CitationHistory, NormalizedCitation, Revision
from dotenv import load_dotenv
import os
import datetime

app = Flask(__name__)
load_dotenv()
engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

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
        # Correlated subquery: latest revision_id for this page
        latest_page_rev_id_sq = (
            select(func.max(Revision.revision_id))
            .where(Revision.page_id == Document.numeric_page_id)
            .scalar_subquery()
        )

        if raw:
            stmt = (
                select(
                    Citation.offset_start,
                    Citation.offset_end,
                    Citation.reference_type,
                    Citation.reference_name,
                    Citation.wiki_article_id,
                    func.min(Revision.revision_timestamp).label("earliest_revision_timestamp"),
                    func.max(Revision.revision_timestamp).label("latest_revision_timestamp"),
                    func.min(Revision.revision_id).label("earliest_revision_id"),
                    func.max(Revision.revision_id).label("latest_revision_id"),
                    func.count(Revision.revision_id).label("appearance_count"),
                    # currently_visible: whether this citation appears in the latest page revision
                    (func.max(Revision.revision_id) == latest_page_rev_id_sq).label("currently_visible"),
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
                .join(Revision, CitationHistory.revision_id == Revision.revision_id)
                .where(WebResource.url == url)
                .group_by(
                    Citation.offset_start,
                    Citation.offset_end,
                    Citation.reference_type,
                    Citation.reference_name,
                    Citation.wiki_article_id,
                    Citation.reference_raw_sha1,
                    Citation.record_sha1
                )
                .order_by(func.max(Revision.revision_timestamp).desc())
            )
            rows = session.execute(stmt).all()
            headers = [
                "offset_start",
                "offset_end",
                "reference_type",
                "reference_name",
                "wiki_article_id",
                "earliest_revision_timestamp",
                "latest_revision_timestamp",
                "earliest_revision_id",
                "latest_revision_id",
                "appearance_count",
                "currently_visible",
                "reference_raw_sha1",
                "record_sha1"
            ]
        else:
            stmt = (
                select(
                    NormalizedCitation.reference_normalized,
                    Citation.reference_type,
                    func.coalesce(func.nullif(func.array_agg(func.distinct(Citation.reference_name)), '{NULL}'), '{}').label("reference_names"),
                    func.min(Revision.revision_timestamp).label("earliest_revision_timestamp"),
                    func.max(Revision.revision_timestamp).label("latest_revision_timestamp"),
                    func.min(Revision.revision_id).label("earliest_revision_id"),
                    func.max(Revision.revision_id).label("latest_revision_id"),
                    func.count(Revision.revision_id).label("appearance_count"),
                    (func.max(Revision.revision_id) == latest_page_rev_id_sq).label("currently_visible"),
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
                .join(Revision, CitationHistory.revision_id == Revision.revision_id)
                .join(NormalizedCitation, and_(
                    NormalizedCitation.record_sha1 == Citation.record_sha1,
                    NormalizedCitation.reference_normalized_sha1 == Citation.reference_normalized_sha1
                ))
                .where(WebResource.url == url)
                .group_by(
                    NormalizedCitation.reference_normalized,
                    Citation.reference_type,
                    # array_agg is aggregated; no group_by needed for reference_names
                    NormalizedCitation.reference_normalized_sha1,
                    NormalizedCitation.record_sha1
                )
                .order_by(func.max(Revision.revision_timestamp).desc())
            )
            rows = session.execute(stmt).all()
            headers = [
                "reference_normalized",
                "reference_type",
                "reference_names",
                "earliest_revision_timestamp",
                "latest_revision_timestamp",
                "earliest_revision_id",
                "latest_revision_id",
                "appearance_count",
                "currently_visible",
                "reference_normalized_sha1",
                "record_sha1"
            ]

    
        #rows = [row + (row[max_ts_idx] == max_timestamp,) for row in rows]

    # Map reference_type to human-readable label and adjust headers
    ref_type_idx = next((i for i, h in enumerate(headers) if h == "reference_type"), None)
    if ref_type_idx is not None:
        type_labels = {0: "other", 1: "inline", 2: "endnote"}
        rows = [
            tuple(
                (type_labels.get(cell, str(cell)) if idx == ref_type_idx else cell)
                for idx, cell in enumerate(row)
            )
            for row in rows
        ]
        headers[ref_type_idx] = "reference_type_label"

    if output == "html":
        ref_column_names = {"offset_start", "offset_end", "reference_normalized"}
        ref_column_index = next((i for i, h in enumerate(headers) if h in ref_column_names), None)

        tooltips = {
            "record_sha1": "SHA1 hash of the domain, page title, and normalized reference wikitext.",
            "reference_raw_sha1": "SHA1 hash of the reference wikitext as it appears in-article.",
            "reference_normalized_sha1": "SHA1 hash of the normalized reference wikitext.",
            "currently_visible": "True if the citation is still present in the latest revision of the article.",
            "offset_start": "Start byte/char offset of the raw reference in the wikitext (inclusive). May be None.",
            "offset_end": "End byte/char offset of the raw reference in the wikitext (exclusive). May be None.",
            "reference_type_label": "Human-readable reference type label (e.g., other, inline, endnote).",
            "reference_name": "If the citation uses a named <ref name=...>, the provided name (may be null).",
            "wiki_article_id": "Numeric page ID of the Wikipedia article where this citation appears.",
            "appearance_count": "Number of distinct revisions in which this citation appears for this page.",
            "reference_names": "All distinct <ref name=...> values observed for this normalized citation.",
            "earliest_revision_id": "Earliest revision ID containing this citation.",
            "latest_revision_id": "Latest revision ID containing this citation."
        }

        # Provide mapping from timestamp columns to their corresponding revision ID column indices
        try:
            earliest_ts_idx = headers.index("earliest_revision_timestamp")
            latest_ts_idx = headers.index("latest_revision_timestamp")
            earliest_id_idx = headers.index("earliest_revision_id")
            latest_id_idx = headers.index("latest_revision_id")
        except ValueError:
            earliest_ts_idx = latest_ts_idx = earliest_id_idx = latest_id_idx = None

        return render_template(
            "results.html",
            url=url,
            headers=headers,
            rows=rows,
            ref_column_index=ref_column_index,
            tooltips=tooltips,
            earliest_ts_idx=earliest_ts_idx,
            latest_ts_idx=latest_ts_idx,
            earliest_id_idx=earliest_id_idx,
            latest_id_idx=latest_id_idx
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
