// Report pages ported from the v1 Citation Explorer:
//   /citations/{sha1}[?domain=&page_id=]        revisions that contain a citation (on one page, or every page)
//   /citations/{sha1}/pages[?domain=&page_id=]  pages whose citations normalize to the same text
//   /templates/{id}/report?parameter_key=&parameter_value=   citations with that template parameter value
//   /templates/report?legacy_id=…               an old explorer template link whose id could not be mapped
const API_BASE = "/api/v2";
const HISTORY_PAGE_SIZE = 500;
const LIST_PAGE_SIZE = 100;
// the API serves a citation's history across all pages only up to this many pages (HISTORY_MAX_PAGES)
const HISTORY_MAX_PAGES = 25;

const reportRoot = document.getElementById("report");
const reportTitle = document.getElementById("report-title");
const reportSubtitle = document.getElementById("report-subtitle");
const params = new URLSearchParams(window.location.search);

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function el(tag, { className = "", text = null, href = null, external = false, title = null } = {}, children = []) {
  const node = document.createElement(tag);
  if (className) {
    node.className = className;
  }
  if (text !== null && text !== undefined) {
    node.textContent = text;
  }
  if (href) {
    node.href = href;
    if (external) {
      node.target = "_blank";
      node.rel = "noreferrer";
    }
  }
  if (title) {
    node.title = title;
  }
  children.filter(Boolean).forEach((child) => node.append(child));
  return node;
}

function formatInteger(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function formatTimestamp(value) {
  if (!value) {
    return "Unknown";
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
}

function plural(count, word) {
  return `${formatInteger(count)} ${word}${Number(count) === 1 ? "" : "s"}`;
}

async function fetchJson(endpoint) {
  const response = await fetch(endpoint, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    let detail = `Request failed with status ${response.status}.`;
    try {
      const payload = await response.json();
      if (typeof payload.error === "string") {
        detail = payload.error;
      } else if (typeof payload.detail === "string") {
        detail = payload.detail;
      } else if (Array.isArray(payload.detail) && payload.detail[0]?.msg) {
        detail = payload.detail[0].msg;
      }
    } catch (_) {
      // keep the generic message
    }
    throw new Error(detail);
  }
  return response.json();
}

function dashboardUrl(domain, pageId) {
  return `/?${new URLSearchParams({ domain, page_id: String(pageId) })}`;
}

function reportQuery(domain, pageId) {
  return domain && pageId ? `?${new URLSearchParams({ domain, page_id: String(pageId) })}` : "";
}

function citationReportUrl(sha1, domain, pageId) {
  return `/citations/${encodeURIComponent(sha1)}${reportQuery(domain, pageId)}`;
}

function citationPagesUrl(sha1, domain, pageId) {
  return `/citations/${encodeURIComponent(sha1)}/pages${reportQuery(domain, pageId)}`;
}

function revisionUrl(domain, revisionId) {
  return `https://${domain}/w/index.php?oldid=${encodeURIComponent(revisionId)}`;
}

function pageLabel(domain, pageId, title) {
  return title ? title.replaceAll("_", " ") : `${domain} page ${pageId}`;
}

function pageLink(domain, pageId, title) {
  return el("a", { href: dashboardUrl(domain, pageId), text: pageLabel(domain, pageId, title) });
}

function setHeader(title, subtitle) {
  reportTitle.textContent = title;
  reportSubtitle.textContent = subtitle || "";
  document.title = `${title} · Wikipedia Citations Database`;
}

function card(...children) {
  return el("article", { className: "card report-card" }, children);
}

function showError(message) {
  reportRoot.replaceChildren(card(el("p", { className: "status-message is-error", text: message })));
}

function table(headings) {
  const body = el("tbody");
  const node = el("table", { className: "report-table" }, [
    el("thead", {}, [el("tr", {}, headings.map((h) => el("th", { text: h })))]),
    body,
  ]);
  return { node, body };
}

function cell(...children) {
  return el("td", {}, children.map((c) => (typeof c === "string" ? document.createTextNode(c) : c)));
}

/** A paged list: `fetchPage(offset)` returns {rows: [tr…], total, count}. */
function pagedSection({ headings, fetchPage, summarize, emptyText }) {
  const summary = el("p", { className: "muted", text: "Loading…" });
  const { node, body } = table(headings);
  const wrap = el("div", { className: "report-table-wrap" }, [node]);
  const more = el("button", { className: "secondary-button hidden", text: "Show more" });
  more.type = "button";
  let offset = 0;

  async function load() {
    more.disabled = true;
    try {
      const page = await fetchPage(offset);
      page.rows.forEach((row) => body.appendChild(row));
      offset += page.count;
      summary.textContent = page.total ? summarize(offset, page.total) : emptyText;
      wrap.classList.toggle("hidden", !page.total);
      more.classList.toggle("hidden", offset >= page.total || page.count === 0);
    } catch (error) {
      summary.textContent = error.message || "Unable to load this report.";
      summary.classList.add("is-error");
    } finally {
      more.disabled = false;
    }
  }

  more.addEventListener("click", load);
  load();
  return [summary, wrap, el("div", { className: "button-row" }, [more])];
}

function citationCard(sha1, info, links) {
  const badges = el("div", { className: "source-badges" });
  if (info?.citation_type) {
    badges.appendChild(el("span", { className: "source-badge", text: info.citation_type.replaceAll("_", " ") }));
  }
  return card(
    el("p", { className: "section-label", text: "Normalized citation" }),
    el("p", {}, [el("code", { className: "report-sha", text: sha1 })]),
    badges,
    el("pre", { className: "citation-text", text: info?.reference_normalized ?? "" }),
    links.length ? el("div", { className: "citation-actions" }, links) : null
  );
}

// ---------------------------------------------------------------------------
// /citations/{sha1}: revision history
// ---------------------------------------------------------------------------

async function historyReport(sha1) {
  const domain = params.get("domain");
  const pageId = params.get("page_id");
  const scoped = Boolean(domain && pageId);
  setHeader("Citation History", scoped ? "Every revision of one page that contains this citation." :
    "Every revision, on every page, that contains this citation.");

  const info = await fetchJson(`${API_BASE}/citations/${sha1}/pages?limit=0`);
  const links = [
    el("a", { className: "inline-action", href: citationPagesUrl(sha1, domain, pageId),
              text: `Cited on ${plural(info.total, "page")}` }),
  ];
  const allPagesAllowed = info.total <= HISTORY_MAX_PAGES;
  if (scoped && allPagesAllowed) {
    links.push(el("a", { className: "inline-action", href: citationReportUrl(sha1), text: "History on all pages" }));
  }

  if (!scoped && !allPagesAllowed) {
    // the API refuses this (one revision-history bucket per page); send people to one page's history
    reportRoot.replaceChildren(
      citationCard(sha1, info, links),
      card(
        el("p", { text: `This citation is on ${plural(info.total, "page")}, too many to list every revision at once.` }),
        el("p", { className: "muted", text: "Pick a page from the list to see that page's revisions." }),
        el("div", { className: "button-row" }, [
          el("a", { className: "timeline-action-link", href: citationPagesUrl(sha1), text: "Show the pages" }),
        ])
      )
    );
    return;
  }

  let scopeLabel = "across all pages";
  const sections = pagedSection({
    headings: ["Revision", "Timestamp", "Page"],
    emptyText: "No revision history was found for this citation.",
    summarize: (shown, total) => `Showing ${formatInteger(shown)} of ${plural(total, "revision")} ${scopeLabel}, oldest first.`,
    fetchPage: async (offset) => {
      const query = new URLSearchParams({ limit: String(HISTORY_PAGE_SIZE), offset: String(offset) });
      if (scoped) {
        query.set("domain", domain);
        query.set("page_id", pageId);
      }
      const result = await fetchJson(`${API_BASE}/citations/${sha1}/history?${query}`);
      if (scoped && result.revisions[0]) {
        const first = result.revisions[0];
        scopeLabel = `on ${pageLabel(first.domain, first.page_id, first.page_title)}`;
      }
      return {
        total: result.total,
        count: result.revisions.length,
        rows: result.revisions.map((r) => el("tr", {}, [
          cell(el("a", { href: revisionUrl(r.domain, r.revision_id), external: true, text: String(r.revision_id),
                         title: "Open this revision on the wiki" })),
          cell(formatTimestamp(r.revision_timestamp)),
          cell(pageLink(r.domain, r.page_id, r.page_title)),
        ])),
      };
    },
  });

  reportRoot.replaceChildren(citationCard(sha1, info, links), card(...sections));
}

// ---------------------------------------------------------------------------
// /citations/{sha1}/pages: pages that cite it
// ---------------------------------------------------------------------------

async function pagesReport(sha1) {
  const currentDomain = params.get("domain");
  const currentPageId = params.get("page_id");
  setHeader("Pages Citing This Citation", "Every page with a citation that normalizes to the same text, in any revision.");

  const info = await fetchJson(`${API_BASE}/citations/${sha1}/pages?limit=0`);
  const links = [el("a", { className: "inline-action", href: citationReportUrl(sha1, currentDomain, currentPageId),
                           text: currentPageId ? "Revision history on this page" : "Revision history" })];

  const sections = pagedSection({
    headings: ["Page", "Page ID", "Instances", "Ref names", ""],
    emptyText: "No pages were found for this citation.",
    summarize: (shown, total) => `Showing ${formatInteger(shown)} of ${plural(total, "page")}, by title.`,
    fetchPage: async (offset) => {
      const result = await fetchJson(`${API_BASE}/citations/${sha1}/pages?limit=${LIST_PAGE_SIZE}&offset=${offset}`);
      return {
        total: result.total,
        count: result.pages.length,
        rows: result.pages.map((p) => {
          const isCurrent = p.domain === currentDomain && String(p.page_id) === String(currentPageId);
          const name = el("span", {}, [pageLink(p.domain, p.page_id, p.page_title)]);
          if (isCurrent) {
            name.append(el("span", { className: "muted", text: " (this page)" }));
          }
          const row = el("tr", { className: isCurrent ? "is-current" : "" }, [
            cell(name),
            cell(el("code", { text: String(p.page_id) })),
            cell(formatInteger(p.n_instances)),
            cell(p.reference_names.length ? p.reference_names.join(", ") : "—"),
            cell(
              el("a", { className: "inline-action", href: citationReportUrl(sha1, p.domain, p.page_id), text: "Revisions" }),
              " ",
              el("a", { className: "inline-action", href: p.url, external: true, text: "Wiki" })
            ),
          ]);
          return row;
        }),
      };
    },
  });

  reportRoot.replaceChildren(citationCard(sha1, info, links), card(...sections));
}

// ---------------------------------------------------------------------------
// /templates/{id}/report: citations with a template parameter value
// ---------------------------------------------------------------------------

async function templateReport(templateId) {
  const key = (params.get("parameter_key") || "").trim();
  const value = (params.get("parameter_value") || "").trim();
  setHeader("Template Parameter Report", "Citations whose template has this parameter value.");
  if (!key) {
    showError("This report needs a parameter_key and a parameter_value.");
    return;
  }

  let header = null;
  const sections = pagedSection({
    headings: ["Normalized citation", "First seen on", ""],
    emptyText: "No citations use this template parameter value.",
    summarize: (shown, total) => `Showing ${formatInteger(shown)} of ${plural(total, "citation")} using this value.`,
    fetchPage: async (offset) => {
      const query = new URLSearchParams({ parameter_key: key, parameter_value: value, limit: String(LIST_PAGE_SIZE),
                                          offset: String(offset) });
      const result = await fetchJson(`${API_BASE}/templates/${templateId}/report?${query}`);
      if (!header) {
        header = result;
        setHeader("Template Parameter Report", `${result.template_name}: ${key} = ${value}`);
      }
      return {
        total: result.total,
        count: result.citations.length,
        rows: result.citations.map((c) => el("tr", {}, [
          cell(el("pre", { className: "citation-text citation-text-compact", text: c.reference_normalized })),
          cell(c.appears_on_page_id
            ? pageLink(c.appears_on_domain, c.appears_on_page_id, c.appears_on_page_title)
            : "Unknown page"),
          cell(
            el("a", { className: "inline-action", text: "History",
                      href: citationReportUrl(c.normalized_sha1, c.appears_on_domain, c.appears_on_page_id) }),
            " ",
            el("a", { className: "inline-action", href: citationPagesUrl(c.normalized_sha1), text: "Pages" })
          ),
        ])),
      };
    },
  });

  const query = el("dl", { className: "report-facts" }, [
    el("dt", { text: "Template" }), el("dd", { text: `#${templateId}` }),
    el("dt", { text: "Parameter" }), el("dd", {}, [el("code", { text: key })]),
    el("dt", { text: "Value" }), el("dd", {}, [el("code", { text: value || "(empty)" })]),
  ]);
  reportRoot.replaceChildren(card(query, ...sections));
}

// ---------------------------------------------------------------------------
// /templates/report?legacy_id=…: unmapped link from the old explorer
// ---------------------------------------------------------------------------

function legacyTemplateNotice() {
  const legacyId = params.get("legacy_id");
  const name = params.get("template_name");
  const key = params.get("parameter_key") || "";
  const value = params.get("parameter_value") || "";
  setHeader("Template Parameter Report", "This link comes from the retired Citation Explorer.");
  const what = name ? `the template “${name}”` : `template #${legacyId}`;
  const reason = name
    ? `${what} is not in the current database.`
    : "template ids were renumbered in the current database, and this server has no table to translate the old id.";
  reportRoot.replaceChildren(card(
    el("p", { text: `This link asked for citations with ${key} = “${value}” in ${what} from the old explorer, but ${reason}` }),
    el("p", { className: "muted", text: "Open a page in the dashboard and use “Find matches” on a template parameter to run the same report." }),
    el("div", { className: "button-row" }, [el("a", { className: "timeline-action-link", href: "/", text: "Go to the dashboard" })])
  ));
}

// ---------------------------------------------------------------------------

async function route() {
  const path = window.location.pathname.replace(/\/+$/, "");
  let match;
  try {
    if ((match = path.match(/^\/citations\/([0-9a-f]{40})\/pages$/))) {
      await pagesReport(match[1]);
    } else if ((match = path.match(/^\/citations\/([0-9a-f]{40})$/))) {
      await historyReport(match[1]);
    } else if ((match = path.match(/^\/templates\/(\d+)\/report$/))) {
      await templateReport(match[1]);
    } else if (path === "/templates/report") {
      legacyTemplateNotice();
    } else {
      setHeader("Report Not Found");
      showError("There is no report at this address.");
    }
  } catch (error) {
    showError(error.message || "Unable to load this report.");
  }
}

route();
