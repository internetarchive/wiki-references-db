// The article page: look up an article, then every source it has cited (with its time on the
// page and the wikitext that cites it), and the article's citation metrics. Data:
//   /api/v2/article?url=            URL → (domain, page_id)
//   /api/v2/pages/{d}/{id}/summary   title, latest revision's composition
//   /api/v2/pages/{d}/{id}/timeline  one point per revision (edits per year, references over time)
//   /api/v2/pages/{d}/{id}/sources   sources with intervals, events and wikitext forms
// The page builds its DOM with createElement/textContent only (no HTML strings).
const API_BASE = "/api/v2";
const DEFAULT_DOMAIN = "en.wikipedia.org";
const PAGE_SIZE = { current: 10, removed: 5 };
const EARLIER_FORMS_SHOWN = 2;

const pagePath = (domain, pageId, tail) =>
  `${API_BASE}/pages/${encodeURIComponent(domain)}/${encodeURIComponent(pageId)}/${tail}`;

// the canonical identifier set, in display order (proposed_frontend.md §5)
const IDENTIFIERS = [
  { field: "extracted_doi", label: "DOI", href: (v) => `https://doi.org/${encodeURIComponent(v)}` },
  { field: "extracted_isbn", label: "ISBN", href: (v) => `https://openlibrary.org/search?isbn=${encodeURIComponent(v)}` },
  { field: "extracted_pmid", label: "PMID", href: (v) => `https://pubmed.ncbi.nlm.nih.gov/${encodeURIComponent(v)}/` },
  { field: "extracted_pmc", label: "PMC", href: (v) => `https://www.ncbi.nlm.nih.gov/pmc/articles/PMC${encodeURIComponent(v)}/` },
  { field: "extracted_arxiv", label: "arXiv", href: (v) => `https://arxiv.org/abs/${encodeURIComponent(v)}` },
  { field: "extracted_wikidata_qid", label: "Wikidata", href: (v) => `https://www.wikidata.org/wiki/${encodeURIComponent(v)}` },
  { field: "extracted_librarybase_id", label: "LibraryBase", href: null },
  { field: "extracted_url", label: "URL", href: (v) => v },
];

const CITATION_TYPE_LABELS = {
  template: "Template",
  shorthand: "Short form",
  ref_tag: "Plain text",
  bare_url: "Bare URL",
  endnote: "Endnote",
  external_link: "External link",
};

const $ = (id) => document.getElementById(id);

const state = {
  domain: null,
  pageId: null,
  status: "all",
  sort: "longest",
  span: null, // [first, latest] Date of the page's revisions: the bars' shared timeline
  lists: { current: { offset: 0, total: 0, token: 0 }, removed: { offset: 0, total: 0, token: 0 } },
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (v === null || v === undefined || v === false) continue;
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (k === "style") node.setAttribute("style", v);
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v === true ? "" : v);
  }
  for (const c of children.flat()) {
    if (c === null || c === undefined || c === false) continue;
    node.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return node;
}

function link(text, href, { external = false, cls = null, title = null } = {}) {
  return el("a", { href, class: cls, title, target: external ? "_blank" : null, rel: external ? "noreferrer" : null, text });
}

const fmtInt = (n) => new Intl.NumberFormat("en").format(Number(n || 0));
const toDate = (v) => (v ? new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(v) ? v : `${v}Z`) : null);
const fmtMonth = (v) => (v ? toDate(v).toLocaleDateString("en-GB", { month: "short", year: "numeric", timeZone: "UTC" }) : "–");
const fmtDay = (v) =>
  v ? toDate(v).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" }) : "–";

function fmtDuration(days) {
  if (days >= 365 * 1.5) return `${Math.round(days / 365.25)} years`;
  if (days >= 330) return "1 year";
  if (days >= 45) return `${Math.round(days / 30.4)} months`;
  if (days >= 1.5) return `${Math.round(days)} days`;
  return "less than a day";
}

const plural = (n, word, words = `${word}s`) => `${fmtInt(n)} ${Number(n) === 1 ? word : words}`;

function diffUrl(revisionId) {
  return `https://${state.domain}/w/index.php?diff=prev&oldid=${encodeURIComponent(revisionId)}`;
}

function setStatus(message, isError = false) {
  $("status").textContent = message || "";
  $("status").classList.toggle("is-error", Boolean(isError));
}

async function fetchJson(url) {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    let detail = `Request failed (${response.status}).`;
    try {
      const body = await response.json();
      if (typeof body.error === "string") detail = body.error;
      else if (typeof body.detail === "string") detail = body.detail;
      else if (Array.isArray(body.detail) && body.detail[0]?.msg) detail = body.detail[0].msg;
    } catch (_) {
      // keep the generic message
    }
    throw new Error(detail);
  }
  return response.json();
}

function reportUrl(params) {
  const url = new URL(window.location.href);
  url.search = new URLSearchParams(params).toString();
  url.hash = "";
  return `${url.pathname}${url.search}`;
}

// position of a timestamp on the page's timeline, in percent
function pct(value) {
  if (!state.span || !value) return 0;
  const [a, b] = state.span;
  const t = toDate(value).getTime();
  if (b <= a) return 0;
  return Math.max(0, Math.min(100, ((t - a) / (b - a)) * 100));
}

// ---------------------------------------------------------------------------
// loading an article
// ---------------------------------------------------------------------------

async function lookUpUrl(articleUrl) {
  setStatus(`Looking up ${articleUrl}…`);
  try {
    const article = await fetchJson(`${API_BASE}/article?url=${encodeURIComponent(articleUrl)}`);
    await loadArticle(article.domain, article.page_id);
  } catch (error) {
    setStatus(error.message || "That article could not be found.", true);
  }
}

async function loadRandom() {
  setStatus("Picking a random article…");
  try {
    const summary = await fetchJson(`${API_BASE}/pages/random/summary`);
    await loadArticle(summary.domain, summary.page_id);
  } catch (error) {
    setStatus(error.message || "No random article is available.", true);
  }
}

async function loadArticle(domain, pageId) {
  state.domain = domain;
  state.pageId = String(pageId);
  setStatus(`Loading page ${pageId} on ${domain}…`);
  let summary;
  let timeline;
  try {
    [summary, timeline] = await Promise.all([
      fetchJson(pagePath(domain, pageId, "summary")),
      fetchJson(pagePath(domain, pageId, "timeline")),
    ]);
  } catch (error) {
    setStatus(error.message || "Unable to load that article.", true);
    return;
  }
  const first = toDate(summary.first_revision_timestamp);
  const latest = toDate(summary.latest_revision_timestamp);
  state.span = first && latest ? [first.getTime(), latest.getTime()] : null;

  renderHeader(summary);
  renderStrip(summary, timeline);
  renderMetrics(summary, timeline);
  $("article").classList.remove("hidden");
  document.title = `${summary.page_title || `Page ${pageId}`} · Wikipedia Citations Database`;
  window.history.replaceState({}, "", reportUrl({ domain, page_id: state.pageId, ...extraParams() }));
  setStatus("");
  applyFilter();
  await Promise.all([loadSources("current", false), loadSources("removed", false)]);
}

function extraParams() {
  const out = {};
  if (state.status !== "all") out.status = state.status;
  if (state.sort !== "longest") out.sort = state.sort;
  return out;
}

// ---------------------------------------------------------------------------
// header and history strip
// ---------------------------------------------------------------------------

function renderHeader(summary) {
  $("article-title").textContent = summary.page_title ? summary.page_title.replaceAll("_", " ") : `Page ${summary.page_id}`;
  $("article-domain").textContent = summary.domain;
  $("wiki-link").href = summary.url || `https://${summary.domain}/?curid=${summary.page_id}`;
  $("count-current").textContent = "–";
  $("count-removed").textContent = "–";
}

function renderStrip(summary, timeline) {
  $("revision-count").textContent = plural(summary.revision_count, "revision");
  $("first-seen").textContent = fmtDay(summary.first_revision_timestamp);
  $("latest-seen").textContent = fmtDay(summary.latest_revision_timestamp);
  const points = timeline.points || [];
  const bars = $("year-bars");
  const axis = $("year-axis");
  bars.replaceChildren();
  axis.replaceChildren();
  if (!points.length) return;
  const years = points.map((p) => toDate(p.timestamp).getUTCFullYear());
  const y0 = Math.min(...years);
  const y1 = Math.max(...years);
  const counts = new Array(y1 - y0 + 1).fill(0);
  years.forEach((y) => (counts[y - y0] += 1));
  const peak = Math.max(...counts);
  counts.forEach((n, i) => {
    bars.append(el("span", { style: `height: ${Math.max(1, (n / peak) * 40)}px`, title: `${y0 + i}: ${plural(n, "edit")}` }));
  });
  // year labels at the bars' centres, about six of them, and "now" at the end
  const nYears = counts.length;
  const step = Math.max(1, Math.ceil(nYears / 6));
  for (let i = 0; i < nYears; i += step) {
    axis.append(el("span", { style: `left: ${((i + 0.5) / nYears) * 100}%`, text: String(y0 + i) }));
  }
  axis.append(el("span", { class: "is-end", style: "left: 100%", text: "now" }));
}

// ---------------------------------------------------------------------------
// sources
// ---------------------------------------------------------------------------

function applyFilter() {
  document.querySelectorAll(".filter").forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.status === state.status)));
  $("current-block").classList.toggle("hidden", state.status === "removed");
  $("removed-block").classList.toggle("hidden", state.status === "current");
  $("sort").value = state.sort;
}

async function loadSources(status, append) {
  const list = state.lists[status];
  const token = ++list.token;
  const container = $(`${status}-list`);
  const statusLine = $(`${status}-status`);
  const more = $(`${status}-more`);
  if (!append) {
    list.offset = 0;
    container.replaceChildren();
    statusLine.textContent = "Loading sources…";
    more.classList.add("hidden");
  }
  more.disabled = true;
  const query = new URLSearchParams({ status, sort: state.sort, limit: String(PAGE_SIZE[status]), offset: String(list.offset) });
  try {
    const result = await fetchJson(`${pagePath(state.domain, state.pageId, "sources")}?${query}`);
    if (token !== list.token) return;
    renderCounts(result.counts);
    result.sources.forEach((source) => container.append(renderSource(source)));
    list.offset += result.sources.length;
    list.total = result.total;
    statusLine.textContent = result.total
      ? ""
      : status === "current"
        ? "No sources are cited in the latest revision."
        : "No sources have been removed from this article.";
    const left = result.total - list.offset;
    more.textContent = `Show ${fmtInt(Math.min(left, PAGE_SIZE[status]))} more ${status} sources (${fmtInt(left)} left)`;
    more.classList.toggle("hidden", left <= 0);
  } catch (error) {
    if (token === list.token) statusLine.textContent = error.message || "Unable to load sources.";
  } finally {
    if (token === list.token) more.disabled = false;
  }
}

function renderCounts(counts) {
  $("count-current").textContent = fmtInt(counts.current);
  $("count-removed").textContent = fmtInt(counts.removed);
  document.querySelectorAll("[data-count]").forEach((s) => (s.textContent = fmtInt(counts[s.dataset.count])));
  $("removed-summary").textContent = `${plural(counts.removed, "source")} that ${counts.removed === 1 ? "was" : "were"} cited at some point and ${counts.removed === 1 ? "is" : "are"} not in the latest revision.`;
}

function sourceHref(source) {
  if (source.extracted_url) return source.extracted_url;
  for (const { field, href } of IDENTIFIERS) {
    if (source[field] && href) return href(source[field]);
  }
  return null;
}

function displayKey(key) {
  if (!key) return "";
  // part:<sha1> is a content hash (the fallback key): shorten it for display
  return key.startsWith("part:") ? `${key.slice(0, 21)}…` : key;
}

function renderSource(source) {
  const removed = !source.is_current;
  const card = el("article", { class: `card${removed ? " is-removed" : ""}` });
  const forms = source.forms || [];
  const leadForm = forms.find((f) => f.current) || [...forms].sort((a, b) => (b.last_seen || "").localeCompare(a.last_seen || ""))[0];
  const identified = Boolean(source.title);

  // head: title (or the wikitext, for a source with no title), key, byline; badges
  const ident = el("div", { class: "card-ident" });
  if (identified) {
    const href = sourceHref(source);
    ident.append(href ? link(source.title, href, { external: true, cls: "card-title" }) : el("span", { class: "card-title", text: source.title }));
  } else if (leadForm) {
    ident.append(el("pre", { class: "wt wt-lead", text: leadForm.wikitext }));
  }
  if (source.source_key) ident.append(el("span", { class: "card-key", text: displayKey(source.source_key), title: source.source_key }));
  const by = [source.author && `${source.author}${source.year ? ` (${source.year})` : ""}`, source.publication].filter(Boolean);
  if (by.length) ident.append(el("span", { class: "card-byline", text: by.join(" · ") }));

  const badges = el("div", { class: "badges" });
  if (removed && source.last_seen) {
    const removal = (source.intervals || []).at(-1);
    badges.append(el("span", { class: "badge is-removed", text: `Removed ${fmtMonth(removal?.removed_at || source.last_seen)}` }));
  }
  if (!identified && (source.source_key || "").startsWith("part:")) {
    badges.append(el("span", { class: "badge is-warn", text: "Not yet identified" }));
  }
  if (source.kind) badges.append(el("span", { class: "badge", text: source.kind }));
  if (forms.some((f) => f.label === "Short form")) badges.append(el("span", { class: "badge", text: "Short-form citations" }));
  IDENTIFIERS.forEach(({ field, label }) => {
    if (source[field] && field !== "extracted_url") badges.append(el("span", { class: "badge", text: label }));
  });
  card.append(el("div", { class: "card-head" }, ident, badges));

  card.append(renderBar(source));
  card.append(renderForms(source, forms, leadForm, identified));

  const foot = el("div", { class: "card-foot" });
  if (source.other_pages > 0) {
    foot.append(link(`Other articles citing this source (${fmtInt(source.other_pages)})`,
      `/sources/${encodeURIComponent(source.global_source_id)}/pages?${new URLSearchParams({ domain: state.domain, page_id: state.pageId })}`));
  } else {
    foot.append(el("span", { class: "muted", text: "No other articles cite it." }));
  }
  card.append(foot);
  return card;
}

function renderBar(source) {
  const removed = !source.is_current;
  const bar = el("div", { class: "bar" }, el("div", { class: "bar-track" }));
  for (const iv of source.intervals || []) {
    const left = pct(iv.start);
    const right = pct(iv.removed_at || iv.end);
    const label = `On the page ${fmtDay(iv.start)} – ${iv.removed_at ? fmtDay(iv.removed_at) : "now"}; open the diff that added it`;
    bar.append(el("a", {
      class: `bar-span${removed ? " is-removed" : ""}`, href: diffUrl(iv.start_revision_id), target: "_blank", rel: "noreferrer",
      style: `left: ${left}%; width: ${Math.max(0.4, right - left)}%`, title: label, "aria-label": label,
    }));
  }
  for (const e of source.events || []) {
    const finalRemoval = removed && e.kind === "removed" && e === (source.events || []).filter((x) => x.kind === "removed").at(-1);
    if (e.kind !== "form_changed" && !finalRemoval) continue;
    const label = `${e.kind === "removed" ? "Removed" : e.label || "Wikitext changed"}, ${fmtDay(e.timestamp)}; open that diff`;
    bar.append(el("a", {
      class: `bar-mark${e.kind === "removed" ? " is-removed" : ""}`, href: diffUrl(e.revision_id), target: "_blank", rel: "noreferrer",
      style: `left: calc(${pct(e.timestamp)}% - 1px)`, title: label, "aria-label": label,
    }));
  }
  return el("div", { class: "bar-block" }, bar, el("p", { class: "card-when" }, ...whenLine(source)));
}

// "Added Mar 2004 · removed Mar 2014 · restored May 2016 · still cited · 22 years on the page · 5 uses …"
function whenLine(source) {
  const parts = [];
  const events = source.events || [];
  const add = (node) => {
    if (parts.length) parts.push(" · ");
    parts.push(node);
  };
  const eventLink = (word, e) => {
    const span = el("span");
    span.append(`${word} `, link(fmtMonth(e.timestamp), diffUrl(e.revision_id), { external: true }));
    return span;
  };
  const added = events.find((e) => e.kind === "added");
  if (added) add(eventLink("Added", added));
  const later = events.filter((e) => e !== added);
  const churn = later.filter((e) => e.kind === "removed" || e.kind === "restored");
  if (churn.length > 4) {
    const restored = churn.filter((e) => e.kind === "restored").length;
    add(`removed and restored ${plural(restored, "time")}`);
    const conversions = later.filter((e) => e.kind === "form_changed");
    conversions.forEach((e) => add(eventLink(e.label || "changed", e)));
    if (!source.is_current) {
      const last = churn.filter((e) => e.kind === "removed").at(-1);
      if (last) add(eventLink("last removed", last));
    }
  } else {
    later.forEach((e) => add(eventLink(e.kind === "form_changed" ? e.label || "changed" : e.kind, e)));
  }
  if (source.is_current) {
    add("still cited");
    add(`${fmtDuration(source.days_on_page)} on the page`);
    add(plural(source.uses_latest, "use") + " in the latest revision");
  } else {
    add(`on the page for ${fmtDuration(source.days_on_page)}`);
  }
  return parts;
}

function templateReportUrl(name, key, value) {
  return `/templates/report?${new URLSearchParams({
    domain: state.domain, template: name, parameter_key: String(key), parameter_value: String(value ?? "").trim(),
  })}`;
}

function renderTemplates(templates) {
  const box = el("details", { class: "tpl" }, el("summary", { text: `Template parameters (${templates.length})` }));
  templates.forEach((t) => {
    const body = el("tbody");
    Object.entries(t.parameters || {}).forEach(([k, v]) => {
      const value = String(v ?? "");
      body.append(el("tr", {},
        el("td", {}, el("code", { text: k })),
        el("td", { text: value }),
        el("td", {}, value.trim() ? link("Find matches", templateReportUrl(t.template_name, k, value), {
          title: `Other citations whose ${t.template_name} has ${k} = this value`,
        }) : "")));
    });
    box.append(el("table", {},
      el("caption", { style: "text-align: left; font-weight: 600; padding: 4px 0", text: t.template_name }),
      el("thead", {}, el("tr", {}, el("th", { text: "Parameter" }), el("th", { text: "Value" }), el("th", { text: "" }))),
      body));
  });
  return box;
}

function renderFormRow(form, { removedCard = false } = {}) {
  const earlier = !form.current;
  const label = removedCard && earlier ? "Last wikitext" : form.label;
  const body = el("div", { class: "form-body" }, el("pre", { class: "wt", text: form.wikitext || "(text unavailable)" }));
  const links = el("div", { class: "form-links" });
  if (form.normalized_sha1) {
    links.append(link("Revisions with this wikitext",
      `/citations/${form.normalized_sha1}?${new URLSearchParams({ domain: state.domain, page_id: state.pageId })}`));
    links.append(link("Pages with this wikitext", `/citations/${form.normalized_sha1}/pages?${new URLSearchParams({ domain: state.domain, page_id: state.pageId })}`));
  }
  if (links.childNodes.length) body.append(links);
  if (form.templates?.length) body.append(renderTemplates(form.templates));
  const side = earlier
    ? `${fmtMonth(form.first_seen)} – ${fmtMonth(form.last_seen)}`
    : `since ${fmtMonth(form.first_seen)} · ${plural(form.uses_latest, "use")}`;
  return el("div", { class: `form-row${earlier && !removedCard ? " is-earlier" : ""}` },
    el("span", { class: "form-label", text: label }), body, el("span", { class: "form-side", text: side }));
}

function renderForms(source, forms, leadForm, identified) {
  const box = el("div", { class: "forms" });
  const removed = !source.is_current;
  if (removed) {
    // a removed source: its last wikitext (unless it already leads the card), the rest folded
    const byLast = [...forms].sort((a, b) => (b.last_seen || "").localeCompare(a.last_seen || ""));
    const shown = identified ? byLast.slice(0, 1) : [];
    shown.forEach((f) => box.append(renderFormRow(f, { removedCard: true })));
    const rest = byLast.filter((f) => !shown.includes(f) && f !== (identified ? null : leadForm));
    if (rest.length) {
      const more = el("details", { class: "form-more" }, el("summary", { text: `${plural(rest.length, "earlier form")}` }));
      rest.forEach((f) => more.append(renderFormRow(f)));
      box.append(more);
    }
    return box;
  }
  const current = forms.filter((f) => f.current);
  const earlier = forms.filter((f) => !f.current);
  current.forEach((f) => box.append(renderFormRow(f)));
  earlier.slice(0, EARLIER_FORMS_SHOWN).forEach((f) => box.append(renderFormRow(f)));
  if (earlier.length > EARLIER_FORMS_SHOWN) {
    const more = el("details", { class: "form-more" },
      el("summary", { text: `${plural(earlier.length - EARLIER_FORMS_SHOWN, "more earlier form")}` }));
    earlier.slice(EARLIER_FORMS_SHOWN).forEach((f) => more.append(renderFormRow(f)));
    box.append(more);
  }
  return box;
}

// ---------------------------------------------------------------------------
// metrics
// ---------------------------------------------------------------------------

function renderMetrics(summary, timeline) {
  const points = timeline.points || [];
  renderRefsChart(points);
  renderChanges(points);

  const types = (summary.latest_revision?.citation_type_breakdown || []).filter((t) => t.count > 0);
  renderHBars($("types"), types.map((t) => [CITATION_TYPE_LABELS[t.citation_type] || t.citation_type, t.count]));
  const kinds = (summary.source_profile?.source_kind_breakdown || []).filter((k) => k.count > 0);
  renderHBars($("kinds"), kinds.map((k) => [k.label, k.count]));

  const list = $("templates");
  list.replaceChildren();
  const templates = summary.latest_revision?.top_templates || [];
  if (!templates.length) list.append(el("li", { class: "muted", text: "No templates in the latest revision." }));
  templates.forEach((t) => list.append(el("li", {}, el("span", { class: "mono", text: t.template_name }), el("span", { text: fmtInt(t.count) }))));
}

function renderHBars(container, rows) {
  container.replaceChildren();
  if (!rows.length) {
    container.append(el("p", { class: "muted", text: "Nothing to show." }));
    return;
  }
  const max = Math.max(...rows.map((r) => r[1]));
  rows.sort((a, b) => b[1] - a[1]).forEach(([label, n]) => {
    container.append(el("div", { class: "hbar" },
      el("span", { text: label }),
      el("div", { class: "hbar-fill", style: `width: ${(n / max) * 100}%`, role: "img", "aria-label": `${label}: ${fmtInt(n)}` }),
      el("span", { text: fmtInt(n) })));
  });
}

function renderRefsChart(points) {
  const svg = $("refs-chart");
  const ns = "http://www.w3.org/2000/svg";
  svg.replaceChildren();
  $("refs-axis").replaceChildren();
  if (!points.length) {
    $("refs-summary").textContent = "No revisions are tracked for this article.";
    return;
  }
  const W = 600;
  const H = 160;
  const top = 8;
  const bottom = 150;
  const t0 = toDate(points[0].timestamp).getTime();
  const t1 = toDate(points.at(-1).timestamp).getTime();
  const peak = Math.max(1, ...points.map((p) => p.reference_count));
  const x = (t) => (t1 > t0 ? ((t - t0) / (t1 - t0)) * (W - 12) + 6 : W / 2);
  const y = (v) => bottom - (v / peak) * (bottom - top);
  // one vertex per horizontal pixel column: last value in it (the chart reads as a step series)
  const cols = new Map();
  points.forEach((p) => cols.set(Math.round(x(toDate(p.timestamp).getTime())), p.reference_count));
  const pts = [...cols.entries()].sort((a, b) => a[0] - b[0]).map(([cx, v]) => `${cx},${y(v).toFixed(1)}`).join(" ");
  const grid = (value) => {
    const g = document.createElementNS(ns, "line");
    g.setAttribute("x1", "0");
    g.setAttribute("x2", String(W));
    g.setAttribute("y1", String(y(value)));
    g.setAttribute("y2", String(y(value)));
    g.setAttribute("stroke", value === 0 ? "#c8ccd1" : "#eaecf0");
    g.setAttribute("vector-effect", "non-scaling-stroke");
    svg.append(g);
  };
  [0, peak / 2, peak].forEach(grid);
  const line = document.createElementNS(ns, "polyline");
  line.setAttribute("fill", "none");
  line.setAttribute("stroke", "#3366cc");
  line.setAttribute("stroke-width", "2");
  line.setAttribute("vector-effect", "non-scaling-stroke");
  line.setAttribute("points", pts);
  svg.append(line);

  const latest = points.at(-1).reference_count;
  const changes = deltas(points);
  const drop = changes.filter((c) => c.delta < 0).sort((a, b) => a.delta - b.delta)[0];
  svg.setAttribute("aria-label", `References over time: ${fmtInt(points[0].reference_count)} at first, peak ${fmtInt(peak)}, ${fmtInt(latest)} in the latest revision`);
  const summaryLine = $("refs-summary");
  summaryLine.replaceChildren(`${plural(latest, "reference")} in the latest revision · peak ${fmtInt(peak)}`);
  if (drop) summaryLine.append(` · largest drop ${fmtInt(drop.delta)} in `, link(fmtMonth(drop.timestamp), diffUrl(drop.rev_id), { external: true }));

  const y0 = new Date(t0).getUTCFullYear();
  const y1 = new Date(t1).getUTCFullYear();
  const ticks = [...new Set([y0, Math.round(y0 + (y1 - y0) / 3), Math.round(y0 + (2 * (y1 - y0)) / 3), y1])];
  ticks.forEach((yr) => $("refs-axis").append(el("span", { text: String(yr) })));
}

function deltas(points) {
  const out = [];
  for (let i = 1; i < points.length; i += 1) {
    const d = points[i].reference_count - points[i - 1].reference_count;
    if (d) out.push({ delta: d, rev_id: points[i].rev_id, timestamp: points[i].timestamp });
  }
  return out;
}

function renderChanges(points) {
  const list = $("changes");
  list.replaceChildren();
  const top = deltas(points).sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta)).slice(0, 6);
  if (!top.length) {
    list.append(el("li", { class: "muted", text: "No changes in the reference count." }));
    return;
  }
  top.forEach((c) => {
    list.append(el("li", {},
      link(fmtDay(c.timestamp), diffUrl(c.rev_id), { external: true, title: "Open this revision's diff" }),
      el("span", { class: `delta ${c.delta > 0 ? "is-up" : "is-down"}`, text: c.delta > 0 ? `+${fmtInt(c.delta)}` : `−${fmtInt(-c.delta)}` })));
  });
}

// ---------------------------------------------------------------------------
// wiring
// ---------------------------------------------------------------------------

$("url-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const value = $("article-url").value.trim();
  if (value) lookUpUrl(value);
});

$("random-button").addEventListener("click", loadRandom);

$("pageid-toggle").addEventListener("click", () => {
  const form = $("pageid-form");
  const open = form.classList.toggle("hidden") === false;
  $("pageid-toggle").setAttribute("aria-expanded", String(open));
  if (open) $("page-id-input").focus();
});

$("pageid-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const domain = $("domain-input").value.trim().toLowerCase() || DEFAULT_DOMAIN;
  const pageId = $("page-id-input").value.trim();
  if (/^\d+$/.test(pageId)) loadArticle(domain, pageId);
  else setStatus("A page ID is a number.", true);
});

$("copy-link").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(window.location.href);
    $("copy-link").textContent = "Link copied";
  } catch (_) {
    $("copy-link").textContent = "Copy the address bar instead";
  }
  setTimeout(() => ($("copy-link").textContent = "Copy link to this report"), 2500);
});

document.querySelectorAll(".filter").forEach((button) => {
  button.addEventListener("click", () => {
    state.status = button.dataset.status;
    applyFilter();
    window.history.replaceState({}, "", reportUrl({ domain: state.domain, page_id: state.pageId, ...extraParams() }));
  });
});

$("sort").addEventListener("change", () => {
  state.sort = $("sort").value;
  window.history.replaceState({}, "", reportUrl({ domain: state.domain, page_id: state.pageId, ...extraParams() }));
  loadSources("current", false);
  loadSources("removed", false);
});

$("current-more").addEventListener("click", () => loadSources("current", true));
$("removed-more").addEventListener("click", () => loadSources("removed", true));

window.addEventListener("DOMContentLoaded", () => {
  const params = new URLSearchParams(window.location.search);
  if (["all", "current", "removed"].includes(params.get("status"))) state.status = params.get("status");
  if (["longest", "uses", "recent"].includes(params.get("sort"))) state.sort = params.get("sort");
  applyFilter();
  let domain = params.get("domain");
  let pageId = params.get("page_id");
  const hash = window.location.hash.match(/^#\/?([^/]+)\/(\d+)$/);   // #domain/page_id links
  if (!pageId && hash) [, domain, pageId] = hash;
  const articleUrl = params.get("url");                                // /explorer/article?url= redirects here
  if (pageId) {
    loadArticle(domain || DEFAULT_DOMAIN, pageId);
  } else if (articleUrl) {
    $("article-url").value = articleUrl;
    lookUpUrl(articleUrl);
  }
});
