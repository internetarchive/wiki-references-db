const API_BASE = "/api/v2";
const DEFAULT_DOMAIN = "en.wikipedia.org";
let currentDomain = DEFAULT_DOMAIN;

const pagePath = (domain, pageId, tail) =>
  `${API_BASE}/pages/${encodeURIComponent(domain)}/${encodeURIComponent(pageId)}/${tail}`;

// Display labels for the six citation types (proposed_extractor.md)
const CITATION_TYPE_LABELS = {
  ref_tag: "Ref tags",
  template: "Templates",
  bare_url: "Bare URLs",
  endnote: "Endnotes",
  shorthand: "Shorthand",
  external_link: "External links",
};

// The canonical identifier set, in display order (proposed_frontend.md §5)
const IDENTIFIERS = [
  { field: "extracted_doi", label: "DOI", href: (v) => `https://doi.org/${encodeURIComponent(v)}` },
  { field: "extracted_isbn", label: "ISBN", href: (v) => `https://openlibrary.org/search?isbn=${encodeURIComponent(v)}` },
  { field: "extracted_pmid", label: "PMID", href: (v) => `https://pubmed.ncbi.nlm.nih.gov/${encodeURIComponent(v)}/` },
  { field: "extracted_pmc", label: "PMC", href: (v) => `https://www.ncbi.nlm.nih.gov/pmc/articles/PMC${encodeURIComponent(v)}/` },
  { field: "extracted_arxiv", label: "arXiv", href: (v) => `https://arxiv.org/abs/${encodeURIComponent(v)}` },
  { field: "extracted_wikidata_qid", label: "Wikidata", href: (v) => `https://www.wikidata.org/wiki/${encodeURIComponent(v)}` },
  { field: "extracted_librarybase_id", label: "LibraryBase", href: null },
  { field: "extracted_url", label: "Link", href: (v) => v },
];

const pageForm = document.getElementById("page-form");
const pageIdInput = document.getElementById("page-id-input");
const domainInput = document.getElementById("domain-input");
const domainOptions = document.getElementById("domain-options");
const randomButton = document.getElementById("random-button");
const statusMessage = document.getElementById("status-message");
const summaryPanel = document.getElementById("summary-panel");

const pageTitle = document.getElementById("page-title");
const pageIdValue = document.getElementById("page-id");
const pageDomainValue = document.getElementById("page-domain");
const pageNamespace = document.getElementById("page-namespace");
const wikipediaLink = document.getElementById("wikipedia-link");
const latestReferenceCount = document.getElementById("latest-reference-count");
const revisionCount = document.getElementById("revision-count");
const firstSeen = document.getElementById("first-seen");
const latestSeen = document.getElementById("latest-seen");
const latestRevisionMeta = document.getElementById("latest-revision-meta");
const citationTypeChart = document.getElementById("citation-type-chart");
const topTemplatesList = document.getElementById("top-templates-list");
const sourceKindChart = document.getElementById("source-kind-chart");
const timelineChart = document.getElementById("timeline-chart");
const timelinePeakRefs = document.getElementById("timeline-peak-refs");
const timelinePeakTemplates = document.getElementById("timeline-peak-templates");
const timelinePeakReuses = document.getElementById("timeline-peak-reuses");
const timelinePeakHighConfidence = document.getElementById("timeline-peak-high-confidence");
const timelinePointCount = document.getElementById("timeline-point-count");
const timelineBiggestIncrease = document.getElementById("timeline-biggest-increase");
const timelineBiggestDecrease = document.getElementById("timeline-biggest-decrease");
const timelineInspectorLabel = document.getElementById("timeline-inspector-label");
const timelineInspectorSummary = document.getElementById("timeline-inspector-summary");
const timelineInspectorMeta = document.getElementById("timeline-inspector-meta");
const timelineInspectorTypes = document.getElementById("timeline-inspector-types");
const timelineInspectorActions = document.getElementById("timeline-inspector-actions");
const timelineChangeList = document.getElementById("timeline-change-list");
const topSourcesSummary = document.getElementById("top-sources-summary");
const topSourcesList = document.getElementById("top-sources-list");
const sourceUsageCache = new Map();
let currentPageId = null;
let timelineChartInstance = null;
let timelineResizeListenerAttached = false;

function setBusyState(isBusy) {
  randomButton.disabled = isBusy;
  const submitButton = pageForm.querySelector('button[type="submit"]');
  submitButton.disabled = isBusy;
}

function setStatus(message, isError = false) {
  statusMessage.textContent = message;
  statusMessage.style.color = isError ? "#8a2f17" : "";
}

function formatTimestamp(value) {
  if (!value) {
    return "Unknown";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

function formatWikipediaUrlFromPageId(pageId, domain = currentDomain) {
  return `https://${domain}/?curid=${encodeURIComponent(pageId)}`;
}

function formatCitationTypeLabel(citationType) {
  return CITATION_TYPE_LABELS[citationType] || citationType;
}

function dashboardUrl(domain, pageId) {
  const url = new URL(window.location.href);
  url.search = "";
  url.hash = "";
  url.searchParams.set("domain", domain);
  url.searchParams.set("page_id", pageId);
  return `${url.pathname}${url.search}`;
}

function formatInteger(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function formatSignedInteger(value) {
  const number = Number(value || 0);
  const absoluteValue = formatInteger(Math.abs(number));
  if (number > 0) {
    return `+${absoluteValue}`;
  }
  if (number < 0) {
    return `-${absoluteValue}`;
  }
  return absoluteValue;
}

function formatCompactDate(value) {
  if (!value) {
    return "Unknown";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatRevisionUrl(revId) {
  return `https://${currentDomain}/w/index.php?oldid=${encodeURIComponent(revId)}`;
}

function formatRevisionDiffUrl(fromRevId, toRevId) {
  if (!fromRevId) {
    return formatRevisionUrl(toRevId);
  }
  return `https://${currentDomain}/w/index.php?title=Special:Diff&diff=${encodeURIComponent(toRevId)}&oldid=${encodeURIComponent(fromRevId)}`;
}

function formatCitationTypeBreakdownSummary(items) {
  if (!items.length) {
    return "No citation-type breakdown available for this revision.";
  }
  return items
    .map((item) => `${formatInteger(item.count)} ${formatCitationTypeLabel(item.citation_type).toLowerCase()}`)
    .join(" • ");
}

function formatTimelineChangeStat(change) {
  if (!change) {
    return "-";
  }
  return `${formatSignedInteger(change.delta)} refs`;
}

function createTimelineActionLink(label, href) {
  const link = document.createElement("a");
  link.className = "timeline-action-link";
  link.href = href;
  link.target = "_blank";
  link.rel = "noreferrer";
  link.textContent = label;
  return link;
}

function createTimelineActionButton(label, onClick) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "timeline-action-link";
  button.textContent = label;
  button.addEventListener("click", onClick);
  return button;
}

function renderTimelineInspector(pointDetail, options = {}) {
  const {
    isPinned = false,
    onClearPin = null,
  } = options;

  if (!pointDetail) {
    timelineInspectorLabel.textContent = "Selected revision";
    timelineInspectorSummary.textContent = "Hover the chart to inspect revisions. Click a point to pin it.";
    timelineInspectorMeta.textContent = "";
    timelineInspectorTypes.textContent = "";
    timelineInspectorActions.replaceChildren();
    return;
  }

  timelineInspectorLabel.textContent = isPinned
    ? `Pinned revision ${pointDetail.rev_id}`
    : `Revision ${pointDetail.rev_id}`;
  const deltaText = pointDetail.delta !== 0
    ? ` (${formatSignedInteger(pointDetail.delta)})`
    : "";
  timelineInspectorSummary.textContent = `${formatInteger(pointDetail.reference_count)} references${deltaText}.`;

  timelineInspectorMeta.textContent = `${formatTimestamp(pointDetail.timestamp)} • ${formatInteger(
    pointDetail.template_count
  )} templates • ${formatInteger(pointDetail.named_ref_count)} reuses • ${formatInteger(
    pointDetail.high_confidence_count
  )} high-confidence sources`;
  timelineInspectorTypes.textContent = `Citation types: ${formatCitationTypeBreakdownSummary(
    pointDetail.citation_type_breakdown
  )}`;

  const actions = [
    createTimelineActionLink(
      "View revision",
      formatRevisionUrl(pointDetail.rev_id)
    ),
  ];

  if (pointDetail.previousPoint) {
    actions.push(
      createTimelineActionLink(
        "View diff",
        formatRevisionDiffUrl(pointDetail.previousPoint.rev_id, pointDetail.rev_id)
      )
    );
  }

  if (isPinned && typeof onClearPin === "function") {
    actions.push(createTimelineActionButton("Return to latest", onClearPin));
  }

  timelineInspectorActions.replaceChildren(...actions);
}

function renderTimelineChangeList(changes) {
  timelineChangeList.replaceChildren();

  if (!changes.length) {
    const emptyItem = document.createElement("li");
    emptyItem.className = "empty-state";
    emptyItem.textContent = "No tracked revision-to-revision changes were available.";
    timelineChangeList.appendChild(emptyItem);
    return;
  }

  changes.slice(0, 5).forEach((change) => {
    const item = document.createElement("li");
    item.className = "timeline-change-item";

    const title = document.createElement("p");
    title.className = `timeline-change-title ${change.delta > 0 ? "is-increase" : "is-decrease"}`;
    title.textContent = `${formatSignedInteger(change.delta)} refs on ${formatTimestamp(
      change.toPoint.timestamp
    )}`;

    const meta = document.createElement("p");
    meta.className = "timeline-change-meta";
    meta.textContent = `Revision ${change.toPoint.rev_id} compared with tracked revision ${change.fromPoint.rev_id} • ${formatInteger(
      change.toPoint.reference_count
    )} refs after change`;

    const actions = document.createElement("div");
    actions.className = "timeline-change-actions";
    actions.append(
      createTimelineActionLink(
        "View diff",
        formatRevisionDiffUrl(change.fromPoint.rev_id, change.toPoint.rev_id)
      ),
      createTimelineActionLink(
        "View revision",
        formatRevisionUrl(change.toPoint.rev_id)
      )
    );

    item.append(title, meta, actions);
    timelineChangeList.appendChild(item);
  });
}

function titleCaseToken(value) {
  return value.replace(/[A-Za-z]+/g, (match) => (
    match.charAt(0).toUpperCase() + match.slice(1).toLowerCase()
  ));
}

function formatAuthorKey(authorKey) {
  if (!authorKey) {
    return "";
  }
  return authorKey
    .split(/\s+/)
    .filter(Boolean)
    .map((token) => titleCaseToken(token))
    .join(" ");
}

function formatSourceDetailLine(source) {
  const parts = [];
  const author = formatAuthorKey(source.author_key);

  if (author && source.year) {
    parts.push(`${author} (${source.year})`);
  } else if (author) {
    parts.push(author);
  } else if (source.year) {
    parts.push(source.year);
  }

  if (source.source_type === "both") {
    parts.push("Appears as template and ref-tag");
  } else if (source.source_type === "template") {
    parts.push("Appears as citation template");
  } else if (source.source_type === "ref_tag") {
    parts.push("Appears as plain-text reference");
  }

  return parts.join(" • ");
}

function formatSourceFacts(source) {
  const facts = [
    `Seen in ${formatInteger(source.n_revisions_cited)} revisions`,
    source.is_current ? "Still cited in latest revision" : "Historical source only",
  ];

  if (source.cross_page_count) {
    facts.push(`Used on ${formatInteger(source.cross_page_count)} Wikipedia pages`);
  }
  if (source.page_citation_count) {
    facts.push(`${formatInteger(source.page_citation_count)} citation uses on this page`);
  }

  return facts;
}

function createIdentifierLink(label, value, href) {
  const item = document.createElement("li");
  item.className = "source-identifier-item";

  const prefix = document.createElement("span");
  prefix.className = "source-identifier-label";
  prefix.textContent = `${label}:`;

  const content = document.createElement(href ? "a" : "span");
  content.className = "source-identifier-value";
  content.textContent = value;

  if (href) {
    content.href = href;
    content.target = "_blank";
    content.rel = "noreferrer";
  }

  item.append(prefix, content);
  return item;
}

function formatRelatedPageFacts(page) {
  const facts = [];
  if (page.n_revisions_cited) {
    facts.push(`Seen in ${formatInteger(page.n_revisions_cited)} revisions`);
  }
  if (page.is_current === true) {
    facts.push("Still cites this source");
  } else if (page.is_current === false) {
    facts.push("Historical use only");
  }
  return facts;
}

function renderSourceUsagePages(container, sourceUsage) {
  container.replaceChildren();
  container.classList.remove("hidden");

  const summary = document.createElement("p");
  summary.className = "source-usage-summary";
  summary.textContent = sourceUsage.pages.length
    ? `Showing ${formatInteger(sourceUsage.pages.length)} of ${formatInteger(sourceUsage.total_pages)} related pages using this source.`
    : "No other tracked pages are using this source.";
  container.appendChild(summary);

  if (!sourceUsage.pages.length) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "No other tracked pages were returned.";
    container.appendChild(empty);
    return;
  }

  const list = document.createElement("ul");
  list.className = "related-page-list";

  sourceUsage.pages.forEach((page) => {
    const item = document.createElement("li");
    item.className = "related-page-item";

    const link = document.createElement("a");
    link.className = "related-page-link";
    link.href = dashboardUrl(page.domain || currentDomain, page.page_id);
    link.textContent = page.page_title || `Page ${page.page_id}`;
    if (page.domain && page.domain !== currentDomain) {
      link.textContent += ` (${page.domain})`;
    }

    const meta = document.createElement("p");
    meta.className = "related-page-meta";
    meta.textContent = formatRelatedPageFacts(page).join(" • ")
      || `Page ID ${page.page_id}`;

    item.append(link, meta);
    list.appendChild(item);
  });

  container.appendChild(list);
}

async function toggleSourceUsage(button, source) {
  const container = button.parentElement?.nextElementSibling;
  if (!container) {
    return;
  }

  if (container.dataset.loaded === "true") {
    const isHidden = container.classList.toggle("hidden");
    button.textContent = isHidden ? "Show related pages" : "Hide related pages";
    return;
  }

  button.disabled = true;
  button.textContent = "Loading related pages...";

  try {
    const cacheKey = `${currentDomain}:${currentPageId}:${source.global_source_id}`;
    let sourceUsage = sourceUsageCache.get(cacheKey);
    if (!sourceUsage) {
      sourceUsage = await fetchJson(
        `${API_BASE}/sources/${encodeURIComponent(source.global_source_id)}/pages?exclude_domain=${encodeURIComponent(currentDomain)}&exclude_page_id=${encodeURIComponent(currentPageId)}&limit=50`
      );
      sourceUsageCache.set(cacheKey, sourceUsage);
    }

    renderSourceUsagePages(container, sourceUsage);
    container.dataset.loaded = "true";
    button.textContent = "Hide related pages";
  } catch (error) {
    container.replaceChildren();
    container.classList.remove("hidden");
    const message = document.createElement("p");
    message.className = "empty-state";
    message.textContent = error.message || "Unable to load related pages.";
    container.appendChild(message);
    button.textContent = "Show related pages";
  } finally {
    button.disabled = false;
  }
}

function renderTopSources(topSources) {
  topSourcesList.replaceChildren();

  const sourceCount = topSources.sources.length;
  topSourcesSummary.textContent = sourceCount
    ? `Showing ${formatInteger(sourceCount)} of ${formatInteger(topSources.total_sources)} resolved sources ranked by how long they stay on the page.`
    : "No resolved source rows were returned for this page.";

  if (!sourceCount) {
    const emptyItem = document.createElement("li");
    emptyItem.className = "empty-state";
    emptyItem.textContent = "No resolved sources are available for this page yet.";
    topSourcesList.appendChild(emptyItem);
    return;
  }

  topSources.sources.forEach((source, index) => {
    const item = document.createElement("li");
    item.className = "source-item";

    const rank = document.createElement("div");
    rank.className = "source-rank";
    rank.textContent = String(index + 1);

    const body = document.createElement("div");
    body.className = "source-body";

    const headingRow = document.createElement("div");
    headingRow.className = "source-heading-row";

    const title = document.createElement(source.extracted_url ? "a" : "span");
    title.className = "source-title";
    title.textContent = source.source_label;
    if (source.extracted_url) {
      title.href = source.extracted_url;
      title.target = "_blank";
      title.rel = "noreferrer";
    }

    const badges = document.createElement("div");
    badges.className = "source-badges";

    if (source.verdict) {
      const verdictBadge = document.createElement("span");
      verdictBadge.className = "source-badge";
      verdictBadge.textContent = source.verdict.replaceAll("_", " ");
      badges.appendChild(verdictBadge);
    }

    IDENTIFIERS.forEach(({ field, label }) => {
      if (source[field]) {
        const badge = document.createElement("span");
        badge.className = "source-badge source-badge-identifier";
        badge.textContent = label;
        badges.appendChild(badge);
      }
    });

    headingRow.append(title, badges);

    const detailLine = document.createElement("p");
    detailLine.className = "source-detail-line";
    detailLine.textContent = formatSourceDetailLine(source) || "Metadata is limited for this source.";

    const identifiers = document.createElement("ul");
    identifiers.className = "source-identifier-list";

    IDENTIFIERS.forEach(({ field, label, href }) => {
      const value = source[field];
      if (value) {
        identifiers.appendChild(createIdentifierLink(label, value, href ? href(value) : null));
      }
    });

    const facts = document.createElement("ul");
    facts.className = "source-fact-list";

    formatSourceFacts(source).forEach((factText) => {
      const fact = document.createElement("li");
      fact.textContent = factText;
      facts.appendChild(fact);
    });

    const actionRow = document.createElement("div");
    actionRow.className = "source-action-row";

    if ((source.cross_page_count || 0) > 1) {
      const usageButton = document.createElement("button");
      usageButton.type = "button";
      usageButton.className = "source-usage-button";
      usageButton.textContent = "Show related pages";
      usageButton.addEventListener("click", () => {
        toggleSourceUsage(usageButton, source);
      });
      actionRow.appendChild(usageButton);
    }

    const relatedPagesContainer = document.createElement("div");
    relatedPagesContainer.className = "source-usage-container hidden";

    body.append(headingRow, detailLine);
    if (identifiers.childElementCount) {
      body.appendChild(identifiers);
    }
    body.append(facts, actionRow, relatedPagesContainer);
    item.append(rank, body);
    topSourcesList.appendChild(item);
  });
}

function renderSourceKindChart(items) {
  sourceKindChart.replaceChildren();

  const nonZeroItems = items.filter((item) => item.count > 0);
  if (!nonZeroItems.length) {
    const emptyMessage = document.createElement("p");
    emptyMessage.className = "empty-state";
    emptyMessage.textContent = "No source-kind breakdown was returned for this revision.";
    sourceKindChart.appendChild(emptyMessage);
    return;
  }

  const maxCount = Math.max(...nonZeroItems.map((item) => item.count), 1);

  nonZeroItems.forEach((item) => {
    const row = document.createElement("div");
    row.className = "bar-row";

    const meta = document.createElement("div");
    meta.className = "bar-meta";

    const label = document.createElement("span");
    label.className = "bar-label";
    label.textContent = item.label;

    const value = document.createElement("span");
    value.className = "bar-value";
    value.textContent = `${formatInteger(item.count)} refs`;

    meta.append(label, value);

    const track = document.createElement("div");
    track.className = "bar-track";

    const fill = document.createElement("div");
    fill.className = "bar-fill";
    fill.style.width = `${(item.count / maxCount) * 100}%`;

    track.appendChild(fill);
    row.append(meta, track);
    sourceKindChart.appendChild(row);
  });
}

function renderCitationTypeChart(items) {
  citationTypeChart.replaceChildren();

  if (!items.length) {
    const emptyMessage = document.createElement("p");
    emptyMessage.className = "empty-state";
    emptyMessage.textContent = "No citation types were returned for this revision.";
    citationTypeChart.appendChild(emptyMessage);
    return;
  }

  const maxCount = Math.max(...items.map((item) => item.count), 1);

  items.forEach((item) => {
    const row = document.createElement("div");
    row.className = "bar-row";

    const meta = document.createElement("div");
    meta.className = "bar-meta";

    const label = document.createElement("span");
    label.className = "bar-label";
    label.textContent = formatCitationTypeLabel(item.citation_type);

    const value = document.createElement("span");
    value.className = "bar-value";
    value.textContent = `${item.count} refs`;

    meta.append(label, value);

    const track = document.createElement("div");
    track.className = "bar-track";

    const fill = document.createElement("div");
    fill.className = "bar-fill";
    fill.style.width = `${(item.count / maxCount) * 100}%`;

    track.appendChild(fill);
    row.append(meta, track);
    citationTypeChart.appendChild(row);
  });
}

function renderTopTemplates(items) {
  topTemplatesList.replaceChildren();

  if (!items.length) {
    const emptyItem = document.createElement("li");
    emptyItem.className = "empty-state";
    emptyItem.textContent = "No templates were present in the latest revision.";
    topTemplatesList.appendChild(emptyItem);
    return;
  }

  items.forEach((item) => {
    const row = document.createElement("li");
    row.className = "template-item";

    const name = document.createElement("span");
    name.className = "template-name";
    name.textContent = item.template_name;

    const count = document.createElement("span");
    count.className = "bar-value";
    count.textContent = `${item.count}`;

    row.append(name, count);
    topTemplatesList.appendChild(row);
  });
}

function renderTimeline(timeline) {
  if (timelineChartInstance) {
    timelineChartInstance.dispose();
    timelineChartInstance = null;
  }

  timelineChart.replaceChildren();

  if (!timeline.points.length) {
    const emptyMessage = document.createElement("p");
    emptyMessage.className = "empty-state";
    emptyMessage.textContent = "No timeline points were returned for this page.";
    timelineChart.appendChild(emptyMessage);

    timelinePeakRefs.textContent = "0";
    timelinePeakTemplates.textContent = "0";
    timelinePeakReuses.textContent = "0";
    timelinePeakHighConfidence.textContent = "0";
    timelinePointCount.textContent = "0";
    timelineBiggestIncrease.textContent = "-";
    timelineBiggestDecrease.textContent = "-";
    renderTimelineInspector(null);
    renderTimelineChangeList([]);
    return;
  }

  const plottedPoints = timeline.points.map((point, index, points) => {
    const previousPoint = index > 0 ? points[index - 1] : null;
    return {
      ...point,
      index,
      previousPoint,
      delta: previousPoint ? point.reference_count - previousPoint.reference_count : 0,
    };
  });

  const peakPoint = plottedPoints.reduce((best, current) => {
    if (current.reference_count > best.reference_count) {
      return current;
    }
    return best;
  }, plottedPoints[0]);

  const changes = plottedPoints
    .filter((point) => point.previousPoint && point.delta !== 0)
    .map((point) => ({
      fromPoint: point.previousPoint,
      toPoint: point,
      delta: point.delta,
      magnitude: Math.abs(point.delta),
    }))
    .sort((left, right) => right.magnitude - left.magnitude || right.toPoint.index - left.toPoint.index);

  const biggestIncrease = changes
    .filter((change) => change.delta > 0)
    .sort((left, right) => right.delta - left.delta || right.toPoint.index - left.toPoint.index)[0];
  const biggestDecrease = changes
    .filter((change) => change.delta < 0)
    .sort((left, right) => left.delta - right.delta || right.toPoint.index - left.toPoint.index)[0];

  timelinePeakRefs.textContent = String(peakPoint.reference_count);
  timelinePeakTemplates.textContent = String(peakPoint.template_count);
  timelinePeakReuses.textContent = String(peakPoint.named_ref_count);
  timelinePeakHighConfidence.textContent = String(peakPoint.high_confidence_count);
  timelinePointCount.textContent = String(plottedPoints.length);
  timelineBiggestIncrease.textContent = formatTimelineChangeStat(biggestIncrease);
  timelineBiggestDecrease.textContent = formatTimelineChangeStat(biggestDecrease);
  renderTimelineChangeList(changes);

  const latestPoint = plottedPoints[plottedPoints.length - 1];
  let pinnedPointIndex = null;

  const clearPinnedSelection = () => {
    pinnedPointIndex = null;
    renderTimelineInspector(latestPoint);
  };

  const showTimelinePoint = (point, { pinned = false } = {}) => {
    renderTimelineInspector(point, {
      isPinned: pinned,
      onClearPin: clearPinnedSelection,
    });
  };

  showTimelinePoint(latestPoint);

  if (!window.echarts) {
    const emptyMessage = document.createElement("p");
    emptyMessage.className = "empty-state";
    emptyMessage.textContent = "The chart library did not load, so the interactive timeline is unavailable.";
    timelineChart.appendChild(emptyMessage);
    return;
  }

  timelineChartInstance = window.echarts.init(timelineChart);

  const highlightedPoints = [
    {
      label: "Peak",
      point: peakPoint,
      color: "#0645ad",
    },
  ];

  if (biggestIncrease) {
    highlightedPoints.push({
      label: "Biggest jump",
      point: biggestIncrease.toPoint,
      color: "#14866d",
    });
  }
  if (biggestDecrease) {
    highlightedPoints.push({
      label: "Biggest drop",
      point: biggestDecrease.toPoint,
      color: "#b32424",
    });
  }

  timelineChartInstance.setOption({
    animation: false,
    grid: {
      top: 24,
      right: 24,
      bottom: plottedPoints.length > 40 ? 96 : 56,
      left: 56,
      containLabel: false,
    },
    tooltip: {
      trigger: "axis",
      axisPointer: {
        type: "line",
        snap: true,
      },
      backgroundColor: "rgba(32, 33, 34, 0.95)",
      borderWidth: 0,
      textStyle: {
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif',
      },
      formatter(params) {
        const hoveredPoint = plottedPoints[params[0]?.dataIndex ?? plottedPoints.length - 1];
        const deltaSummary = hoveredPoint.previousPoint
          ? `${formatSignedInteger(hoveredPoint.delta)} vs tracked revision ${hoveredPoint.previousPoint.rev_id}`
          : "Earliest tracked revision";
        return [
          `<strong>Revision ${escapeHtml(hoveredPoint.rev_id)}</strong>`,
          escapeHtml(formatTimestamp(hoveredPoint.timestamp)),
          `${escapeHtml(formatInteger(hoveredPoint.reference_count))} references`,
          `${escapeHtml(formatInteger(hoveredPoint.template_count))} templates • ${escapeHtml(
            formatInteger(hoveredPoint.named_ref_count)
          )} reuses`,
          `${escapeHtml(formatInteger(hoveredPoint.high_confidence_count))} high-confidence sources`,
          escapeHtml(deltaSummary),
        ].join("<br />");
      },
    },
    xAxis: {
      type: "category",
      boundaryGap: false,
      data: plottedPoints.map((point) => point.index),
      axisLine: {
        lineStyle: {
          color: "#a2a9b1",
        },
      },
      axisTick: {
        show: false,
      },
      axisLabel: {
        color: "#54595d",
        hideOverlap: true,
        formatter(value) {
          const index = Number(value);
          const point = plottedPoints[index];
          if (!point) {
            return "";
          }
          const labelIndexes = new Set([
            0,
            Math.floor((plottedPoints.length - 1) * 0.33),
            Math.floor((plottedPoints.length - 1) * 0.66),
            plottedPoints.length - 1,
          ]);
          return labelIndexes.has(index) ? formatCompactDate(point.timestamp) : "";
        },
      },
    },
    yAxis: {
      type: "value",
      minInterval: 1,
      splitNumber: 4,
      axisLabel: {
        color: "#54595d",
        formatter(value) {
          return formatInteger(value);
        },
      },
      splitLine: {
        lineStyle: {
          color: "#eaecf0",
        },
      },
    },
    dataZoom: plottedPoints.length > 40
      ? [
          {
            type: "inside",
            zoomLock: false,
          },
          {
            type: "slider",
            height: 18,
            bottom: 18,
            borderColor: "#c8ccd1",
            backgroundColor: "#f1f3f5",
            fillerColor: "rgba(51, 102, 204, 0.18)",
          },
        ]
      : [],
    legend: {
      bottom: plottedPoints.length > 40 ? 44 : 4,
      left: 56,
      itemWidth: 18,
      textStyle: { color: "#54595d" },
      data: ["References", "High-confidence sources"],
    },
    series: [
      {
        name: "High-confidence sources",
        type: "line",
        step: "end",
        showSymbol: false,
        lineStyle: {
          width: 2,
          type: "dashed",
          color: "#14866d",
        },
        itemStyle: {
          color: "#14866d",
        },
        data: plottedPoints.map((point) => point.high_confidence_count),
      },
      {
        name: "References",
        type: "line",
        step: "end",
        showSymbol: false,
        symbol: "circle",
        symbolSize: 7,
        lineStyle: {
          width: 3,
          color: "#3366cc",
        },
        areaStyle: {
          color: "rgba(51, 102, 204, 0.12)",
        },
        itemStyle: {
          color: "#3366cc",
        },
        emphasis: {
          focus: "series",
          scale: true,
        },
        data: plottedPoints.map((point) => point.reference_count),
        markPoint: {
          symbol: "circle",
          symbolSize: 16,
          label: {
            show: false,
          },
          data: highlightedPoints.map((entry) => ({
            name: entry.label,
            coord: [entry.point.index, entry.point.reference_count],
            itemStyle: {
              color: entry.color,
            },
          })),
        },
      },
    ],
  });

  timelineChartInstance.on("updateAxisPointer", (event) => {
    if (pinnedPointIndex !== null) {
      return;
    }
    const axisInfo = event.axesInfo?.[0];
    if (!axisInfo) {
      return;
    }
    const index = Number(axisInfo.value);
    if (Number.isInteger(index) && plottedPoints[index]) {
      showTimelinePoint(plottedPoints[index]);
    }
  });

  timelineChartInstance.on("click", (event) => {
    const index = event.dataIndex;
    if (Number.isInteger(index) && plottedPoints[index]) {
      if (pinnedPointIndex === index) {
        clearPinnedSelection();
        return;
      }
      pinnedPointIndex = index;
      showTimelinePoint(plottedPoints[index], { pinned: true });
    }
  });

  timelineChartInstance.getZr().on("globalout", () => {
    if (pinnedPointIndex !== null && plottedPoints[pinnedPointIndex]) {
      showTimelinePoint(plottedPoints[pinnedPointIndex], { pinned: true });
      return;
    }
    showTimelinePoint(latestPoint);
  });

  if (!timelineResizeListenerAttached) {
    window.addEventListener("resize", () => {
      if (timelineChartInstance) {
        timelineChartInstance.resize();
      }
    });
    timelineResizeListenerAttached = true;
  }
}

function renderPage(summary, timeline, topSources) {
  summaryPanel.classList.remove("hidden");
  currentPageId = summary.page_id;
  currentDomain = summary.domain || currentDomain;
  rememberDomain(currentDomain);

  pageTitle.textContent = summary.page_title || `Page ${summary.page_id}`;
  pageIdValue.textContent = summary.page_id;
  pageDomainValue.textContent = currentDomain;
  pageNamespace.textContent = summary.page_namespace ?? "Unknown";

  latestReferenceCount.textContent = summary.latest_revision.reference_count;
  revisionCount.textContent = summary.revision_count;
  firstSeen.textContent = formatTimestamp(summary.first_revision_timestamp);
  latestSeen.textContent = formatTimestamp(summary.latest_revision_timestamp);

  latestRevisionMeta.textContent = `Revision ${summary.latest_revision.rev_id} • ${formatTimestamp(
    summary.latest_revision.timestamp
  )}`;

  wikipediaLink.href = summary.url || formatWikipediaUrlFromPageId(summary.page_id);
  wikipediaLink.classList.remove("hidden");

  renderCitationTypeChart(summary.latest_revision.citation_type_breakdown);
  renderTopTemplates(summary.latest_revision.top_templates);
  renderSourceKindChart(summary.source_profile.source_kind_breakdown);
  renderTimeline(timeline);
  renderTopSources(topSources);
}

async function fetchJson(endpoint) {
  const response = await fetch(endpoint, {
    headers: { Accept: "application/json" },
  });

  if (!response.ok) {
    let detail = `Request failed with status ${response.status}.`;
    try {
      const errorPayload = await response.json();
      if (typeof errorPayload.error === "string") {
        detail = errorPayload.error;
      } else if (typeof errorPayload.detail === "string") {
        detail = errorPayload.detail;
      } else if (Array.isArray(errorPayload.detail) && errorPayload.detail[0]?.msg) {
        detail = errorPayload.detail[0].msg;
      }
    } catch (_) {
      // Ignore JSON parse failures and keep the generic error text.
    }
    throw new Error(detail);
  }

  return response.json();
}

const DOMAIN_STORAGE_KEY = "wcd.domains";

function knownDomains() {
  try {
    const stored = JSON.parse(window.localStorage.getItem(DOMAIN_STORAGE_KEY) || "[]");
    return Array.isArray(stored) ? stored : [];
  } catch (_) {
    return [];
  }
}

function rememberDomain(domain) {
  const domains = [domain, ...knownDomains().filter((d) => d !== domain)].slice(0, 12);
  try {
    window.localStorage.setItem(DOMAIN_STORAGE_KEY, JSON.stringify(domains));
  } catch (_) {
    // storage unavailable: the datalist just stays as it is
  }
  renderDomainOptions(domains);
}

function renderDomainOptions(domains) {
  domainOptions.replaceChildren(
    ...[DEFAULT_DOMAIN, ...domains.filter((d) => d !== DEFAULT_DOMAIN)].map((d) => {
      const option = document.createElement("option");
      option.value = d;
      return option;
    })
  );
}

function selectedDomain() {
  const value = domainInput.value.trim().toLowerCase();
  return value || DEFAULT_DOMAIN;
}

async function loadPage(endpoint, statusLabel) {
  setBusyState(true);
  setStatus(statusLabel);

  try {
    const summary = await fetchJson(endpoint);
    const [timeline, topSources] = await Promise.all([
      fetchJson(pagePath(summary.domain, summary.page_id, "timeline")),
      fetchJson(pagePath(summary.domain, summary.page_id, "top-sources?limit=25")),
    ]);
    renderPage(summary, timeline, topSources);
    pageIdInput.value = String(summary.page_id);
    domainInput.value = summary.domain;
    setStatus(`Loaded page ${summary.page_id} on ${summary.domain}.`);

    window.history.replaceState({}, "", dashboardUrl(summary.domain, summary.page_id));
  } catch (error) {
    setStatus(error.message || "Unable to load page summary.", true);
  } finally {
    setBusyState(false);
  }
}

function loadPageById(domain, pageId) {
  loadPage(pagePath(domain, pageId, "summary"), `Loading page ${pageId} on ${domain}...`);
}

async function loadPageByUrl(articleUrl) {
  setBusyState(true);
  setStatus(`Resolving ${articleUrl}...`);
  try {
    const article = await fetchJson(`${API_BASE}/article?url=${encodeURIComponent(articleUrl)}`);
    loadPageById(article.domain, article.page_id);
  } catch (error) {
    setStatus(error.message || "Unable to resolve that article URL.", true);
    setBusyState(false);
  }
}

pageForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const value = pageIdInput.value.trim();
  if (!value) {
    setStatus("Enter a page ID or an article URL before loading.", true);
    return;
  }
  if (/^\d+$/.test(value)) {
    loadPageById(selectedDomain(), value);
  } else {
    loadPageByUrl(value);
  }
});

randomButton.addEventListener("click", () => {
  const domain = domainInput.value.trim().toLowerCase();
  const query = domain ? `?domain=${encodeURIComponent(domain)}` : "";
  loadPage(`${API_BASE}/pages/random/summary${query}`, "Selecting a random page...");
});

window.addEventListener("DOMContentLoaded", () => {
  renderDomainOptions(knownDomains());
  const params = new URLSearchParams(window.location.search);
  let domain = params.get("domain");
  let pageId = params.get("page_id");
  // also accept #domain/page_id (shareable hash links)
  const hashMatch = window.location.hash.match(/^#\/?([^/]+)\/(\d+)$/);
  if (!pageId && hashMatch) {
    [, domain, pageId] = hashMatch;
  }
  if (domain) {
    domainInput.value = domain;
  }
  if (pageId) {
    pageIdInput.value = pageId;
    loadPageById(domain || DEFAULT_DOMAIN, pageId);
  }
});
