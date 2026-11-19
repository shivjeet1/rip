"use strict";

const state = {
  mode: "MOCK",
  health: null,
  overview: null,
  incidents: [],
  selectedId: null,
  detail: null,
  activeTab: "diff",
  busy: false,
  diagnosing: false,
  detailRequest: 0,
  recoveryCommand: "",
};

const scenarioLabels = {
  NORMAL: "NORMAL",
  A: "A · PROVIDER SKEW",
  B: "B · SCHEMA DRIFT",
  C: "C · PARTIAL PAYOUT",
};

const categoryTitles = {
  DATA_SKEW: "Provider skew is throttling claim adjudication",
  SCHEMA_DRIFT: "Schema drift tripped the Silver quality gate",
  PARTIAL_PAYOUT_WRITE: "Partial payout batch requires manual cleanup",
  OTHER: "Unclassified pipeline failure needs review",
};

const scenarioTitles = {
  NORMAL: "Baseline completed",
  A: "Provider skew injected",
  B: "Schema drift injected",
  C: "Partial payout committed",
};

const byId = (id) => document.getElementById(id);

function escapeHtml(value) {
  const replacements = {
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  };
  return String(value === null || value === undefined ? "" : value).replace(/[&<>"']/g, (character) => replacements[character]);
}

function formatNumber(value) {
  const numeric = Number(value || 0);
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(numeric);
}

function formatMoney(value, compact) {
  const numeric = Number(value || 0);
  if (compact) {
    const absolute = Math.abs(numeric);
    if (absolute >= 1000000000) {
      return `$${(numeric / 1000000000).toFixed(2)}B`;
    }
    if (absolute >= 1000000) {
      return `$${(numeric / 1000000).toFixed(1)}M`;
    }
    if (absolute >= 1000) {
      return `$${(numeric / 1000).toFixed(1)}K`;
    }
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(numeric);
}

function formatPercent(value, digits) {
  const numeric = Number(value || 0);
  return `${numeric.toFixed(digits === undefined ? 2 : digits)}%`;
}

function formatDuration(value) {
  const numeric = Number(value || 0);
  if (numeric >= 60) {
    return `${(numeric / 60).toFixed(1)}m`;
  }
  return `${numeric.toFixed(1)}s`;
}

function formatTimestamp(value) {
  if (!value) {
    return "—";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  return parsed.toLocaleString("en-US", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatRelative(value) {
  if (!value) {
    return "no cache";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  const seconds = Math.round((Date.now() - parsed.getTime()) / 1000);
  if (seconds < 5) {
    return "just now";
  }
  if (seconds < 60) {
    return `${seconds}s ago`;
  }
  if (seconds < 3600) {
    return `${Math.floor(seconds / 60)}m ago`;
  }
  if (seconds < 86400) {
    return `${Math.floor(seconds / 3600)}h ago`;
  }
  return `${Math.floor(seconds / 86400)}d ago`;
}

function setText(id, value) {
  const element = byId(id);
  if (element) {
    element.textContent = value;
  }
}

function setTone(element, tone) {
  if (element) {
    element.setAttribute("data-tone", tone);
  }
}

function setGlobalProgress(visible) {
  const progress = byId("global-progress");
  if (progress) {
    progress.hidden = !visible;
  }
}

function updateCacheLabel(value) {
  const label = byId("cache-sync-label");
  if (!label) {
    return;
  }
  label.textContent = formatRelative(value);
  label.title = value ? formatTimestamp(value) : "No cache synchronization yet";
}

function toast(message, tone) {
  const region = byId("toast-region");
  if (!region) {
    return;
  }
  const node = document.createElement("div");
  node.className = `toast toast--${tone || "info"}`;
  node.textContent = message;
  region.appendChild(node);
  window.setTimeout(() => {
    node.classList.add("is-leaving");
    window.setTimeout(() => node.remove(), 220);
  }, tone === "error" ? 7200 : 4800);
}

async function requestJson(path, options) {
  const request = options || {};
  const headers = Object.assign({ Accept: "application/json" }, request.headers || {});
  if (request.body) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(path, Object.assign({}, request, { headers }));
  if (!response.ok) {
    let message = `Request failed with status ${response.status}`;
    try {
      const payload = await response.json();
      if (payload && payload.detail) {
        message = typeof payload.detail === "string" ? payload.detail : JSON.stringify(payload.detail);
      }
    } catch (error) {
      message = `${message} ${response.statusText || ""}`.trim();
    }
    throw new Error(message);
  }
  return response.json();
}

function applyIncidentPayload(payload, preferredId) {
  state.incidents = Array.isArray(payload.incidents) ? payload.incidents : [];
  if (payload.mode) {
    state.mode = payload.mode;
  }
  updateCacheLabel(payload.cache_synced_at);
  setText("queue-count", formatNumber(state.incidents.length));
  setText("queue-subtitle", state.incidents.length ? "Criticality first, newest signal next." : "No active incidents in the current cache.");
  const selectedExists = state.incidents.some((incident) => incident.incident_id === preferredId);
  const currentExists = state.incidents.some((incident) => incident.incident_id === state.selectedId);
  if (selectedExists) {
    state.selectedId = preferredId;
  } else if (!currentExists) {
    state.selectedId = state.incidents.length ? state.incidents[0].incident_id : null;
  }
  renderQueue();
  if (!state.incidents.length) {
    state.detail = null;
    showEmptyTriage();
  }
  return state.selectedId;
}

async function fetchIncidents(options) {
  const settings = options || {};
  const suffix = settings.refresh ? "?refresh=true" : "";
  const payload = await requestJson(`/api/incidents${suffix}`);
  return applyIncidentPayload(payload, settings.preferredId || null);
}

function renderQueue() {
  const list = byId("incident-list");
  if (!list) {
    return;
  }
  if (!state.incidents.length) {
    list.innerHTML = '<div class="queue-empty">No incidents yet. Trigger Scenario A, B or C to populate the queue.</div>';
    return;
  }
  list.innerHTML = state.incidents
    .map((incident) => {
      const status = String(incident.status || "UNKNOWN").toLowerCase();
      const priority = String(incident.business_criticality || "MEDIUM").toLowerCase();
      const selected = incident.incident_id === state.selectedId ? " is-selected" : "";
      const diagnosed = incident.diagnosed ? '<span class="incident-diagnosed">DIAGNOSED</span>' : "";
      const skew = Number(incident.top_key_skew_ratio || 0);
      return `
        <button class="incident-item${selected}" type="button" data-incident-id="${escapeHtml(incident.incident_id)}" data-status="${escapeHtml(status)}" aria-pressed="${incident.incident_id === state.selectedId}">
          <span class="incident-topline">
            <span class="incident-scenario">${escapeHtml(scenarioLabels[incident.scenario] || incident.scenario)}</span>
            <span class="priority-tag" data-priority="${escapeHtml(priority)}">${escapeHtml(incident.business_criticality || "MEDIUM")}</span>
            ${diagnosed}
          </span>
          <span class="incident-error">${escapeHtml(incident.error_class || "UNCLASSIFIED_FAILURE")}</span>
          <span class="incident-meta">
            <span>${escapeHtml(formatMoney(incident.payout_exposure_usd, true))} exposure</span>
            <span>${escapeHtml(formatPercent(skew * 100, 1))} skew</span>
            <span class="incident-status" data-status="${escapeHtml(status)}"><i class="incident-status-dot"></i>${escapeHtml(incident.status)}</span>
          </span>
        </button>
      `;
    })
    .join("");
}

function showEmptyTriage() {
  byId("triage-empty").hidden = false;
  byId("triage-content").hidden = true;
  byId("triage-loading").hidden = true;
}

function showTriageLoading() {
  byId("triage-empty").hidden = true;
  byId("triage-content").hidden = true;
  byId("triage-loading").hidden = false;
}

function renderHealth() {
  const health = state.health;
  if (!health) {
    return;
  }
  const modePill = byId("mode-pill");
  modePill.setAttribute("data-mode", health.mock_mode ? "mock" : "live");
  setText("mode-label", health.mock_mode ? "MOCK" : "DATABRICKS LIVE");
  setText("hero-mode", health.mock_mode ? "Offline mock" : "Live workspace");
  setText("hero-llm", health.llm_ready ? health.llm_model : "deterministic fallback");
  updateCacheLabel(health.cache_synced_at);
}

function renderOverview() {
  const overview = state.overview;
  if (!overview) {
    return;
  }
  if (overview.mode) {
    state.mode = overview.mode;
  }
  setText("total-runs-value", formatNumber(overview.total_runs));
  const latest = overview.latest_run;
  setText(
    "total-runs-meta",
    latest ? `Latest: ${scenarioLabels[latest.scenario] || latest.scenario} · ${latest.status}` : "Pipeline history"
  );
  setText("sla-breaches-value", formatNumber(overview.sla_breaches));
  setText(
    "sla-breaches-meta",
    overview.sla_breaches ? `${formatNumber(overview.open_incidents)} open incidents in cache` : "No breached stages"
  );
  setText("payout-exposure-value", formatMoney(overview.payout_exposure_usd, true));
  setText(
    "payout-exposure-meta",
    `Critical ${formatMoney(overview.critical_payout_exposure_usd, true)} · ${formatNumber(overview.open_incidents)} open`
  );
  const rate = Number(overview.latest_reconciliation_rate || 0);
  setText("reconciliation-value", formatPercent(rate));
  const rejection = overview.latest_reconciliation
    ? `${formatNumber(overview.latest_reconciliation.records_valid)} of ${formatNumber(overview.latest_reconciliation.records_received)} valid`
    : "No reconciliation report";
  setText("reconciliation-meta", rejection);
  setTone(byId("sla-card"), overview.sla_breaches ? "warning" : "neutral");
  setTone(byId("exposure-card"), overview.payout_exposure_usd ? "danger" : "neutral");
  setTone(byId("reconciliation-card"), rate >= 98 ? "safe" : rate >= 85 ? "warning" : "danger");
  setText("hero-open", formatNumber(overview.open_incidents));
  setText("hero-diagnosed", formatNumber(overview.diagnosed_incidents));
  updateCacheLabel(overview.cache_synced_at);
}

function closeAllTabs() {
  const panels = ["panel-diff", "panel-plan", "panel-trace"];
  panels.forEach((id) => {
    const panel = byId(id);
    if (panel) {
      panel.hidden = true;
    }
  });
  document.querySelectorAll("[data-tab]").forEach((button) => {
    button.classList.remove("is-active");
    button.setAttribute("aria-selected", "false");
    button.tabIndex = -1;
  });
}

function setActiveTab(tab, focus) {
  if (!["diff", "plan", "trace"].includes(tab)) {
    return;
  }
  state.activeTab = tab;
  closeAllTabs();
  const button = byId(`tab-${tab}`);
  const panel = byId(`panel-${tab}`);
  if (button) {
    button.classList.add("is-active");
    button.setAttribute("aria-selected", "true");
    button.tabIndex = 0;
    if (focus) {
      button.focus();
    }
  }
  if (panel) {
    panel.hidden = false;
  }
}

function highlightCode(source) {
  const pattern = /("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|\b(?:from|import|as|def|return|if|else|elif|for|while|with|try|except|raise|and|or|not|in|is|None|True|False|class|lambda|select|where|DELETE|FROM|MERGE|INTO|MATCHED|THEN|UPDATE|INSERT)\b|\b\d+(?:\.\d+)?\b|#[^\n]*)/g;
  let output = "";
  let lastIndex = 0;
  let match = pattern.exec(source);
  while (match !== null) {
    output += escapeHtml(source.slice(lastIndex, match.index));
    const token = match[0];
    let className = "tok-keyword";
    if (token.startsWith('"') || token.startsWith("'")) {
      className = "tok-string";
    } else if (token.startsWith("#")) {
      className = "tok-comment";
    } else if (/^\d/.test(token)) {
      className = "tok-number";
    }
    output += `<span class="${className}">${escapeHtml(token)}</span>`;
    lastIndex = match.index + token.length;
    match = pattern.exec(source);
  }
  output += escapeHtml(source.slice(lastIndex));
  return output;
}

function renderDiff(diffText) {
  if (!diffText) {
    return '<div class="diff-empty">No remediation diff is cached. Run the agent to inspect notebook source and generate a verified patch.</div>';
  }
  const lines = String(diffText).replace(/\r\n/g, "\n").split("\n");
  let oldLine = 0;
  let newLine = 0;
  let output = "";
  lines.forEach((line) => {
    if (line.startsWith("@@")) {
      const hunk = line.match(/^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@/);
      if (hunk) {
        oldLine = Number(hunk[1]);
        newLine = Number(hunk[2]);
      }
      output += `<div class="diff-line diff-line--hunk"><span class="diff-code">${escapeHtml(line)}</span></div>`;
      return;
    }
    if (line.startsWith("--- ") || line.startsWith("+++ ")) {
      output += `<div class="diff-line diff-line--file"><span class="diff-code">${escapeHtml(line)}</span></div>`;
      return;
    }
    if (line.startsWith("diff --git") || line.startsWith("index ") || line.startsWith("new file mode") || line.startsWith("deleted file mode")) {
      output += `<div class="diff-line diff-line--meta"><span class="diff-code">${escapeHtml(line)}</span></div>`;
      return;
    }
    if (!line) {
      output += '<div class="diff-line diff-line--ctx"><span class="diff-num"></span><span class="diff-num"></span><span class="diff-sign"> </span><span class="diff-code"></span></div>';
      return;
    }
    const marker = line.charAt(0);
    const content = line.slice(1);
    const oldLabel = marker === "+" ? "" : String(oldLine);
    const newLabel = marker === "-" ? "" : String(newLine);
    if (marker === "+") {
      output += `<div class="diff-line diff-line--add"><span class="diff-num">${oldLabel}</span><span class="diff-num">${newLabel}</span><span class="diff-sign">+</span><span class="diff-code">${highlightCode(content)}</span></div>`;
      newLine += 1;
      return;
    }
    if (marker === "-") {
      output += `<div class="diff-line diff-line--del"><span class="diff-num">${oldLabel}</span><span class="diff-num">${newLabel}</span><span class="diff-sign">−</span><span class="diff-code">${highlightCode(content)}</span></div>`;
      oldLine += 1;
      return;
    }
    output += `<div class="diff-line diff-line--ctx"><span class="diff-num">${oldLabel}</span><span class="diff-num">${newLabel}</span><span class="diff-sign"> </span><span class="diff-code">${highlightCode(content)}</span></div>`;
    oldLine += 1;
    newLine += 1;
  });
  return output;
}

function highlightPlan(line) {
  let html = escapeHtml(line);
  html = html.replace(/\b(AdaptiveSparkPlan|Project|Filter|Sort|SortMergeJoin|BroadcastHashJoin|HashAggregate|Exchange|ColumnarToRow|FileScan|WrittenTable|LocalTableScan|Batched|Location|PartitionFilters|PushedFilters|ReadSchema)\b/g, '<span class="plan-op">$1</span>');
  html = html.replace(/(\d+(?:\.\d+)?)/g, '<span class="plan-number">$1</span>');
  return html;
}

function renderPlan(planText) {
  if (!planText) {
    return '<div class="code-empty">No formatted physical plan was captured for this stage.</div>';
  }
  return String(planText)
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line, index) => {
      const lowered = line.toLowerCase();
      let className = "plan-line";
      if (line.startsWith("==")) {
        className += " plan-title";
      }
      if (lowered.includes("skew") || lowered.includes("rejection_rate") || lowered.includes("quality gate")) {
        className += " plan-warning";
      }
      return `<div class="${className}"><span class="code-gutter">${index + 1}</span><span class="code-text">${highlightPlan(line)}</span></div>`;
    })
    .join("");
}

function renderTrace(traceText) {
  if (!traceText) {
    return '<div class="code-empty">No traceback was captured. This stage did not raise a Python exception.</div>';
  }
  return String(traceText)
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line, index) => {
      let className = "code-line trace-frame";
      if (line.startsWith("Traceback")) {
        className = "code-line trace-first";
      } else if (/^[A-Za-z_][A-Za-z0-9_.]*:/.test(line)) {
        className = "code-line trace-exception";
      } else if (line.trimStart().startsWith('File "')) {
        className = "code-line trace-file";
      }
      return `<div class="${className}"><span class="code-gutter">${index + 1}</span><span class="code-text">${escapeHtml(line)}</span></div>`;
    })
    .join("");
}

function renderBadges(incident, diagnosis) {
  const container = byId("badge-row");
  const status = String(incident.status || "UNKNOWN");
  const statusTone = status === "FAILED" ? "fail" : status === "DEGRADED" ? "warn" : "safe";
  const priority = String(incident.business_criticality || "MEDIUM").toLowerCase();
  const priorityTone = priority === "critical" ? "fail" : priority === "high" ? "warn" : "neutral";
  const writeTone = String(incident.write_mode || "").includes("append") ? "fail" : "neutral";
  const badges = [
    `<span class="badge badge--${statusTone}">${escapeHtml(status)}</span>`,
    `<span class="badge badge--${priorityTone}">${escapeHtml(incident.business_criticality || "MEDIUM")} criticality</span>`,
    `<span class="badge badge--neutral">${escapeHtml(scenarioLabels[incident.scenario] || incident.scenario)}</span>`,
    `<span class="badge badge--${writeTone}">${escapeHtml(incident.write_mode || "write mode unknown")}</span>`,
    incident.sla_breached ? '<span class="badge badge--fail">SLA breached</span>' : '<span class="badge badge--safe">SLA intact</span>',
  ];
  if (diagnosis) {
    badges.push(`<span class="badge badge--safe">${escapeHtml(diagnosis.root_cause_category)}</span>`);
  }
  if (incident.diagnosed) {
    badges.push('<span class="badge badge--safe">Agent diagnosis cached</span>');
  }
  container.innerHTML = badges.join("");
}

function renderReconciliation(reconciliation) {
  const bar = byId("recon-bar");
  const legend = byId("recon-legend");
  if (!reconciliation) {
    setText("recon-received", "0");
    setText("recon-valid", "0");
    setText("recon-rejected", "0");
    setText("recon-rejection-rate", "0.00%");
    setText("recon-rate", "0.00%");
    byId("recon-rate").classList.remove("is-warning", "is-danger");
    bar.innerHTML = "";
    bar.setAttribute("aria-label", "No reconciliation report");
    legend.innerHTML = "";
    return;
  }
  const received = Number(reconciliation.records_received || 0);
  const valid = Number(reconciliation.records_valid || 0);
  const rejected = Number(reconciliation.records_rejected || 0);
  const duplicate = Number(reconciliation.duplicate_count || 0);
  const malformed = Number(reconciliation.malformed_amount_count || 0);
  const invalidProvider = Number(reconciliation.invalid_provider_count || 0);
  const invalidPolicy = Number(reconciliation.invalid_policy_count || 0);
  const classifiedRejects = duplicate + malformed + invalidProvider + invalidPolicy;
  const other = Math.max(0, rejected - classifiedRejects);
  const passRate = received ? (valid / received) * 100 : 0;
  const rejectionRate = Number(reconciliation.rejection_rate || 0) * 100;
  setText("recon-received", formatNumber(received));
  setText("recon-valid", formatNumber(valid));
  setText("recon-rejected", formatNumber(rejected));
  setText("recon-rejection-rate", formatPercent(rejectionRate));
  const rateElement = byId("recon-rate");
  setText("recon-rate", formatPercent(passRate));
  rateElement.classList.toggle("is-warning", passRate < 98 && passRate >= 85);
  rateElement.classList.toggle("is-danger", passRate < 85);
  const segments = [
    ["valid", "Valid", valid],
    ["malformed", "Malformed amount", malformed],
    ["duplicate", "Duplicate IDs", duplicate],
    ["invalid-provider", "Invalid provider", invalidProvider],
    ["invalid-policy", "Invalid policy", invalidPolicy],
    ["other", "Other rejected", other],
  ];
  const total = Math.max(1, received);
  bar.innerHTML = segments
    .filter((segment) => segment[2] > 0)
    .map((segment) => {
      const width = (segment[2] / total) * 100;
      return `<span class="recon-segment recon-segment--${segment[0]}" style="width:${width.toFixed(4)}%" title="${escapeHtml(segment[1])}: ${formatNumber(segment[2])}"></span>`;
    })
    .join("");
  bar.setAttribute(
    "aria-label",
    `Received ${formatNumber(received)}, valid ${formatNumber(valid)}, rejected ${formatNumber(rejected)} distributed across ${segments.filter((segment) => segment[2] > 0).length} categories`
  );
  legend.innerHTML = segments
    .filter((segment) => segment[2] > 0)
    .map(
      (segment) =>
        `<span class="recon-legend-item"><i class="swatch swatch--${segment[0]}"></i>${escapeHtml(segment[1])} <strong>${formatNumber(segment[2])}</strong></span>`
    )
    .join("");
}

function splitRecommendation(recommendation) {
  const text = String(recommendation || "");
  const fencePattern = /```[a-zA-Z]*\n?([\s\S]*?)```/g;
  const blocks = [];
  let match = fencePattern.exec(text);
  while (match !== null) {
    blocks.push(match[1].trim());
    match = fencePattern.exec(text);
  }
  const note = text
    .replace(fencePattern, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  return {
    command: blocks.length ? blocks.join("\n\n") : text.trim(),
    note: blocks.length ? note : "Deploy the diff, verify reconciliation, then rerun the affected stage.",
  };
}

function renderRecovery(diagnosis) {
  const block = byId("recovery-block");
  const code = byId("recovery-sql");
  const note = byId("recovery-note");
  const copy = byId("copy-recovery");
  if (!diagnosis) {
    block.setAttribute("data-safety", "none");
    code.textContent = "Run the agent to generate a verified recovery path.";
    note.textContent = "No diagnosis is cached for this incident yet.";
    copy.disabled = true;
    state.recoveryCommand = "";
    setText("recovery-title", "Recovery SQL / command");
    return;
  }
  const safety = String(diagnosis.retry_safety || "");
  const manual = safety === "MANUAL_CLEANUP_REQUIRED";
  block.setAttribute("data-safety", manual ? "manual" : "safe");
  setText("recovery-title", manual ? "Manual cleanup SQL" : "Recovery SQL / command");
  const parts = splitRecommendation(diagnosis.retry_recommendation);
  state.recoveryCommand = parts.command;
  code.textContent = parts.command || "No recovery command was produced.";
  note.textContent = parts.note;
  copy.disabled = !parts.command;
}

function renderArtifacts(detail) {
  const diagnosis = detail.diagnosis;
  const diffText = diagnosis ? diagnosis.code_diff : "";
  const planText = detail.query_plan_formatted || "";
  const traceText = detail.stack_trace || "";
  setText("diff-count", String(diffText ? diffText.split("\n").length : 0));
  setText("plan-count", String(planText ? planText.split("\n").length : 0));
  setText("trace-count", String(traceText ? traceText.split("\n").length : 0));
  const diffFileLine = String(diffText || "")
    .split("\n")
    .find((line) => line.startsWith("+++ "));
  const diffFile = diffFileLine ? diffFileLine.replace(/^\+\+\+\s+/, "").replace(/^b\//, "") : detail.notebook_path || "Unified diff";
  setText("diff-file", diffFile);
  setText("plan-file", `${detail.incident.task_key} · formatted plan`);
  setText("trace-file", `${detail.incident.task_key} · traceback`);
  setText("artifact-subtitle", `${detail.notebook_path || "Notebook source"} · ${detail.incident.run_id}`);
  byId("diff-code").innerHTML = renderDiff(diffText);
  byId("plan-code").innerHTML = renderPlan(planText);
  byId("trace-code").innerHTML = renderTrace(traceText);
  setActiveTab(state.activeTab, false);
}

function renderDetail() {
  const detail = state.detail;
  if (!detail) {
    showEmptyTriage();
    return;
  }
  const incident = detail.incident;
  const diagnosis = detail.diagnosis;
  byId("triage-empty").hidden = true;
  byId("triage-loading").hidden = true;
  byId("triage-content").hidden = false;
  const category = diagnosis ? diagnosis.root_cause_category : String(incident.error_class || "OTHER");
  const title = categoryTitles[category] || categoryTitles.OTHER;
  setText("incident-eyebrow", `INCIDENT ${incident.incident_id} · ${scenarioLabels[incident.scenario] || incident.scenario}`);
  setText("triage-title", title);
  setText("triage-subtitle", `${incident.run_id} · ${incident.task_key} · ${formatTimestamp(incident.created_at)}`);
  setText("run-agent-label", diagnosis ? "Re-run Agent" : "Run Agent");
  renderBadges(incident, diagnosis);
  setText("stat-stage", incident.task_key || "—");
  setText("stat-rows", `${formatNumber(incident.row_count_in)} / ${formatNumber(incident.row_count_out)}`);
  setText("stat-rejected", formatNumber(incident.rejected_row_count));
  const skew = Number(incident.top_key_skew_ratio || 0);
  setText("stat-skew", formatPercent(skew * 100, 1));
  setText("stat-write-mode", incident.write_mode || "—");
  setText("stat-duration", formatDuration(incident.duration_sec));
  const rejectedCell = byId("stat-rejected");
  rejectedCell.classList.toggle("is-danger", Number(incident.rejected_row_count || 0) > 0);
  const skewCell = byId("stat-skew");
  skewCell.classList.toggle("is-warning", skew >= 0.65);
  const writeCell = byId("stat-write-mode");
  writeCell.classList.toggle("is-danger", String(incident.write_mode || "").includes("append"));
  const retryCard = byId("retry-card");
  const retryBadge = byId("retry-badge");
  if (diagnosis) {
    const manual = diagnosis.retry_safety === "MANUAL_CLEANUP_REQUIRED";
    retryCard.setAttribute("data-safety", manual ? "manual" : "safe");
    retryBadge.setAttribute("data-safety", manual ? "manual" : "safe");
    retryBadge.textContent = diagnosis.retry_safety.replace(/_/g, " ");
    setText(
      "retry-copy",
      manual
        ? "A non-idempotent append already committed payout rows. Remove the partial batch before any retry and deploy the idempotent writer."
        : "Telemetry and writer mode support an idempotent retry after the remediation diff is deployed and reconciliation is verified."
    );
    setText("retry-model", diagnosis.model_used);
  } else {
    retryCard.setAttribute("data-safety", "none");
    retryBadge.setAttribute("data-safety", "none");
    retryBadge.textContent = "NOT DIAGNOSED";
    setText("retry-copy", "Run the agent to evaluate idempotency, partial writes and the correct retry path.");
    setText("retry-model", "awaiting agent");
  }
  setText("impact-exposure", formatMoney(diagnosis ? diagnosis.payout_exposure_usd : incident.payout_exposure_usd));
  setText(
    "impact-summary",
    diagnosis
      ? diagnosis.summary
      : "No diagnosis is cached. The agent will quantify delayed claims, quarantine volume and committed payout exposure from telemetry."
  );
  setText(
    "impact-business",
    diagnosis
      ? diagnosis.insurance_business_impact
      : "Insurance consequences are translated after the agent reads the query plan, traceback and notebook source."
  );
  renderReconciliation(detail.reconciliation);
  renderArtifacts(detail);
  renderRecovery(diagnosis);
}

async function loadDetail(incidentId) {
  if (!incidentId) {
    showEmptyTriage();
    return;
  }
  state.selectedId = incidentId;
  state.detailRequest += 1;
  const requestId = state.detailRequest;
  renderQueue();
  showTriageLoading();
  byId("triage-panel").setAttribute("aria-busy", "true");
  try {
    const payload = await requestJson(`/api/incidents/${encodeURIComponent(incidentId)}`);
    if (requestId !== state.detailRequest) {
      return;
    }
    state.detail = payload;
    state.activeTab = "diff";
    renderDetail();
  } catch (error) {
    if (requestId === state.detailRequest) {
      showEmptyTriage();
      toast(`Incident telemetry could not be loaded: ${error.message}`, "error");
    }
  } finally {
    if (requestId === state.detailRequest) {
      byId("triage-panel").setAttribute("aria-busy", "false");
    }
  }
}

async function selectIncident(incidentId) {
  if (!incidentId || incidentId === state.selectedId && state.detail && state.detail.incident.incident_id === incidentId) {
    return;
  }
  await loadDetail(incidentId);
}

async function refreshOverview() {
  const overview = await requestJson("/api/overview");
  state.overview = overview;
  renderOverview();
  return overview;
}

async function syncWarehouse() {
  if (state.busy || state.diagnosing) {
    return;
  }
  const button = byId("sync-button");
  button.classList.add("is-loading");
  button.disabled = true;
  setGlobalProgress(true);
  try {
    const results = await Promise.all([requestJson("/api/incidents?refresh=true"), requestJson("/api/overview")]);
    state.overview = results[1];
    renderOverview();
    const nextId = applyIncidentPayload(results[0], state.selectedId);
    if (nextId) {
      await loadDetail(nextId);
    }
    toast("Warehouse cache synchronized. Incident and reconciliation telemetry are current.", "success");
  } catch (error) {
    toast(`Warehouse sync failed: ${error.message}`, "error");
  } finally {
    button.classList.remove("is-loading");
    button.disabled = false;
    setGlobalProgress(false);
  }
}

function setScenarioFeedback(message, tone) {
  const feedback = byId("scenario-feedback");
  feedback.classList.remove("is-busy", "is-error", "is-warning");
  if (tone) {
    feedback.classList.add(`is-${tone}`);
  }
  setText("scenario-feedback-text", message);
}

function setScenarioBusy(busy, activeScenario) {
  state.busy = busy;
  document.querySelectorAll("[data-scenario]").forEach((button) => {
    button.disabled = busy;
    button.classList.toggle("is-loading", Boolean(activeScenario) && button.getAttribute("data-scenario") === activeScenario);
  });
  const syncButton = byId("sync-button");
  const agentButton = byId("run-agent-button");
  if (syncButton) {
    syncButton.disabled = busy;
  }
  if (agentButton) {
    agentButton.disabled = busy || state.diagnosing;
  }
}

async function triggerScenario(scenario) {
  if (state.busy || state.diagnosing) {
    return;
  }
  setScenarioBusy(true, scenario);
  setGlobalProgress(true);
  setScenarioFeedback(`Executing Scenario ${scenario} through Bronze, Silver and Gold…`, "busy");
  try {
    const result = await requestJson("/api/scenarios/trigger", {
      method: "POST",
      body: JSON.stringify({ scenario }),
    });
    const results = await Promise.all([requestJson("/api/incidents"), requestJson("/api/overview")]);
    state.overview = results[1];
    renderOverview();
    const nextId = applyIncidentPayload(results[0], result.incident_id || state.selectedId);
    if (nextId) {
      await loadDetail(nextId);
    }
    const tone = result.status === "FAILED" ? "error" : result.status === "DEGRADED" ? "warning" : "busy";
    setScenarioFeedback(`${scenarioTitles[scenario] || result.status} · ${result.message}`, tone === "busy" ? null : tone);
    toast(`${scenarioTitles[scenario] || scenario}: ${result.message}`, result.status === "FAILED" ? "error" : result.status === "DEGRADED" ? "warning" : "success");
  } catch (error) {
    setScenarioFeedback(`Scenario ${scenario} failed to execute: ${error.message}`, "error");
    toast(`Scenario ${scenario} failed: ${error.message}`, "error");
  } finally {
    setScenarioBusy(false, null);
    setGlobalProgress(false);
  }
}

async function diagnoseSelectedIncident() {
  if (state.diagnosing || state.busy || !state.selectedId) {
    return;
  }
  state.diagnosing = true;
  const button = byId("run-agent-button");
  button.classList.add("is-loading");
  button.disabled = true;
  setGlobalProgress(true);
  setText("run-agent-label", "Analyzing telemetry");
  try {
    const result = await requestJson(`/api/incidents/${encodeURIComponent(state.selectedId)}/diagnose`, { method: "POST" });
    if (state.detail && state.detail.incident.incident_id === state.selectedId) {
      state.detail.diagnosis = result;
      state.detail.incident.diagnosed = true;
      state.detail.incident.retry_safety = result.retry_safety;
      state.detail.incident.root_cause_category = result.root_cause_category;
      state.detail.incident.diagnosis_summary = result.summary;
    }
    const incident = state.incidents.find((item) => item.incident_id === state.selectedId);
    if (incident) {
      incident.diagnosed = true;
      incident.retry_safety = result.retry_safety;
      incident.root_cause_category = result.root_cause_category;
      incident.diagnosis_summary = result.summary;
    }
    renderQueue();
    renderDetail();
    const overview = await requestJson("/api/overview");
    state.overview = overview;
    renderOverview();
    if (result.retry_safety === "MANUAL_CLEANUP_REQUIRED") {
      toast("Agent diagnosis complete. Manual cleanup is required before retry; recovery SQL is ready.", "warning");
    } else {
      toast("Agent diagnosis complete. Unified diff and retry recommendation are ready.", "success");
    }
  } catch (error) {
    toast(`Agent diagnosis failed: ${error.message}`, "error");
  } finally {
    state.diagnosing = false;
    button.classList.remove("is-loading");
    button.disabled = false;
    setText("run-agent-label", state.detail && state.detail.diagnosis ? "Re-run Agent" : "Run Agent");
    setGlobalProgress(false);
  }
}

function activeArtifactText() {
  if (!state.detail) {
    return "";
  }
  if (state.activeTab === "plan") {
    return state.detail.query_plan_formatted || "";
  }
  if (state.activeTab === "trace") {
    return state.detail.stack_trace || "";
  }
  return state.detail.diagnosis ? state.detail.diagnosis.code_diff : "";
}

async function copyToClipboard(value) {
  const text = String(value || "");
  if (!text) {
    throw new Error("Nothing to copy");
  }
  if (navigator.clipboard && window.isSecureContext) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const field = document.createElement("textarea");
  field.value = text;
  field.setAttribute("readonly", "");
  field.style.position = "fixed";
  field.style.opacity = "0";
  document.body.appendChild(field);
  field.select();
  const copied = document.execCommand("copy");
  field.remove();
  if (!copied) {
    throw new Error("Clipboard access is unavailable");
  }
}

async function copyArtifact() {
  try {
    await copyToClipboard(activeArtifactText());
    toast("Artifact copied to clipboard.", "success");
  } catch (error) {
    toast(error.message, "error");
  }
}

async function copyRecovery() {
  try {
    await copyToClipboard(state.recoveryCommand);
    toast("Recovery command copied to clipboard.", "success");
  } catch (error) {
    toast(error.message, "error");
  }
}

function bindEvents() {
  byId("sync-button").addEventListener("click", syncWarehouse);
  byId("run-agent-button").addEventListener("click", diagnoseSelectedIncident);
  byId("artifact-copy").addEventListener("click", copyArtifact);
  byId("copy-recovery").addEventListener("click", copyRecovery);
  byId("scenario-bar").addEventListener("click", (event) => {
    const button = event.target.closest("[data-scenario]");
    if (button) {
      triggerScenario(button.getAttribute("data-scenario"));
    }
  });
  byId("incident-list").addEventListener("click", (event) => {
    const button = event.target.closest("[data-incident-id]");
    if (button) {
      selectIncident(button.getAttribute("data-incident-id"));
    }
  });
  const tabs = document.querySelectorAll("[data-tab]");
  tabs.forEach((button) => {
    button.addEventListener("click", () => setActiveTab(button.getAttribute("data-tab"), false));
    button.addEventListener("keydown", (event) => {
      if (event.key !== "ArrowRight" && event.key !== "ArrowLeft") {
        return;
      }
      event.preventDefault();
      const names = ["diff", "plan", "trace"];
      const current = names.indexOf(button.getAttribute("data-tab"));
      const offset = event.key === "ArrowRight" ? 1 : -1;
      const next = names[(current + offset + names.length) % names.length];
      setActiveTab(next, true);
    });
  });
}

async function boot() {
  bindEvents();
  setGlobalProgress(true);
  try {
    try {
      state.health = await requestJson("/api/health");
      renderHealth();
    } catch (error) {
      toast(`Health check unavailable: ${error.message}`, "warning");
    }
    try {
      await refreshOverview();
    } catch (error) {
      toast(`Overview unavailable: ${error.message}`, "error");
    }
    try {
      const incidentId = await fetchIncidents({});
      if (incidentId) {
        await loadDetail(incidentId);
      }
    } catch (error) {
      toast(`Incident queue unavailable: ${error.message}`, "error");
      showEmptyTriage();
    }
  } finally {
    setGlobalProgress(false);
  }
}

boot();
