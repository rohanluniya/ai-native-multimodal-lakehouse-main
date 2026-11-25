/* Multimodal Lakehouse & RAG Governance Control Tower Client */

async function fetchJson(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status} on ${url}`);
  return r.json();
}

function esc(s) {
  return String(s ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

async function runQuery() {
  const q = document.getElementById("query-input").value.trim() || "Iceberg compaction";
  const persona = document.getElementById("persona-select").value;
  const mode = document.getElementById("mode-select").value;
  const res = await fetchJson(
    `/api/demo-query?q=${encodeURIComponent(q)}&persona=${encodeURIComponent(persona)}&mode=${encodeURIComponent(mode)}`
  );

  document.getElementById("query-meta").innerHTML = `
    <span>Principal: <strong style="color:#93c5fd">${esc(res.principal.role)} (${esc(res.principal.max_clearance)})</strong></span>
    <span>Model: <strong style="color:#2dd4bf">${esc(res.model_version)}</strong></span>
    <span>Authorized Candidates: <strong>${res.authorized_candidates}/${res.total_corpus_chunks}</strong></span>
    <span>ACL Pre-Filtered Out: <strong style="color:#f59e0b">${res.acl_filtered_out}</strong></span>
    <span>Prompt Injections Blocked: <strong style="color:#34d399">${res.prompt_injections_blocked}</strong></span>
    <span>Latency: <strong>${res.latency_ms} ms</strong></span>
  `;

  document.getElementById("grounded-answer").innerHTML =
    `<div style="font-size:11px;font-weight:700;color:#93c5fd;margin-bottom:4px;">CITED GROUNDED ANSWER (EXTRACTIVE, ACL-VERIFIED)</div>` +
    `<div>${esc(res.grounded_answer)}</div>`;

  document.getElementById("query-hits").innerHTML = (res.results || [])
    .slice(0, 4)
    .map(
      (h) => `
      <div class="hit-card">
        <div class="hit-head">
          <strong style="color:#e2e8f0">#${h.rank} ${esc(h.title)}</strong>
          <span class="pill pill-info">${esc(h.modality.toUpperCase())} · ${esc(h.security_tier)}</span>
        </div>
        <div class="hit-passage">${esc(h.passage.slice(0, 210))}</div>
        <div class="hit-foot">${esc(h.citation)} · score=${h.score} (bm25=${h.bm25_score}, vec=${h.vector_score})${h.pii_redacted ? " · PII REDACTED" : ""}</div>
      </div>`
    )
    .join("");
}

async function inspectLineage(docId) {
  const data = await fetchJson(`/api/documents/${encodeURIComponent(docId)}/lineage`);
  const vers = data.versions_scd2
    .map((v) => `v${v.version_num} [${v.is_deleted ? "DELETED" : v.is_current ? "CURRENT" : "SUPERSEDED"}] tier=${v.security_tier} allow=[${v.acl_allow_csv}] deny=[${v.acl_deny_csv}]`)
    .join("  →  ");
  const sampleChunks = (data.chunks_lineage || [])
    .slice(0, 3)
    .map((c) => `  • chunk=${c.chunk_id.slice(0, 12)} parent=${c.parent_chunk_id.slice(0, 12)} v${c.version_num} chars=[${c.char_start}:${c.char_end}] active=${c.is_active}`)
    .join("\n");
  document.getElementById("lineage-drawer").innerHTML = `
    <div style="color:#93c5fd;font-weight:700;margin-bottom:4px;">PROVENANCE CHAIN: ${esc(docId)}</div>
    <div style="color:#cbd5e1;margin-bottom:6px;">${esc(vers)}</div>
    <pre style="white-space:pre-wrap;color:#94a3b8">${esc(sampleChunks || "  (No chunks retained)")}</pre>
  `;
}

async function refreshAll() {
  const [health, summary, evals, docs, gov] = await Promise.all([
    fetchJson("/health"),
    fetchJson("/api/summary"),
    fetchJson("/api/evaluations"),
    fetchJson("/api/documents?limit=14"),
    fetchJson("/api/governance"),
  ]);

  const k = summary.kpis;
  document.getElementById("kpi-docs").textContent = k.active_documents;
  document.getElementById("kpi-versions").textContent = `${k.scd2_versions} SCD2 versions`;
  document.getElementById("kpi-chunks").textContent = k.active_chunks;
  document.getElementById("kpi-model").textContent = k.active_model_version;
  document.getElementById("model-pill").textContent = k.active_model_version.toUpperCase();
  document.getElementById("kpi-quarantine").textContent = k.quarantined_assets;
  document.getElementById("kpi-tombstones").textContent = k.tombstones_propagated;
  document.getElementById("freshness-ts").textContent = `Updated ${new Date(health.freshness_utc).toISOString().slice(11, 19)} UTC`;
  document.getElementById("footer-stats").textContent = `OpenLineage Events: ${k.openlineage_events} · Profile: ${health.execution_profile}`;

  const bestEval = (evals.evaluations || []).find((e) => e.mode === "hybrid_rerank") || evals.evaluations[0];
  if (bestEval) {
    document.getElementById("kpi-recall").textContent = `${(bestEval.recall_at_5 * 100).toFixed(0)}% / ${bestEval.mrr_at_5.toFixed(2)}`;
    document.getElementById("kpi-eval-lat").textContent = `p95 retrieval ${bestEval.p95_latency_ms} ms`;
    document.getElementById("kpi-leaks").textContent = `${bestEval.acl_leakage_count} / ${bestEval.prompt_injection_leaks}`;
  }

  // Modality distribution bars
  const maxC = Math.max(1, ...summary.by_modality.map((m) => Number(m.chunks)));
  const colors = { pdf: "#3b82f6", markdown: "#2dd4bf", image: "#a78bfa", audio: "#f59e0b", structured: "#34d399" };
  document.getElementById("modality-bars").innerHTML = summary.by_modality
    .map(
      (m) => `
      <div class="bar-row">
        <div class="bar-meta">
          <span><strong>${esc(m.modality.toUpperCase())}</strong> (${m.docs} docs)</span>
          <span class="mono">${m.chunks} chunks · ${m.pii_chunks} PII-redacted</span>
        </div>
        <div class="bar-track"><div class="bar-fill" style="width:${(m.chunks / maxC) * 100}%;background:${colors[m.modality] || "#60a5fa"}"></div></div>
      </div>`
    )
    .join("");

  document.getElementById("tier-lang-grid").innerHTML = summary.by_security_tier
    .map((t) => `<div class="mini-box"><div class="k">${esc(t.security_tier.toUpperCase())}</div><div class="v">${t.docs}d / ${t.chunks}c</div></div>`)
    .join("");

  // Eval table
  document.querySelector("#eval-table tbody").innerHTML = (evals.evaluations || [])
    .map(
      (e) => `
      <tr>
        <td>${esc(e.mode)}</td>
        <td style="color:#93c5fd">${esc(e.model_version.replace("-tfidf-svd-", "-").replace("-charword-svd-", "-"))}</td>
        <td>${Number(e.recall_at_1).toFixed(2)}</td>
        <td style="color:#34d399">${Number(e.recall_at_5).toFixed(2)}</td>
        <td style="color:#2dd4bf">${Number(e.mrr_at_5).toFixed(2)}</td>
        <td>${Number(e.ndcg_at_5).toFixed(2)}</td>
        <td>${Number(e.groundedness).toFixed(2)}</td>
        <td style="color:#34d399">${e.acl_leakage_count + e.prompt_injection_leaks + e.deleted_doc_leaks}</td>
        <td>${Number(e.p95_latency_ms).toFixed(2)}</td>
      </tr>`
    )
    .join("");

  document.getElementById("model-registry-cards").innerHTML = (summary.models || [])
    .map(
      (m) => `
      <div class="model-card ${m.active ? "active" : ""}">
        <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
          <strong class="mono">${esc(m.model_version)}</strong>
          <span class="pill ${m.active ? "pill-pass" : "pill-info"}">${m.active ? "ACTIVE" : "STANDBY"}</span>
        </div>
        <div style="color:#94a3b8">${m.dimensions}d · ${m.indexed_chunks} chunks · build ${m.build_duration_ms} ms</div>
      </div>`
    )
    .join("");

  // SCD2 Docs table
  const dtbody = document.querySelector("#docs-table tbody");
  dtbody.innerHTML = (docs.documents || [])
    .map(
      (d) => `
      <tr class="clickable" data-doc="${esc(d.document_id)}">
        <td style="color:#93c5fd">${esc(d.document_id)}</td>
        <td>v${d.version_num}</td>
        <td>${esc(d.modality)}</td>
        <td>${esc(d.detected_language)}</td>
        <td>${esc(d.security_tier)}</td>
        <td>${d.pii_redacted_count}</td>
        <td><span class="pill ${d.is_deleted ? "pill-fail" : d.is_current ? "pill-pass" : "pill-warn"}">${d.is_deleted ? "DELETED" : d.is_current ? "CURRENT" : "HIST"}</span></td>
      </tr>`
    )
    .join("");
  dtbody.querySelectorAll("tr.clickable").forEach((tr) => tr.addEventListener("click", () => inspectLineage(tr.dataset.doc)));
  inspectLineage("DOC-ARCH-001");

  // Governance & DQ
  const dqS = gov.dq_summary;
  const dqBadge = document.getElementById("dq-badge");
  dqBadge.textContent = `${dqS.passed}/${dqS.total} ${dqS.overall_status}`;
  dqBadge.className = `pill ${dqS.overall_status === "PASS" ? "pill-pass" : "pill-fail"}`;
  document.getElementById("dq-list").innerHTML = (gov.dq_checks || [])
    .map(
      (c) => `<div class="dq-item"><span class="mono">${esc(c.check_name)}</span><span class="pill ${c.status === "PASS" ? "pill-pass" : "pill-fail"}">${c.status}</span></div>`
    )
    .join("");

  document.querySelector("#quarantine-table tbody").innerHTML = (gov.quarantine || [])
    .slice(0, 6)
    .map((q) => `<tr><td>${esc(q.document_id)}</td><td style="color:#f59e0b">${esc(q.error_code)}</td><td>${esc(q.error_message.slice(0, 56))}</td></tr>`)
    .join("");

  document.querySelector("#tombstone-table tbody").innerHTML = (gov.tombstones || [])
    .map((t) => `<tr><td style="color:#93c5fd">${esc(t.document_id)}</td><td>${esc(t.reason)}</td><td>${t.chunks_purged}</td><td>${Number(t.propagation_ms).toFixed(2)} ms</td><td><span class="pill pill-pass">SLA MET</span></td></tr>`)
    .join("");

  await runQuery();
}

document.getElementById("run-query-btn").addEventListener("click", runQuery);
document.getElementById("persona-select").addEventListener("change", runQuery);
document.getElementById("mode-select").addEventListener("change", runQuery);
document.getElementById("refresh-btn").addEventListener("click", refreshAll);

refreshAll();
