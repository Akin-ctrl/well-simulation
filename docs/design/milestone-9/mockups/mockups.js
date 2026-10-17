/* Design fixtures only. This gallery is never bundled into the application. */
const screenControl = document.querySelector("#screen");
const stateControl = document.querySelector("#state");
const names = {
  fleet: "Wellhead overview",
  monitoring: "Monitoring",
  forecast: "Forecast",
  scenario: "Scenarios",
  runs: "Saved runs",
};
const params = new URLSearchParams(location.search);
screenControl.value = Object.hasOwn(names, params.get("screen"))
  ? params.get("screen")
  : "fleet";
stateControl.value = [
  "ready",
  "loading",
  "empty",
  "stale",
  "error",
  "permission",
  "invalid",
].includes(params.get("state"))
  ? params.get("state")
  : "ready";
const panel = (title, body) =>
  `<section class="panel"><div class="panel-head"><h2>${title}</h2></div><div class="body">${body}</div></section>`;
const field = (id, label, value, type = "text", invalid = false) =>
  `<div class="field"><label for="${id}">${label}</label><input id="${id}" type="${type}" value="${value}" ${invalid ? `aria-invalid="true" aria-describedby="${id}-error"` : ""}>${invalid ? `<p id="${id}-error" class="field-error">${id === "horizon" ? "Enter a horizon from 5 to 120 minutes." : "Enter a target between 0 and 100%."}</p>` : ""}</div>`;
const table = (caption, heads, rows) =>
  `<div class="scroll" role="region" aria-label="${caption}" tabindex="0"><table class="wide"><caption>${caption}</caption><thead><tr>${heads.map((h) => `<th scope="col">${h}</th>`).join("")}</tr></thead><tbody>${rows.map((row) => `<tr>${row.map((c) => `<td>${c}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
function chart(mode = false) {
  const scenario = mode === true;
  const history = mode === "history";
  return `<figure><figcaption>Tubing pressure (PSI), 10:00–10:30 UTC</figcaption><svg class="chart" viewBox="0 0 640 260" role="img" aria-label="Tubing pressure trends slightly down; ${scenario ? "the scenario falls below the unchanged baseline after minute five" : history ? "the measured samples form this history" : "the forecast follows the copied operating state"}."><g fill="none" stroke="#dce1e8"><path d="M55 30H615M55 95H615M55 160H615M55 225H615"/></g><g fill="#586477" font-size="14"><text x="0" y="35">2,500</text><text x="0" y="100">2,000</text><text x="0" y="165">1,500</text><text x="55" y="250">10:00</text><text x="300" y="250">10:15</text><text x="570" y="250">10:30</text></g><path d="M55 75L150 80L245 84L340 88L435 93L530 98L615 102" stroke="${scenario ? "#59677d" : "#553afe"}" stroke-width="3" stroke-dasharray="${scenario ? "7 4" : ""}" fill="none"/>${scenario ? '<path d="M55 75L150 80L245 116L340 141L435 152L530 158L615 162" stroke="#553afe" stroke-width="3" fill="none"/><path d="M150 30V225" stroke="#586477" stroke-dasharray="3 3"/><text x="158" y="24" fill="#586477" font-size="14">Change at 5 min</text>' : ""}</svg><div class="legend"><span>${scenario ? "┄ Unchanged baseline" : history ? "━ Tubing pressure" : "━ Forecast"}</span>${scenario ? "<span>━ Scenario</span>" : ""}</div></figure><details><summary>View plotted values as a table</summary>${table(
    "Illustrative samples, UTC",
    [
      "Time",
      scenario
        ? "Baseline (PSI)"
        : history
          ? "Pressure (PSI)"
          : "Forecast (PSI)",
      ...(scenario ? ["Scenario (PSI)"] : []),
    ],
    [
      ["10:00", "2,150", ...(scenario ? ["2,150"] : [])],
      ["10:30", "1,940", ...(scenario ? ["1,480"] : [])],
    ],
  )}</details>`;
}
function notice(screen, state) {
  if (state === "stale")
    return `<div class="notice" role="status"><strong>${screen === "runs" ? "History has not been refreshed" : screen === "forecast" || screen === "scenario" ? "Current model check overdue" : "Readings are stale"}</strong><p>${screen === "runs" ? "Saved results retain their original timestamps. Refresh the list to find newer runs." : screen === "forecast" || screen === "scenario" ? "The saved result remains unchanged. A new run requires a current model state." : "Last reading 46 seconds ago. Values remain visible with their original timestamps."}</p><button>Refresh status</button></div>`;
  if (state === "error")
    return `<div class="notice" role="alert"><strong>${screen === "forecast" || screen === "scenario" ? "The response did not arrive" : "Could not refresh this view"}</strong><p>${screen === "forecast" || screen === "scenario" ? "Your draft is retained. The run might have been saved." : "Retained values have not been verified. Check their timestamps."}</p>${screen === "forecast" || screen === "scenario" ? '<a href="?screen=runs">Check saved runs before submitting again</a>' : "<button>Try again</button>"}</div>`;
  if (state === "permission")
    return `<div class="notice" role="alert"><strong>${screen === "forecast" || screen === "scenario" ? "Analysis is unavailable for this account" : "Access denied"}</strong><p>${screen === "forecast" || screen === "scenario" ? "You can still read results allowed by your account. Ask an administrator for analysis access." : "Your account cannot read this resource. Ask an administrator to review your access."}</p><a href="?screen=fleet">Return to overview</a></div>`;
  return "";
}
function render() {
  const screen = screenControl.value,
    state = stateControl.value,
    analysis = ["forecast", "scenario"].includes(screen);
  const url = new URL(location);
  url.searchParams.set("screen", screen);
  url.searchParams.set("state", state);
  history.replaceState(null, "", url);
  let body = "";
  if (state === "permission" && !analysis) body = "";
  else if (state === "loading" && !analysis)
    body = panel(
      "Loading " + names[screen].toLowerCase(),
      '<p role="status">Reading the latest data…</p><div aria-hidden="true"><div class="skeleton"></div><div class="skeleton"></div><div class="skeleton"></div></div>',
    );
  else if (screen === "fleet")
    body = `<div class="metrics"><div class="metric"><p class="muted">Configured wellheads</p><div class="number">${state === "empty" ? "0" : "4"}</div></div><div class="metric"><p class="muted">Tracked signals</p><div class="number">${state === "empty" ? "0" : "18"}</div></div><div class="metric"><p class="muted">Active alarms</p><div class="number">${state === "empty" ? "0" : "2"}</div></div></div><div class="filter">${field("search", "Search wellheads", "")}<button>Needs attention</button></div>${panel(
      "Wellhead snapshot",
      state === "empty"
        ? '<div class="empty"><h3>No wellheads configured</h3><p>Add wellheads through the existing configuration process.</p></div>'
        : table(
            "Latest readings and model status",
            [
              "Wellhead",
              "Pressure (PSI)",
              "Oil rate (bbl/d)",
              "Data age",
              "Model",
              "Abnormal",
            ],
            [
              [
                '<a href="?screen=monitoring">WH-001</a>',
                "2,150",
                "820",
                state === "stale"
                  ? "46 seconds · stale"
                  : "4 seconds · current",
                "Running",
                "0",
              ],
              [
                '<a href="?screen=monitoring">WH-002</a>',
                "1,920",
                "645",
                "8 seconds · current",
                "Running",
                "2",
              ],
              [
                "WH-003",
                "Unavailable",
                "Unavailable",
                "No readings",
                "Unavailable",
                "Unavailable",
              ],
            ],
          ),
    )}`;
  else if (screen === "monitoring")
    body = `<div class="grid">${panel("Model status", '<span class="badge ok">Running</span><p class="muted">Successful process time: 10:00:00 UTC</p><p>Model version: synthetic reference well</p>')}${panel("Installed controls", '<p>Choke: 70%</p><p>Pump: Running · Revision 12</p><p class="muted">Read-only installed state.</p>')}</div>${panel("Current parameters", state === "empty" ? '<div class="empty"><h3>No readings received</h3><p>Model status and installed controls are shown independently.</p></div>' : '<div class="grid"><div><h3>Tubing pressure</h3><p class="number">2,150 PSI</p></div><div><h3>Oil flow rate</h3><p class="number">820 bbl/d</p></div></div>')}${panel("Pressure history", state === "empty" ? '<p class="empty">No samples in this time range.</p>' : chart("history"))}`;
  else if (analysis) {
    const scenario = screen === "scenario";
    body = `<div class="analysis">${panel(scenario ? "New scenario inputs" : "Forecast inputs", `<p class="muted">Start from a copy of the current model state.</p>${state === "invalid" ? `<div class="notice error" tabindex="-1"><h3>Check your inputs</h3><a href="#${scenario ? "target" : "horizon"}">${scenario ? "Target must be between 0 and 100%." : "Horizon must be from 5 to 120 minutes."}</a></div>` : ""}<fieldset ${state === "permission" ? "disabled" : ""} style="border:0;padding:0;min-width:0">${scenario ? field("name", "Scenario name", "Choke adjustment") : ""}${field("horizon", "Horizon (minutes)", state === "invalid" && !scenario ? "1" : "30", "number", state === "invalid" && !scenario)}${field("sample", "Sample every (seconds)", "60", "number")}<p class="muted">5–120 minutes. Samples every 5–300 seconds.</p>${scenario ? `<div class="step"><h3>Step 1</h3>${field("offset", "At minute", "5", "number")}<div class="field"><label for="control">Control</label><select id="control"><option>Choke position</option></select></div>${field("target", "Target (%)", state === "invalid" ? "101" : "80", "number", state === "invalid")}</div><button type="button">Add change</button><p class="muted">Use ordered times. Up to 16 changes.</p>` : ""}<button class="primary" ${state === "loading" ? 'aria-disabled="true"' : ""}>${state === "loading" ? "Calculating…" : scenario ? "Run scenario" : "Run forecast"}</button></fieldset><p class="muted" role="status">${state === "loading" ? "Calculating and saving the result. Your draft is retained." : "Results are saved with their original inputs."}</p>`)}<div>${panel(state === "empty" ? "No result selected" : scenario ? "Choke adjustment" : "Saved forecast", state === "empty" ? '<div class="empty"><p>Run an analysis or reopen a saved result.</p><a href="?screen=runs">Browse saved runs</a></div>' : `<span class="badge">Saved result · 10:00 UTC</span><p class="muted">30 minutes · Samples every 60 seconds</p><div class="field"><label for="signal">Signal</label><select id="signal"><option>Tubing pressure (PSI)</option><option>Oil flow rate (bbl/d)</option></select></div>${chart(scenario)}<div class="summary"><strong>Confidence: moderate</strong><p>Heuristic model assessment. No sampled critical exceedance.</p></div><details><summary>Original run inputs</summary><p>Horizon 30 minutes. Sample interval 60 seconds.${scenario ? " Choke 80% at minute 5." : ""}</p></details>`)}${state === "loading" ? '<p role="status">Previous saved result remains visible while the new run is calculated.</p>' : ""}</div></div>`;
  } else
    body = `<div class="filter"><div class="field"><label for="kind">Run type</label><select id="kind"><option>Scenarios</option><option>Forecasts</option></select></div>${field("run-search", "Search this page", "")}<button>Refresh history</button></div>${panel(
      "Saved scenarios",
      state === "empty"
        ? '<div class="empty"><h3>No saved scenarios</h3><p>Run a scenario to add its result here.</p><a href="?screen=scenario&state=empty">Create a scenario</a></div>'
        : table(
            "Saved results, newest first",
            ["Name", "Created (UTC)", "Horizon", "Sample interval", "Result"],
            [
              [
                "Choke adjustment",
                "16 Oct 2026, 10:00",
                "30 min",
                "60 sec",
                '<a href="?screen=scenario">Open result</a>',
              ],
              [
                "Baseline check",
                "16 Oct 2026, 09:40",
                "15 min",
                "60 sec",
                '<a href="?screen=scenario">Open result</a>',
              ],
            ],
          ),
    )}<p class="muted">Showing 1–2 of 2</p><button disabled>Previous</button> <button disabled>Next</button>`;
  document.querySelector("main").innerHTML =
    `<div class="heading"><div><p class="eyebrow">${screen === "fleet" ? "Production workspace" : "Wellhead workspace"}</p><h1>${screen === "fleet" ? names[screen] : "WH-001"}</h1><p class="muted">${screen === "fleet" ? "Telemetry, alarms and model health in one view." : "North field · Production well"}</p></div><span class="badge ${state === "stale" ? "warning" : "ok"}">${state === "stale" ? "Check overdue" : "Model running"}</span></div>${screen === "fleet" ? "" : `<nav class="tabs" aria-label="Wellhead views">${["monitoring", "forecast", "scenario", "runs"].map((key) => `<a href="?screen=${key}" ${screen === key ? 'aria-current="page"' : ""}>${names[key]}</a>`).join("")}</nav>`}${state === "invalid" && !analysis ? '<p class="notice">Invalid-input state applies to the scenario editor. This screen keeps its normal layout; read failures use the Error state.</p>' : ""}${notice(screen, state)}${body}`;
}
screenControl.addEventListener("change", render);
stateControl.addEventListener("change", render);
render();
