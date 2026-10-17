/* Static design fixtures, never imported by the application. */
const screen = document.querySelector("#screen");
const state = document.querySelector("#state");
const params = new URLSearchParams(location.search);
screen.value = params.get("screen") || "fleet";
state.value = params.get("state") || "ready";
const panel = (title, body) =>
  `<section class="panel"><div class="panel-head"><h2>${title}</h2></div><div class="body">${body}</div></section>`;
const help = (title, body) =>
  `<details class="panel"><summary class="body">${title}</summary><div class="body">${body}</div></details>`;
const field = (id, label, value, type = "text") =>
  `<div class="field"><label for="${id}">${label}</label><input id="${id}" type="${type}" value="${value}"></div>`;
const table = (heads, rows) =>
  `<div class="scroll" role="region" aria-label="Values" tabindex="0"><table class="wide"><thead><tr>${heads.map((h) => `<th scope="col">${h}</th>`).join("")}</tr></thead><tbody>${rows.map((r) => `<tr>${r.map((c) => `<td>${c}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
const purpose = {
  account: [
    "Create an account",
    "Administrators create accounts for dashboard readers.",
  ],
  missing: ["Page not found", "Check the address or return to the dashboard."],
  fleet: [
    "Wellhead overview",
    "Choose a well to check its readings, forecast its behaviour, or try a change.",
  ],
  monitoring: [
    "WH-001",
    "Monitoring · Check recent readings and alarms before starting an analysis.",
  ],
  forecast: [
    "WH-001",
    "Forecast · See how this well may behave if its current controls stay the same.",
  ],
  scenario: [
    "WH-001",
    "Scenarios · Compare a proposed change with the same well running unchanged.",
  ],
  runs: [
    "WH-001",
    "Saved runs · Open a previous result and inspect the inputs that produced it.",
  ],
  alarms: [
    "Alarm Center",
    "Check the most severe alarms first, then open the affected well for current readings.",
  ],
  analytics: [
    "Analytics",
    "Compare recent trends across the fleet. Open a well for its individual history.",
  ],
  guide: [
    "Guide",
    "Learn the workflow, then return here when a term or result needs an explanation.",
  ],
  login: ["Sign in", "Use the account your administrator gave you."],
};
function render() {
  const s = screen.value,
    st = state.value,
    analysis = ["forecast", "scenario"].includes(s);
  history.replaceState(null, "", `?screen=${s}&state=${st}`);
  let body = "";
  if (s === "fleet")
    body =
      help(
        "New here? Start with one well",
        '<ol><li>Open a well and check its readings and data age.</li><li>Run a forecast to keep its controls unchanged.</li><li>Try a scenario to compare a change, then reopen it from Saved runs.</li></ol><a href="?screen=guide">Read the guide</a>',
      ) +
      panel(
        "Your wells",
        st === "empty"
          ? "<p>No wells configured.</p>"
          : table(
              ["Well", "Pressure (PSI)", "Flow (bbl/d)", "Readings", "Alarms"],
              [
                [
                  '<a href="?screen=monitoring">WH-001</a>',
                  "2,150",
                  "820",
                  "4 seconds ago",
                  "0",
                ],
                ["WH-002", "1,920", "645", "4 seconds ago", "2"],
              ],
            ),
      );
  if (s === "monitoring")
    body =
      panel(
        "Current parameters",
        '<div class="grid"><div><h3>Pressures</h3><p>Tubing pressure: 2,150 PSI</p><p>Casing pressure: 950 PSI</p></div><div><h3>Production</h3><p>Flow: 820 bbl/d</p><p>Water cut: 12%</p></div></div>',
      ) +
      panel("Active alarms", "<p>No active alarms for this well.</p>") +
      help(
        "How to read this page",
        "<p>Check the data age before comparing values. A missing reading means unavailable; a zero is a measured value.</p>",
      ) +
      help(
        "Model diagnostics",
        "<p>Model running. Last calculation: 10:00 UTC. Tick and recovery details appear here.</p>",
      );
  if (analysis) {
    const isScenario = s === "scenario";
    body =
      help(
        isScenario ? "Try your first scenario" : "How forecasts work",
        isScenario
          ? "<ol><li>Keep the 30-minute duration.</li><li>Set the choke to 80% at minute 5.</li><li>Run the scenario, then compare the dashed baseline and solid scenario line.</li></ol><p>The example may raise or lower a value, depending on the starting state. The running well keeps its controls.</p>"
          : "<p>The model copies the current well state and holds the controls constant. Each run saves a separate result.</p>",
      ) +
      `<div class="analysis">${panel(isScenario ? "New scenario inputs" : "Forecast inputs", `${isScenario ? field("name", "Scenario name", "Choke adjustment") : ""}${field("horizon", "Duration (minutes)", "30", "number")}<p class="muted">How far ahead to calculate, from 5 to 120 minutes.</p>${field("sample", "Save a point every (seconds)", "60", "number")}<p class="muted">The interval between chart points. It must divide the duration.</p>${isScenario ? `<fieldset class="step"><legend>Change 1</legend>${field("offset", "At minute", "5", "number")}<p class="muted">Count from the start of this run.</p><div class="field"><label for="control">Control</label><select id="control"><option>Choke position</option></select></div>${field("value", "Target (%)", "80", "number")}</fieldset>` : ""}<button class="primary" ${st === "loading" || st === "permission" ? "disabled" : ""}>${st === "loading" ? "Calculating…" : isScenario ? "Run scenario" : "Run forecast"}</button><p class="muted">A run saves a new result. Editing this draft does not change the result beside it.</p>`)}${panel(st === "empty" ? "Your result will appear here" : "Saved result", st === "empty" ? "<p>Choose inputs and run the model, or open a saved result.</p>" : '<p>Choke adjustment · Created 17 October, 10:00</p><p class="muted">These are the inputs from this saved run. Your current draft may differ.</p><p>Tubing pressure (PSI), elapsed minutes</p><svg class="chart" viewBox="0 0 640 260" role="img" aria-label="Illustrative comparison of pressure"><path d="M55 75L615 102" stroke="#59677d" stroke-width="3" stroke-dasharray="7 4" fill="none"/><path d="M55 75L150 80L245 116L340 141L615 162" stroke="#553afe" stroke-width="3" fill="none"/><text x="55" y="220">0 min</text><text x="540" y="220">30 min</text></svg><p>Dashed: unchanged controls. Solid: proposed change.</p><p>Confidence: low. The model has no field calibration.</p>')}</div>`;
  }
  if (s === "runs")
    body =
      panel(
        "Saved runs",
        st === "empty"
          ? "<p>No saved runs yet. Start with a forecast or scenario.</p>"
          : table(
              ["Result", "Created", "Duration", "Action"],
              [
                [
                  "Choke adjustment",
                  "17 Oct, 10:00",
                  "30 minutes",
                  '<a href="?screen=scenario">Open scenario</a>',
                ],
              ],
            ),
      ) +
      help(
        "About saved results",
        "<p>Each result keeps its original inputs. Search filters this page of results. Run a new calculation when you want a different starting state.</p>",
      );
  if (s === "alarms")
    body =
      help(
        "How alarms open and clear",
        "<p>An alarm opens after three consecutive readings cross a threshold. Three normal readings clear it. The value below is the value that opened the alarm.</p>",
      ) +
      panel(
        "Active alarms",
        st === "empty"
          ? "<p>No active alarms.</p>"
          : table(
              [
                "Severity",
                "Well",
                "Parameter",
                "Value when triggered",
                "Threshold",
              ],
              [
                [
                  "Critical",
                  '<a href="?screen=monitoring">WH-002</a>',
                  "Tubing pressure",
                  "3,200 PSI",
                  "> 3,000 PSI",
                ],
              ],
            ),
      );
  if (s === "analytics")
    body =
      help(
        "What these charts show",
        "<p>Each point averages available readings across wells over two minutes. A fleet average can hide a change at one well. Check each chart’s timestamps.</p>",
      ) +
      panel(
        "Alarms triggered in the last 24 hours",
        "<dl><dt>Critical</dt><dd>2</dd><dt>Warning</dt><dd>5</dd></dl>",
      ) +
      panel(
        "Pressure trends",
        st === "empty"
          ? "<p>No readings in this period.</p>"
          : '<p>Two-minute averages across wells. Pressure in PSI.</p><svg class="chart" viewBox="0 0 640 260" role="img" aria-label="Illustrative fleet pressure trend"><path d="M55 75L250 105L400 90L615 120" stroke="#023bb5" stroke-width="3" fill="none"/></svg>',
      );
  if (s === "guide")
    body =
      panel(
        "Start with one well",
        '<ol><li><a href="?screen=fleet">Choose a well from Overview</a> and check its data age.</li><li>Open Forecast and run with the default inputs.</li><li>Open Scenarios and try a choke change at minute 5.</li><li>Compare tubing pressure and flow against the unchanged baseline.</li><li>Open Saved runs to revisit the calculation.</li></ol>',
      ) +
      help(
        "Terms used in the dashboard",
        "<dl><dt>Baseline</dt><dd>The copied well with its original controls.</dd><dt>Water cut</dt><dd>The fraction of liquid production that is water.</dd><dt>Confidence</dt><dd>A model quality indicator, not a probability.</dd></dl>",
      );
  if (s === "login")
    body = panel(
      "Your account",
      `<form>${field("username", "Email or username", "", "text")}${field("password", "Password", "", "password")}<button type="button">Show password</button><p>Ask your administrator if you need an account.</p><button class="primary" type="button">Sign in</button></form>`,
    );
  if (s === "account")
    body =
      st === "permission"
        ? panel(
            "Ask your administrator",
            '<p>Your account cannot create users. Ask an administrator for an account.</p><a href="?screen=login">Sign in</a>',
          )
        : panel(
            "New dashboard reader",
            `<form>${field("username", "Username", "")}${field("email", "Email", "", "email")}${field("password", "Password", "", "password")}${field("confirm", "Confirm password", "", "password")}<button type="button" class="primary">Create account</button><p>This account starts with the USER role.</p></form>`,
          );
  if (s === "missing")
    body = '<a href="?screen=fleet">Back to the dashboard</a>';
  let notice = "";
  if (st === "error")
    notice = analysis
      ? '<div class="notice" role="alert"><strong>The response did not arrive.</strong><p>Your draft remains here. The server may have saved the result.</p><a href="?screen=runs">Check saved runs before submitting again</a></div>'
      : s === "login"
        ? '<div class="notice" role="alert"><strong>We could not sign you in.</strong><p>Check your connection and try again.</p></div>'
        : '<div class="notice" role="alert"><strong>Could not refresh this view.</strong><p>Your last successful data remains visible. Check its timestamps.</p><button>Try again</button></div>';
  if (st === "stale")
    notice =
      '<div class="notice" role="status"><strong>Last checked 2 minutes ago.</strong><p>Check the timestamps before relying on these values.</p></div>';
  if (st === "permission")
    notice =
      '<div class="notice" role="alert"><strong>Access denied.</strong><p>Ask an administrator to review your access.</p></div>';
  if (st === "loading" && !analysis)
    notice = '<p role="status">Loading the requested view…</p>';
  if (st === "invalid")
    notice = `<div class="notice error" role="alert"><h2>Check your inputs</h2><a href="#${["login", "account"].includes(s) ? "username" : "horizon"}">${["login", "account"].includes(s) ? "Enter a username." : "Enter a duration from 5 to 120 minutes."}</a></div>`;
  document.querySelector("#workspace").innerHTML =
    `<div class="heading"><div><p class="eyebrow">Well Simulation</p><h1>${purpose[s][0]}</h1><p class="muted">${purpose[s][1]}</p></div></div>${["monitoring", "forecast", "scenario", "runs"].includes(s) ? '<nav class="tabs" aria-label="Well views"><a href="?screen=monitoring">Monitoring</a><a href="?screen=forecast">Forecast</a><a href="?screen=scenario">Scenarios</a><a href="?screen=runs">Saved runs</a></nav>' : ""}${notice}${st === "permission" && !analysis && s !== "account" ? "" : body}`;
}
screen.addEventListener("change", render);
state.addEventListener("change", render);
render();
