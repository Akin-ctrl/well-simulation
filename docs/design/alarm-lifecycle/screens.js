const role = document.querySelector("#role"),
  state = document.querySelector("#state"),
  workspace = document.querySelector("#workspace");
function render() {
  const s = state.value,
    operator = role.value === "operator",
    disabled = [
      "pending",
      "conflict",
      "uncertain",
      "stale",
      "error",
      "expired",
    ].includes(s);
  const heading =
    "<h1>Alarm Center</h1><p>Check the condition, record that you have seen it, and follow your operating procedure.</p>";
  if (s === "permission") {
    workspace.innerHTML =
      heading +
      '<section class="panel error"><h2>Access denied</h2><p>Your account no longer has access to this view. Reload after an administrator reviews your access.</p></section>';
    return;
  }
  if (s === "loading") {
    workspace.innerHTML = heading + '<p role="status">Loading alarm state…</p>';
    return;
  }
  workspace.innerHTML =
    heading +
    `<div class="tiles">${[
      ["All active", s === "empty" ? 0 : 4],
      ["Active critical", s === "empty" ? 0 : 1],
      ["Needs acknowledgement", s === "empty" ? 0 : 3],
      ["Current shelves", ["shelved", "shelved_clear"].includes(s) ? 1 : 0],
    ]
      .map(
        ([k, v]) =>
          `<section class="panel"><span>${k}</span><strong>${v}</strong></section>`,
      )
      .join(
        "",
      )}</div><p class="muted">Active totals include acknowledged and shelved conditions. Needs acknowledgement also includes retained events that returned to normal.</p><nav class="tabs" aria-label="Alarm views">${["Needs acknowledgement", "All active", "Shelved rules", "History"].map((t) => `<button type="button">${t}</button>`).join("")}</nav><label>Well<select><option>All wells</option><option>Wellhead 1</option></select></label>`;
  if (["stale", "error"].includes(s))
    workspace.innerHTML +=
      '<p role="alert" class="panel error">Refresh failed. Showing the last successful read. Actions stay unavailable until a new read succeeds.</p><button>Try again</button>';
  if (s === "empty") {
    workspace.innerHTML +=
      '<section class="panel"><h2>No events need acknowledgement</h2><p>Open All active to check conditions that remain active.</p></section>';
    return;
  }
  workspace.innerHTML +=
    s === "shelved_clear"
      ? '<article class="panel"><span class="badge">Shelved rule</span><h2>Wellhead 1 · Tubing pressure</h2><p>No active event. The shelf still applies to new events until 11:30.</p><p>Operator1: instrument inspection. Shelf revision 2.</p></article>'
      : `<article class="panel"><div class="actions"><span class="badge error">CRITICAL</span><span>${s === "returned" ? "Returned to normal" : "Active condition"}</span><span>${s === "acknowledged" ? "Acknowledged by operator1" : "Not acknowledged"}</span>${s === "shelved" ? '<span class="badge">Shelved until 11:30</span>' : ""}${s === "expired" ? '<span class="badge">Shelf deadline reached. Refresh to verify.</span>' : ""}</div><h2>Wellhead 1 · Tubing pressure</h2><dl class="facts"><div><dt>Value when triggered</dt><dd>3200 psi</dd></div><div><dt>Current threshold</dt><dd>&gt; 3000 psi</dd></div><div><dt>Triggered</dt><dd>17 Oct, 10:42</dd></div><div><dt>Well location</dt><dd>North field</dd></div></dl><p class="muted">Open the well for current readings. This value describes the event that opened the alarm.</p><details><summary>Event details and recent actions</summary><p>Event 9007199254740993, 2026-10-17T09:42:00.123456Z.</p><p>Operator1 shelved this rule for instrument inspection. Previous acknowledgement belongs to a different event.</p></details><div class="actions"><a href="#workspace">Open well</a>${operator ? '<button id="review">Review action</button>' : "<p>OPERATIONS can acknowledge events and manage shelves. Your account can inspect their history.</p>"}</div></article>`;
  if (operator) {
    workspace.innerHTML += `<section class="panel"><h2>Review action for Wellhead 1</h2>${s === "conflict" ? '<p role="alert">Another operator changed this state. Your reason remains below. Refresh and review the current state before submitting.</p><button>Review current state</button>' : ""}${s === "uncertain" ? '<p role="alert">The reply did not confirm the outcome. Check this request before trying another action.</p><button id="check">Check result</button>' : ""}<form><fieldset ${disabled ? "disabled" : ""}><legend>Acknowledge this event</legend><p>This records that you have seen the event. Normal readings clear the condition.</p><label>Action<select><option>Acknowledge event</option><option>Shelve rule</option><option>End shelf now</option></select></label><label>Shelf duration (minutes)<input type="number" value="30" min="5" max="480"></label><p class="muted">A shelf applies to this well and rule, including new events, until its deadline. Allowed duration: 5–480 minutes.</p><label>Reason<textarea required maxlength="500">Checking the instrument and current readings.</textarea></label><label><input type="checkbox" required> I checked the well, rule and effect of this action.</label><button type="submit">${s === "pending" ? "Saving action…" : "Acknowledge event"}</button></fieldset></form></section>`;
    workspace
      .querySelector("form")
      .addEventListener("submit", (e) => e.preventDefault());
    const form = workspace.querySelector("form"),
      pick = form.querySelector("select"),
      duration = form.querySelector("input[type=number]").closest("label"),
      submit = form.querySelector("button");
    if (["shelved", "shelved_clear"].includes(s)) pick.value = "End shelf now";
    if (s === "acknowledged") pick.value = "Shelve rule";
    const chosen = () => {
      form.querySelector("p").textContent =
        pick.value === "Acknowledge event"
          ? "This records that you have seen the event. Normal readings clear the condition."
          : pick.value === "Shelve rule"
            ? "This temporarily removes the well and rule from Needs acknowledgement, including new events."
            : "This ends the shelf now. Unacknowledged events return to Needs acknowledgement.";
      duration.hidden = pick.value !== "Shelve rule";
      form.querySelector("legend").textContent = pick.value;
      submit.textContent = s === "pending" ? "Saving action…" : pick.value;
    };
    pick.addEventListener("change", chosen);
    chosen();
  }
  workspace.insertAdjacentHTML(
    "beforeend",
    '<details class="panel"><summary>How acknowledgement and shelving work</summary><p>Acknowledgement records recognition. Three consecutive normal readings clear the seeded condition. Shelving temporarily removes this well and rule from Needs acknowledgement. All active totals still count it, and Shelved rules shows its deadline and reason.</p></details>',
  );
}
role.addEventListener("change", render);
state.addEventListener("change", render);
render();
