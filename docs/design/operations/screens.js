/* Review fixtures only. No model requests or live commands. */
const screen = document.querySelector('#screen');
const state = document.querySelector('#state');
const query = new URLSearchParams(location.search);
screen.value = query.get('screen') || 'controls';
state.value = query.get('state') || 'ready';
const panel = (title, body) => `<section class="panel"><div class="panel-head"><h2>${title}</h2></div><div class="body">${body}</div></section>`;
function render() {
  const parameters = screen.value === 'parameters';
  const status = state.value;
  history.replaceState(null, '', `?screen=${screen.value}&state=${status}`);
  const disabled = ['loading', 'stale', 'uncertain'].includes(status) ? 'disabled' : '';
  let content = `<h1>WH-001 · ${parameters ? 'Parameters' : 'Controls'}</h1><p>${parameters ? 'Inspect model inputs and publish a change for the next service restart.' : 'Propose a change to this running well, then review it before activation.'}</p>`;
  if (status === 'permission') {
    document.querySelector('#workspace').innerHTML = content + panel('Access required', `<p>Your account needs ${parameters ? 'asset management' : 'operating'} permission for this view.</p><a href="../milestone-9/mockups/usability.html?screen=monitoring">Return to Monitoring</a>`);
    return;
  }
  if (status === 'loading') content += '<p role="status">Reading the latest state…</p>';
  if (status === 'stale') content += '<p role="alert">We could not verify these values. Refresh before submitting.</p>';
  if (status === 'error') content += '<p role="alert">The revision changed. Read the latest state and create a new proposal.</p>';
  if (status === 'uncertain') content += '<p role="alert">We did not receive the outcome. Check your proposal before trying another change.</p><button type="button">Check outcome</button>';
  content += `<details class="panel"><summary class="body">How this works</summary><div class="body"><ol><li>Choose one ${parameters ? 'parameter' : 'control'} and explain why you want to change it.</li><li>Save a proposal and check its values.</li><li>${parameters ? 'Publish it. A service restart must adopt the new parameters.' : 'Confirm and activate it. Check the command status and running revision.'}</li></ol><p>Saving a draft does not change the running well. A missing response does not prove that a write failed.</p></div></details>`;
  content += panel(parameters ? 'Parameter values' : 'Installed controls', parameters
    ? '<p>Downstream pressure · psi</p><p>Base default: 250. Running value: 250.</p><p>Permitted values: 50 to 1,000 psi. No database override.</p>'
    : '<p>Choke: 100%. Master and wing valves: open. Pump: running. Shutdown: normal.</p><p>Durable revision: 2. Running revision: 2. Last successful tick: 10:00:01 UTC.</p>');
  content += panel('New proposal', `<form><fieldset ${disabled}><legend>Choose a target</legend><div class="field"><label for="target">${parameters ? 'Parameter' : 'Control'}</label><select id="target"><option>${parameters ? 'Downstream pressure (psi)' : 'Choke opening (%)'}</option></select></div><div class="field"><label for="value">New value ${parameters ? '(psi)' : '(%)'}</label><input id="value" type="number" value="${parameters ? 210 : 80}" min="${parameters ? 50 : 0}" max="${parameters ? 1000 : 100}"></div>${parameters ? '<label><input type="checkbox"> Remove the database override</label><p>This restores environment/default resolution, which may differ from the base default.</p>' : ''}<div class="field"><label for="reason">Reason</label><input id="reason" value="Check response to a small change" maxlength="500" required></div><button class="primary" type="button">Save proposal</button></fieldset></form>`);
  if (['review', 'queued', 'applied', 'stale'].includes(status)) content += panel('Review proposal', `<p>${parameters ? 'Downstream pressure: 210 psi. Expected override: none.' : 'Choke opening: 80%. Expected revision: 2.'}</p><p>Reason: Check response to a small change.</p><p>Expires: 18 Oct 2026, 10:00 UTC.</p><p role="status">${status === 'queued' ? 'Queued. Waiting for the model worker.' : status === 'applied' ? parameters ? 'Published. Existing models need a service restart to adopt this change.' : 'Applied in the database at revision 3. Check the running revision for adoption.' : 'Draft. The running model has not changed.'}</p>${(['review', 'stale'].includes(status) || (status === 'queued' && !parameters)) ? `<label><input type="checkbox" ${disabled}> I checked this well, target and reason</label><p>${status === 'queued' ? 'Cancellation can lose a race with application. Check the returned status.' : `<button type="button" ${disabled}>${parameters ? 'Publish parameter' : 'Activate command'}</button>`} <button type="button">Cancel proposal</button></p>` : ''}`);
  content += panel('Your proposals', status === 'empty' ? '<p>You have no proposals for this well.</p>' : '<article><h3>Check response to a small change</h3><p>17 Oct, 10:00 UTC · Draft</p><button type="button">Open proposal</button></article>');
  document.querySelector('#workspace').innerHTML = content;
}
screen.addEventListener('change', render);
state.addEventListener('change', render);
render();
