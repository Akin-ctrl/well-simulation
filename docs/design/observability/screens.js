const services = ['Model', 'Gateway', 'Ingestion', 'API database'];
const descriptions = {
  normal: 'All four endpoints answered. Each service reported that it can do its work.',
  warming: 'The services are starting. Rates need at least two samples.',
  service_down: 'The gateway endpoint did not answer. Its readiness and current charts are unknown.',
  dependency_down: 'Ingestion can answer metrics while it waits for the database. Its last commit gets older.',
  stale: 'The API readiness probe has not completed in the last 30 seconds.',
  empty: 'No samples exist in this time range. Choose a recent range or check the targets.',
  error: 'Grafana could not query Prometheus. Check the connection before using these panels.',
  permission: 'Sign in to view service metrics. This login belongs to Grafana.',
};
function draw() {
  const state = document.querySelector('#state').value;
  const absent = ['empty', 'error', 'permission'].includes(state);
  const reach = services.map((name, i) => absent ? 'Unknown' : state === 'service_down' && i === 1 ? 'Down' : 'Reachable');
  const ready = services.map((name, i) => absent || (state === 'service_down' && i === 1) || (state === 'stale' && i === 3) ? 'Unknown' : state === 'warming' || (state === 'dependency_down' && i === 2) ? 'Not ready' : 'Ready');
  const card = (name, value) => `<section class="panel"><h3>${name}</h3><p class="value ${['Ready','Reachable'].includes(value) ? 'good' : value === 'Down' ? 'bad' : value === 'Not ready' ? 'warn' : 'muted'}">${value}</p></section>`;
  const chart = (name, unit, detail, line) => `<section class="panel"><h3>${name}</h3><p class="muted">${detail}</p>${absent || state === 'warming' ? `<p class="empty">${state === 'error' ? 'Query failed' : state === 'permission' ? 'Sign in required' : 'No samples'}</p>` : `<svg viewBox="0 0 500 150" role="img" aria-label="Illustrative ${name.toLowerCase()} chart in ${unit}"><path d="M25 10V125H490" fill="none" stroke="#788598"/><path d="${line}" fill="none" stroke="#023bb5" stroke-width="3"/><text x="32" y="23" fill="#586477" font-size="14">${unit}</text></svg>`}</section>`;
  document.querySelector('#workspace').innerHTML = `<h1>Service health</h1><p class="muted">Reachability checks the metrics endpoint. Readiness checks whether the service can do its work. Missing samples stay unknown.</p><div class="toolbar"><label>Time range <select aria-label="Time range"><option>Last 30 minutes</option><option>Last 6 hours</option><option>Last 7 days</option></select></label><button type="button">Refresh panels</button><span>Refresh every 15 seconds</span></div><p role="status">${descriptions[state]}</p><h2>Can we reach each service?</h2><div class="cards">${services.map((name,i)=>card(name,reach[i])).join('')}</div><h2>Can each service do its work?</h2><div class="cards">${services.map((name,i)=>card(name,ready[i])).join('')}</div><h2>Pipeline activity</h2><div class="charts">${chart('Committed readings', 'readings/s', 'Complete reading batches committed to the historian.', 'M25 90L100 80L180 82L260 75L340 77L420 71L490 76')}${chart('Time since last commit', 'seconds', 'Age increases after the last successful batch. An absent timestamp stays unknown.', state === 'dependency_down' ? 'M25 110L490 35' : 'M25 110L100 100L105 115L180 100L185 115L260 100L265 115L340 100L345 115L420 100L425 115L490 105')}</div><h2>Model and API</h2><div class="charts">${chart('Simulation lag', 'seconds', 'Delay before the model’s tick pass.', 'M25 115L160 112L240 90L270 113L490 115')}${chart('API request duration', 'seconds', 'The 95th percentile over five minutes. Sparse traffic may have no samples.', 'M25 100L140 90L250 94L340 85L490 95')}</div><details class="panel" style="margin-top:20px"><summary>Automation</summary><p>Scheduled job and control outcomes use separate charts. Check the job state in the well dashboard when a scheduled attempt fails. A scheduler scan older than 90 seconds needs attention.</p></details><details class="panel"><summary>How to read missing data</summary><p>Empty event series can mean that no event has occurred. A stopped target or query error can also leave a gap. Check reachability and the time range first. Open a panel’s Inspect menu to read its values as a table.</p></details>`;
}
document.querySelector('#state').addEventListener('change', draw);
draw();
