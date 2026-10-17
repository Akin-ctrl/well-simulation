# Service metrics

This optional stack helps maintainers investigate delayed telemetry, model lag
and service failures. Use the well dashboard for wells, controls and analysis.
The [design](../docs/design/observability.md) explains the queries and failure states.

## Start and sign in

1. Set `GRAFANA_ADMIN_PASSWORD` in the ignored root `.env` file to a unique random
   password of at least 32 characters. The example leaves it empty. Keep the
   value private; Grafana refuses to start with an absent or shorter value.
2. Start the normal stack, then the optional services:

   ```bash
   docker compose up -d --wait
   docker compose --profile observability up -d --wait prometheus grafana
   ```

3. Open [Grafana](http://localhost:3010), sign in as `admin` with that password,
   and open **Well simulation → Service health**. The same dashboard appears
   on the home page. Grafana accounts are separate from well dashboard accounts.
4. Allow at least two scrapes (about 30 seconds) for rate charts to acquire data.
   The default view covers 30 minutes and refreshes every 15 seconds.

Both ports bind to host loopback. [Prometheus](http://localhost:9090) has no login;
use it locally to inspect **Status → Target health** and queries. This profile
assumes a trusted local machine and Compose network. Remote access needs its
own authenticated TLS deployment design.

Prometheus scrapes internal service addresses. The well dashboard proxy also
exposes the API's service metrics at `/api/metrics` without a login. Grafana's
login does not protect that route. The other service metrics have no host route
in the default Compose setup.

The password initializes a fresh Grafana database. Changing `.env` afterward
will not change an existing Grafana account. Change an existing password in
Grafana's account preferences. For a locked-out local administrator, follow the
[Grafana CLI reset procedure](https://grafana.com/docs/grafana/latest/cli/#reset-admin-password),
using the CLI inside the `grafana` container. Avoid putting a password in shell
history or sharing command output.

## Read the panels

- **Reachable** means the metrics endpoint answered the latest scrape.
- **Ready** means the service can do its work. The API's database probe must be
  no more than 30 seconds old. A future probe timestamp also stays unknown.
- **Down** means a scrape failed. Its corresponding readiness becomes **Unknown**.
- **Time since last commit** grows from the last complete historian batch. It
  keeps increasing during a dependency failure while ingestion stays reachable.
- Charts leave gaps for missing samples. No data does not establish zero failures.
  New outcome series may have no data until an event occurs.
- Expand **Automation** to inspect scheduled jobs and control outcomes. The
  general model projection counter includes scheduled forecasts, so do not add
  it to the scheduled-job counter as if they counted separate work.

Choose a time range to investigate an incident. Open a panel menu and select
**Inspect → Data** for a table. Read descriptions through each panel's information
control. The dashboard comes from versioned JSON; edit that file and review the
queries before changing the shared layout. Provisioning checks it every 30 seconds.

## Stop, restart and recover

```bash
docker compose --profile observability stop grafana prometheus
docker compose --profile observability up -d --wait prometheus grafana
```

The named volumes retain metrics, accounts and preferences. Do not use `down -v`
to stop these tools; that can remove the historian volume as well. Prometheus
retains seven days or 512MB of blocks, whichever limit applies first. Its WAL
(write-ahead log) and active data need extra space; this is not a hard disk quota.

If a card says Down, inspect the matching service's health and logs. If every
panel shows a query error, inspect Prometheus and the Grafana data source before
changing the monitored services. A Not ready card points to that service's
`/ready` response and dependency logs. Do not restart or alter well controls to
repair a metrics display.

## Check configuration changes

From the repository root:

```bash
docker compose --profile observability run --rm --no-deps --entrypoint /bin/promtool prometheus check config /etc/prometheus/prometheus.yml
docker compose --profile observability run --rm --no-deps --workdir /etc/prometheus --entrypoint /bin/promtool prometheus test rules rules.test.yml
```

The tests cover failed and absent scrapes, service failures, stale and future
probe times, missing commits, elapsed commit age and scheduler scan age. CI runs
the same checks. Restart Prometheus after changing its configuration or rules.
