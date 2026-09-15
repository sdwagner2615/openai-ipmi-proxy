"""
Monitoring page for the queue.

GET /monitor       -> single self-contained HTML page (no external assets),
                      polls the JSON endpoint every 2 seconds.
GET /monitor/data  -> JSON snapshot of configuration, known sessions (in
                      queue order) and unknown-API sessions.
"""

import time

__all__ = ["build_data", "HTML_PAGE"]


def build_data(queue, unknown_tracker, config: dict, state: dict) -> dict:
    now = time.monotonic()
    return {
        "config": {
            **config,
            "server_powered_on": state.get("is_powered_on"),
            "server_healthy": state.get("is_healthy"),
            "active_sessions": len(queue.spots),
            "queued_requests": len(queue.queue),
        },
        "sessions": queue.snapshot(now),
        "unknown": unknown_tracker.snapshot(now),
    }


HTML_PAGE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>IPMI Proxy Monitor</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  body { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
         background: #111; color: #ddd; margin: 2rem auto; max-width: 1100px;
         padding: 0 1rem; }
  h1 { font-size: 1.2rem; margin-bottom: 1rem; }
  h2 { font-size: 1rem; margin-top: 1.6rem; }
  table { border-collapse: collapse; width: 100%; font-size: .82rem; }
  th, td { border: 1px solid #333; padding: .35rem .6rem; text-align: left;
           white-space: nowrap; }
  th { background: #1c1c1c; color: #aaa; }
  tr:nth-child(even) td { background: #161616; }
  .kv { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
        gap: .2rem 1.2rem; font-size: .82rem; margin: .5rem 0 1rem; }
  .kv b { color: #999; font-weight: normal; }
  .status { font-weight: bold; }
  .busy { color: #6cf; } .queued { color: #fc6; } .idle { color: #9c9; }
  .retry { color: #f77; }
  .muted { color: #666; font-weight: normal; }
  #updated { color: #666; font-size: .8rem; font-weight: normal; }
</style>
</head>
<body>
<h1>IPMI Proxy Monitor <span id="updated"></span></h1>
<div id="config" class="kv"></div>
<h2>Sessions (queue order)</h2>
<table id="sessions">
  <thead><tr>
    <th>#</th><th>session</th><th>client</th><th>api</th><th>status</th>
    <th>spot</th><th>releases in</th><th>in-flight</th><th>waiting</th>
    <th>queue pos</th><th>last path</th><th>client ip</th>
  </tr></thead>
  <tbody></tbody>
</table>
<h2>Unknown API sessions</h2>
<table id="unknown">
  <thead><tr>
    <th>client</th><th>method</th><th>target url</th><th>requests</th>
    <th>last activity (s ago)</th>
  </tr></thead>
  <tbody></tbody>
</table>
<script>
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? '').replace(/[&<>"']/g,
  (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

function render(d) {
  $('updated').textContent = 'updated ' + new Date().toLocaleTimeString();
  $('config').innerHTML = Object.entries(d.config).map(([k, v]) =>
    '<div><b>' + esc(k) + ':</b> ' + esc(v) + '</div>').join('');

  const rows = d.sessions.map((s) => '<tr>' +
    '<td>' + s.position + '</td>' +
    '<td>' + esc(s.session) + '</td>' +
    '<td>' + esc(s.client) + '</td>' +
    '<td>' + esc(s.api) + '</td>' +
    '<td class="status ' + s.status + '">' + esc(s.status) +
      (s.detail ? ' <span class="muted">' + esc(s.detail) + '</span>' : '') + '</td>' +
    '<td>' + s.spot + '</td>' +
    '<td>' + (s.spot_releases_in === null ? '-' : s.spot_releases_in + 's') + '</td>' +
    '<td>' + s.inflight + '</td>' +
    '<td>' + s.waiting + '</td>' +
    '<td>' + (s.queue_position === null ? '-' : s.queue_position) + '</td>' +
    '<td>' + esc(s.last_path) + '</td>' +
    '<td>' + esc(s.client_ip) + '</td>' +
    '</tr>').join('');
  $('sessions').querySelector('tbody').innerHTML =
    rows || '<tr><td colspan="12" class="muted">no sessions</td></tr>';

  const urows = d.unknown.map((u) => '<tr>' +
    '<td>' + esc(u.id) + '</td>' +
    '<td>' + esc(u.method) + '</td>' +
    '<td>' + esc(u.target_url) + '</td>' +
    '<td>' + u.requests + '</td>' +
    '<td>' + u.last_activity_seconds_ago + '</td>' +
    '</tr>').join('');
  $('unknown').querySelector('tbody').innerHTML =
    urows || '<tr><td colspan="5" class="muted">none</td></tr>';
}

async function refresh() {
  try {
    const r = await fetch('/monitor/data');
    const d = await r.json();
    d.config = Object.fromEntries(
      Object.entries(d.config).map(([k, v]) => [k, v === null ? 'unknown' : v]));
    render(d);
  } catch (e) { /* page retries on the next tick */ }
}
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"""
