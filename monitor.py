"""
Monitoring page for the queue.

GET  /monitor          -> single self-contained HTML page (no external
                          assets), polls the JSON endpoint every 2 seconds.
GET  /monitor/data     -> JSON snapshot of configuration, known sessions
                          (in queue order) and unknown-API sessions.
POST /monitor/shutdown -> toggle the per-power-cycle auto power-off switch.
POST /monitor/timeout  -> set the proxy-to-target read timeout (0 = none).
"""

import time

__all__ = ["HTML_PAGE", "build_data"]


def build_data(queue, unknown_tracker, config: dict, state: dict) -> dict:
    now = time.monotonic()
    manager_at = state.get("queue_manager_at")
    return {
        "config": {
            **config,
            "server_powered_on": state.get("is_powered_on"),
            "server_healthy": state.get("is_healthy"),
            "power_managed": state.get("manage_power_with_proxy"),
            "queue_manager_age": round(now - manager_at, 1) if manager_at else None,
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
         background: #111; color: #ddd; margin: 2rem auto; padding: 0 1rem; }
  h1 { font-size: 1.2rem; margin-bottom: 1rem; }
  h2 { font-size: 1rem; margin-top: 1.6rem; }
  .tablewrap { overflow-x: auto; }
  table { border-collapse: collapse; width: 100%; font-size: .82rem; }
  th, td { border: 1px solid #333; padding: .35rem .6rem; text-align: left; }
  th.nw, td.nw { white-space: nowrap; }
  td.wrap { overflow-wrap: anywhere; word-break: break-word; }
  th { background: #1c1c1c; color: #aaa; }
  tr:nth-child(even) td { background: #161616; }
  .kv { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
        gap: .2rem 1.2rem; font-size: .82rem; margin: .5rem 0 1rem; }
  .kv b { color: #999; font-weight: normal; }
  .status { font-weight: bold; }
  .busy { color: #6cf; } .queued { color: #fc6; } .idle { color: #9c9; }
  .retry { color: #f77; } .waiting { color: #fa6; }
  .shared-spot { color: #c9f; font-weight: bold; }
  .muted { color: #666; font-weight: normal; }
  button.release { font-family: inherit; font-size: .72rem; background: #232323;
                   color: #fc6; border: 1px solid #665c33; border-radius: 3px;
                   padding: .05rem .4rem; cursor: pointer; vertical-align: middle; }
  button.release:hover { background: #fc6; color: #111; }
  button.release:disabled { opacity: .5; cursor: default; }
  button.toggle, button.apply { font-family: inherit; font-size: .72rem;
                   background: #232323; color: #9c9; border: 1px solid #335c33;
                   border-radius: 3px; padding: .05rem .4rem; cursor: pointer;
                   vertical-align: middle; }
  button.toggle:hover, button.apply:hover { background: #9c9; color: #111; }
  button.toggle:disabled, button.apply:disabled { opacity: .5; cursor: default; }
  input.timeout-input { font-family: inherit; font-size: .72rem; background: #232323;
                        color: #ddd; border: 1px solid #444; border-radius: 3px;
                        padding: .05rem .3rem; width: 5.5rem; vertical-align: middle; }
  #updated { color: #666; font-size: .8rem; font-weight: normal; }
</style>
</head>
<body>
<h1>IPMI Proxy Monitor <span id="updated"></span></h1>
<div id="config" class="kv"></div>
<h2>Sessions (queue order)</h2>
<div class="tablewrap">
<table id="sessions">
  <thead><tr>
    <th>#</th><th>session</th><th>client</th><th>api</th><th>status</th>
    <th>client status</th>
    <th>spot</th><th>releases in</th><th>in-flight</th><th>waiting</th>
    <th>queue pos</th><th>last path</th><th>client ip</th>
  </tr></thead>
  <tbody></tbody>
</table>
</div>
<h2>Unknown API sessions</h2>
<div class="tablewrap">
<table id="unknown">
  <thead><tr>
    <th>client</th><th>method</th><th>target url</th><th>requests</th>
    <th>last activity (s ago)</th>
  </tr></thead>
  <tbody></tbody>
</table>
</div>
<script>
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? '').replace(/[&<>"']/g,
  (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

function render(d) {
  $('updated').textContent = 'updated ' + new Date().toLocaleTimeString();
  $('config').innerHTML = Object.entries(d.config).map(([k, v]) => {
    if (k === 'shutdown_enabled') {
      const on = v === true || v === 'true';
      return '<div><b>auto power-off this cycle:</b> <span class="status '
        + (on ? 'idle' : 'retry') + '">' + (on ? 'on' : 'off') + '</span>'
        + ' <span class="muted">after ' + esc(d.config.idle_timeout ?? 'unknown')
        + 's idle; resets to SHUTDOWN_ENABLED on the next power-on</span> '
        + '<button class="toggle" id="shutdown-toggle" data-on="' + (on ? 'on' : 'off') + '" '
        + 'title="Toggle automatic shutdown for the current power cycle">'
        + (on ? 'off' : 'on') + '</button></div>';
    }
    if (k === 'target_read_timeout') {
      const num = Number(v);
      const none = Number.isFinite(num) && num === 0;
      return '<div><b>target read timeout:</b> ' + esc(none ? 'none' : v + 's')
        + ' <span class="muted">max silence from the target; 0 = no timeout</span> '
        + '<input class="timeout-input" id="read-timeout" type="number" min="0" step="1" '
        + 'value="' + (none ? 0 : num) + '"> '
        + '<button class="apply" id="timeout-apply" '
        + 'title="Apply the target read timeout in seconds (0 = no timeout)">apply</button></div>';
    }
    return '<div><b>' + esc(k) + ':</b> ' + esc(v) + '</div>';
  }).join('');

  const rows = d.sessions.map((s) => '<tr>' +
    '<td class="nw">' + s.position + '</td>' +
    '<td class="wrap">' + esc(s.session) + '</td>' +
    '<td class="nw">' + esc(s.client) + '</td>' +
    '<td class="nw">' + esc(s.api) + '</td>' +
    '<td class="status nw ' + s.status + '">' + esc(s.status) +
      (s.detail ? ' <span class="muted">' + esc(s.detail) + '</span>' : '') + '</td>' +
    '<td class="nw ' + (s.client_status === 'waiting' ? 'waiting' : '') + '">'
      + esc(s.client_status || '-') +
      (s.client_status_detail
      ? ' <span class="muted">' + esc(s.client_status_detail) + '</span>' : '') +
      (s.client_status_age !== null && s.client_status_age !== undefined
      ? ' <span class="muted">' + s.client_status_age + 's</span>' : '') + '</td>' +
    '<td class="nw">' + (s.spot === 'shared' ? '<span class="shared-spot">shared</span>' : s.spot)
      + (s.spot === 'held'
      ? ' <button class="release" data-client="' + esc(s.client) +
        '" data-session="' + esc(s.session) +
        '" title="Release this spot now (before the idle timeout)">release</button>'
      : '') + '</td>' +
    '<td class="nw">' + (s.spot_releases_in === null ? '-' : s.spot_releases_in + 's') + '</td>' +
    '<td class="nw">' + s.inflight + '</td>' +
    '<td class="nw">' + s.waiting + '</td>' +
    '<td class="nw">' + (s.queue_position === null ? '-' : s.queue_position) + '</td>' +
    '<td class="wrap">' + esc(s.last_path) + '</td>' +
    '<td class="nw">' + esc(s.client_ip) + '</td>' +
    '</tr>').join('');
  $('sessions').querySelector('tbody').innerHTML =
    rows || '<tr><td colspan="13" class="muted">no sessions</td></tr>';

  const urows = d.unknown.map((u) => '<tr>' +
    '<td class="wrap">' + esc(u.id) + '</td>' +
    '<td class="nw">' + esc(u.method) + '</td>' +
    '<td class="wrap">' + esc(u.target_url) + '</td>' +
    '<td class="nw">' + u.requests + '</td>' +
    '<td class="nw">' + u.last_activity_seconds_ago + '</td>' +
    '</tr>').join('');
  $('unknown').querySelector('tbody').innerHTML =
    urows || '<tr><td colspan="5" class="muted">none</td></tr>';

  document.querySelectorAll('button.release').forEach((b) => {
    b.onclick = async () => {
      b.disabled = true;
      try {
        await fetch('/monitor/release', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ client: b.dataset.client, session: b.dataset.session }),
        });
      } finally {
        refresh();
      }
    };
  });

  const st = $('shutdown-toggle');
  if (st) {
    st.onclick = async () => {
      st.disabled = true;
      try {
        await fetch('/monitor/shutdown', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ enabled: st.dataset.on === 'off' }),
        });
      } finally {
        refresh();
      }
    };
  }

  const ta = $('timeout-apply');
  if (ta) {
    ta.onclick = async () => {
      const val = parseInt($('read-timeout').value, 10);
      if (!Number.isInteger(val) || val < 0) return;
      ta.disabled = true;
      try {
        await fetch('/monitor/timeout', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ read_timeout: val }),
        });
      } finally {
        refresh();
      }
    };
  }
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
