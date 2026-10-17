"""Monitoring page + data snapshot (v2, M1-M6).

GET  /monitor          -> single self-contained HTML page (no external
                           assets), polls the JSON endpoint every 2 seconds.
GET  /monitor/data     -> JSON snapshot: platform config, the server and
                           endpoint tables, known sessions (per endpoint,
                           queue order), unknown/passthrough activity, and
                           the recent power-event audit.
POST /monitor/release  -> manually release a session's spot.
POST /monitor/shutdown -> toggle the per-power-cycle auto power-off switch.
POST /monitor/timeout  -> set the proxy-to-target read timeout (0 = none).
POST /monitor/power    -> manually power a server on/off (monitor v2; a
                           manual on through the proxy takes ownership, D12).

The ``config`` block keeps the v1 (single-server) shape so the ported parity
tests are unchanged; the ``servers`` / ``endpoints`` / ``power_events`` arrays
carry the multi-server-aware detail. No authentication (D7: trusted network,
documented).
"""

import time

from relay.power.base import PowerState

__all__ = ["HTML_PAGE", "build_data"]


def _server_row(server, now: float) -> dict:
    """One row of the servers table (monitor v2)."""
    idle_countdown = None
    if server.owned and server.shutdown_enabled_now and not server.has_activity():
        remaining = server.config.idle_timeout - (now - server.last_activity)
        if remaining >= 0:
            idle_countdown = round(remaining, 1)
    next_off = server.next_scheduled_off()
    return {
        "name": server.name,
        "type": server.config.type,
        "power_state": server.display_state,
        "owned": server.owned,
        "idle_countdown": idle_countdown,
        "next_scheduled_off": next_off.isoformat(timespec="seconds") if next_off else None,
        "shutdown_enabled": server.shutdown_enabled_now,
        "idle_timeout": server.config.idle_timeout,
        "adopt_on_traffic": server.config.adopt_on_traffic,
        "service_url": server.config.service_url,
        "cycle_id": server.cycle_id,
    }


def _endpoint_row(endpoint) -> dict:
    """One row of the endpoints table (monitor v2)."""
    q = endpoint.queue
    return {
        "name": endpoint.name,
        "server": endpoint.server.name,
        "path_prefix": endpoint.config.path_prefix,
        "ready": q.ready,
        "ready_since": endpoint.ready_since,
        "wait_policy": endpoint.config.wait_policy,
        "routing": endpoint.config.routing,
        "concurrency": endpoint.config.concurrency,
        "queue_depth": len(q.queue),
        "free_slots": max(0, q.max_spots - len(q.spots)),
        "queue_timeout": endpoint.config.queue_timeout,
    }


async def build_data(app) -> dict:
    """The /monitor/data snapshot (``app`` is the RelayApp, duck-typed)."""
    now = time.monotonic()
    sessions = []
    for endpoint in app.endpoints.values():
        for row in endpoint.queue.snapshot(now):
            row = dict(row)
            row["endpoint"] = endpoint.name
            sessions.append(row)
    if app.endpoints:
        manager_at = max((ep.last_manager_tick or 0.0) for ep in app.endpoints.values())
    else:
        manager_at = None
    primary = app.catch_all or next(iter(app.endpoints.values()), None)
    server = next(iter(app.servers.values()), None)
    config = {
        # v1 (parity) surface — the primary server/endpoint, unchanged so the
        # single-server monitor and the ported tests keep working.
        "concurrent_sessions": primary.config.concurrency if primary else None,
        "concurrent_session_requests": (
            primary.config.session.per_session_requests if primary else None
        ),
        "request_mode": primary.config.session.request_mode if primary else None,
        "immediate_idle_release": (
            primary.config.session.immediate_idle_release if primary else None
        ),
        "unknown_path_policy": app.config.proxy.unknown_path_policy,
        "session_expiry": primary.config.session.expiry if primary else None,
        "client_busy_window": primary.config.session.busy_window if primary else None,
        "client_status_poll": (
            app.config.clients[0].status.poll_interval if app.config.clients else None
        ),
        "queue_timeout": primary.config.queue_timeout if primary else None,
        "target_server_url": server.config.service_url if server else None,
        "idle_timeout": server.config.idle_timeout if server else None,
        "shutdown_enabled": server.shutdown_enabled_now if server else None,
        "target_read_timeout": app.target_read_timeout,
        "server_powered_on": (
            None
            if server is None or server.power_state is PowerState.UNKNOWN
            else server.power_state is PowerState.ON
        ),
        "server_healthy": (
            None if primary is None or primary.last_check_at is None else primary.queue.ready
        ),
        "power_managed": server.owned if server else None,
        "queue_manager_age": round(now - manager_at, 1) if manager_at else None,
        "active_sessions": sum(len(ep.queue.spots) for ep in app.endpoints.values()),
        "queued_requests": sum(len(ep.queue.queue) for ep in app.endpoints.values()),
    }
    return {
        "config": config,
        "servers": [_server_row(s, now) for s in app.servers.values()],
        "endpoints": [_endpoint_row(ep) for ep in app.endpoints.values()],
        "sessions": sessions,
        "unknown": app.unknown_tracker.snapshot(now),
        "power_events": await app.store.recent_power_events(),
    }


HTML_PAGE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Relay Monitor</title>
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
  .on { color: #9c9; } .off { color: #f77; } .unknown { color: #999; }
  .shared-spot { color: #c9f; font-weight: bold; }
  .muted { color: #666; font-weight: normal; }
  button.release, button.toggle, button.apply, button.pwr { font-family: inherit;
                    font-size: .72rem; background: #232323; border-radius: 3px;
                    padding: .05rem .4rem; cursor: pointer; vertical-align: middle; }
  button.release { color: #fc6; border: 1px solid #665c33; }
  button.release:hover { background: #fc6; color: #111; }
  button.toggle, button.apply { color: #9c9; border: 1px solid #335c33; }
  button.toggle:hover, button.apply:hover { background: #9c9; color: #111; }
  button.pwr.on { color: #9c9; border: 1px solid #335c33; }
  button.pwr.on:hover { background: #9c9; color: #111; }
  button.pwr.off { color: #f77; border: 1px solid #5c3333; }
  button.pwr.off:hover { background: #f77; color: #111; }
  button.release:disabled, button.toggle:disabled, button.apply:disabled,
  button.pwr:disabled { opacity: .5; cursor: default; }
  input.timeout-input { font-family: inherit; font-size: .72rem; background: #232323;
                        color: #ddd; border: 1px solid #444; border-radius: 3px;
                        padding: .05rem .3rem; width: 5.5rem; vertical-align: middle; }
  #updated { color: #666; font-size: .8rem; font-weight: normal; }
</style>
</head>
<body>
<h1>Relay Monitor <span id="updated"></span></h1>
<div id="config" class="kv"></div>

<h2>Servers</h2>
<div class="tablewrap">
<table id="servers">
  <thead><tr>
    <th>server</th><th>type</th><th>power</th><th>owned</th>
    <th>idle in</th><th>next off</th><th>auto-off</th><th>power</th>
  </tr></thead>
  <tbody></tbody>
</table>
</div>

<h2>Endpoints</h2>
<div class="tablewrap">
<table id="endpoints">
  <thead><tr>
    <th>endpoint</th><th>server</th><th>prefix</th><th>ready</th>
    <th>queue</th><th>free</th><th>wait</th><th>routing</th><th>slots</th>
  </tr></thead>
  <tbody></tbody>
</table>
</div>

<h2>Sessions (queue order)</h2>
<div class="tablewrap">
<table id="sessions">
  <thead><tr>
    <th>#</th><th>endpoint</th><th>session</th><th>client</th><th>status</th>
    <th>client status</th>
    <th>spot</th><th>releases in</th><th>in-flight</th><th>waiting</th>
    <th>queue pos</th><th>last path</th><th>client ip</th>
  </tr></thead>
  <tbody></tbody>
</table>
</div>

<h2>Unknown / passthrough sessions</h2>
<div class="tablewrap">
<table id="unknown">
  <thead><tr>
    <th>client</th><th>method</th><th>target url</th><th>requests</th>
    <th>last activity (s ago)</th>
  </tr></thead>
  <tbody></tbody>
</table>
</div>

<h2>Recent power events</h2>
<div class="tablewrap">
<table id="events">
  <thead><tr>
    <th>time</th><th>server</th><th>action</th><th>reason</th><th>by</th><th>result</th>
  </tr></thead>
  <tbody></tbody>
</table>
</div>

<script>
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? '').replace(/[&<>"']/g,
  (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const clock = (ts) => ts ? new Date(ts * 1000).toLocaleString() : '-';

async function post(url, body) {
  await fetch(url, { method: 'POST',
    headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
}

function render(d) {
  $('updated').textContent = 'updated ' + new Date().toLocaleTimeString();
  $('config').innerHTML = Object.entries(d.config).map(([k, v]) => {
    if (k === 'shutdown_enabled') {
      const on = v === true || v === 'true';
      return '<div><b>auto power-off this cycle (primary):</b> <span class="status '
        + (on ? 'on' : 'off') + '">' + (on ? 'on' : 'off') + '</span>'
        + ' <span class="muted">after ' + esc(d.config.idle_timeout ?? 'unknown')
        + 's idle; resets to the config default on the next power-on</span> '
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

  const srvRows = (d.servers || []).map((s) => {
    const on = s.power_state === 'on';
    const cls = on ? 'on' : (s.power_state === 'off' ? 'off' : 'unknown');
    const autoOn = s.shutdown_enabled;
    return '<tr>'
      + '<td class="nw">' + esc(s.name) + '</td>'
      + '<td class="nw">' + esc(s.type) + '</td>'
      + '<td class="nw status ' + cls + '">' + esc(s.power_state) + '</td>'
      + '<td class="nw">' + (s.owned ? 'yes' : 'no') + '</td>'
      + '<td class="nw">' + (s.idle_countdown === null ? '-' : s.idle_countdown + 's') + '</td>'
      + '<td class="nw">' + (s.next_scheduled_off ? esc(s.next_scheduled_off) : '-') + '</td>'
      + '<td class="nw"><span class="' + (autoOn ? 'on' : 'off') + '">'
      + (autoOn ? 'on' : 'off') + '</span> '
      + '<button class="toggle" data-server="' + esc(s.name) + '" data-on="'
      + (autoOn ? 'on' : 'off') + '">' + (autoOn ? 'off' : 'on') + '</button></td>'
      + '<td class="nw"><button class="pwr on" data-server="' + esc(s.name)
      + '" data-action="on">on</button> '
      + '<button class="pwr off" data-server="' + esc(s.name)
      + '" data-action="off">off</button></td>'
      + '</tr>';
  }).join('');
  $('servers').querySelector('tbody').innerHTML =
    srvRows || '<tr><td colspan="8" class="muted">no servers</td></tr>';

  const epRows = (d.endpoints || []).map((e) => '<tr>'
    + '<td class="nw">' + esc(e.name) + '</td>'
    + '<td class="nw">' + esc(e.server) + '</td>'
    + '<td class="nw">' + esc(e.path_prefix) + '</td>'
    + '<td class="nw status ' + (e.ready ? 'on' : 'off') + '">'
    + (e.ready ? 'ready' : 'not ready') + '</td>'
    + '<td class="nw">' + e.queue_depth + '</td>'
    + '<td class="nw">' + e.free_slots + '</td>'
    + '<td class="nw">' + esc(e.wait_policy) + '</td>'
    + '<td class="nw">' + esc(e.routing) + '</td>'
    + '<td class="nw">' + e.concurrency + '</td>'
    + '</tr>').join('');
  $('endpoints').querySelector('tbody').innerHTML =
    epRows || '<tr><td colspan="9" class="muted">no endpoints</td></tr>';

  const rows = (d.sessions || []).map((s) => '<tr>'
    + '<td class="nw">' + s.position + '</td>'
    + '<td class="nw">' + esc(s.endpoint) + '</td>'
    + '<td class="wrap">' + esc(s.session) + '</td>'
    + '<td class="nw">' + esc(s.client) + '</td>'
    + '<td class="status nw ' + s.status + '">' + esc(s.status)
    + (s.detail ? ' <span class="muted">' + esc(s.detail) + '</span>' : '') + '</td>'
    + '<td class="nw ' + (s.client_status === 'waiting' ? 'waiting' : '') + '">'
    + esc(s.client_status || '-')
    + (s.client_status_detail
    ? ' <span class="muted">' + esc(s.client_status_detail) + '</span>' : '')
    + (s.client_status_age !== null && s.client_status_age !== undefined
      ? ' <span class="muted">' + s.client_status_age + 's</span>' : '') + '</td>'
    + '<td class="nw">' + (s.spot === 'shared' ? '<span class="shared-spot">shared</span>' : s.spot)
    + (s.spot === 'held' ? ' <button class="release" data-client="' + esc(s.client)
      + '" data-session="' + esc(s.session)
      + '" title="Release this spot now (before the idle timeout)">release</button>' : '') + '</td>'
    + '<td class="nw">' + (s.spot_releases_in === null ? '-' : s.spot_releases_in + 's') + '</td>'
    + '<td class="nw">' + s.inflight + '</td>'
    + '<td class="nw">' + s.waiting + '</td>'
    + '<td class="nw">' + (s.queue_position === null ? '-' : s.queue_position) + '</td>'
    + '<td class="wrap">' + esc(s.last_path) + '</td>'
    + '<td class="nw">' + esc(s.client_ip) + '</td>'
    + '</tr>').join('');
  $('sessions').querySelector('tbody').innerHTML =
    rows || '<tr><td colspan="13" class="muted">no sessions</td></tr>';

  const urows = (d.unknown || []).map((u) => '<tr>'
    + '<td class="wrap">' + esc(u.id) + '</td>'
    + '<td class="nw">' + esc(u.method) + '</td>'
    + '<td class="wrap">' + esc(u.target_url) + '</td>'
    + '<td class="nw">' + u.requests + '</td>'
    + '<td class="nw">' + u.last_activity_seconds_ago + '</td>'
    + '</tr>').join('');
  $('unknown').querySelector('tbody').innerHTML =
    urows || '<tr><td colspan="5" class="muted">none</td></tr>';

  const erows = (d.power_events || []).map((e) => '<tr>'
    + '<td class="nw">' + clock(e.ts) + '</td>'
    + '<td class="nw">' + esc(e.server) + '</td>'
    + '<td class="nw">' + esc(e.action) + '</td>'
    + '<td class="nw">' + esc(e.reason) + '</td>'
    + '<td class="nw">' + esc(e.initiated_by) + '</td>'
    + '<td class="nw status ' + (e.success ? 'on' : 'off') + '">'
    + (e.success ? 'ok' : 'fail') + '</td>'
    + '</tr>').join('');
  $('events').querySelector('tbody').innerHTML =
    erows || '<tr><td colspan="6" class="muted">no events</td></tr>';

  document.querySelectorAll('button.release').forEach((b) => {
    b.onclick = async () => {
      b.disabled = true;
      try {
        await post('/monitor/release', { client: b.dataset.client, session: b.dataset.session });
      } finally {
        refresh();
      }
    };
  });

  const st = $('shutdown-toggle');
  if (st) {
    st.onclick = async () => {
      st.disabled = true;
      try { await post('/monitor/shutdown', { enabled: st.dataset.on === 'off' }); }
      finally { refresh(); }
    };
  }

  document.querySelectorAll('button.toggle[data-server]').forEach((b) => {
    b.onclick = async () => {
      b.disabled = true;
      try {
        await post('/monitor/shutdown', {
          server: b.dataset.server, enabled: b.dataset.on === 'off',
        });
      } finally {
        refresh();
      }
    };
  });

  document.querySelectorAll('button.pwr').forEach((b) => {
    b.onclick = async () => {
      b.disabled = true;
      try { await post('/monitor/power', { server: b.dataset.server, action: b.dataset.action }); }
      finally { refresh(); }
    };
  });

  const ta = $('timeout-apply');
  if (ta) {
    ta.onclick = async () => {
      const val = parseInt($('read-timeout').value, 10);
      if (!Number.isInteger(val) || val < 0) return;
      ta.disabled = true;
      try { await post('/monitor/timeout', { read_timeout: val }); }
      finally { refresh(); }
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
