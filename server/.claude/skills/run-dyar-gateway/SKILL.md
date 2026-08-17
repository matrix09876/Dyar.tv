---
name: run-dyar-gateway
description: Build, launch, smoke-test, and screenshot the Dyar Connect gateway (server/). Use when asked to run, start, boot, serve, smoke-test, or screenshot the Dyar delivery gateway, its dashboard, kiosk (Executive Brain), driver page, or public customer track page.
---

# Run: Dyar Connect gateway

Node HTTP+WebSocket realtime gateway (no framework, only dep `ws`). Serves a REST/WS
API plus four browser pages: **dashboard** (ops room, PIN-gated), **kiosk** (Executive
Brain screen, PIN-gated), **driver** (courier app, PIN-gated), **track** (public customer
tracking + سارة the customer-service agent). Entry point: `src/server.js`.

It is driven by **`.claude/skills/run-dyar-gateway/driver.mjs`** — a self-contained
harness that boots the server on its own port, runs the API-contract smoke tests, and
screenshots the pages (injecting the PIN to get past the gate on dashboard/kiosk).

> All paths below are relative to the unit dir **`server/`**. `cd` there first.

## Prerequisites

The only runtime dep is `ws` (already in `package.json`). The driver additionally needs
`playwright-core`; Chromium is pre-installed in this container at `/opt/pw-browsers`, so
skip the browser download:

```bash
cd server
npm ci                                                  # installs `ws`
PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 npm i --no-save playwright-core
```

## Run (agent path) — the driver

One command boots the server, runs the API smoke, and writes 3 screenshots. It kills the
server it started, so it leaves nothing running:

```bash
cd server
node .claude/skills/run-dyar-gateway/driver.mjs
```

Expected tail (11 checks, all ✓):

```
🔒 فحص عقد الـAPI:
  ✓ GET /api/health = 200 ويكشف وسم البناء [v=...]
  ✓ GET /api/brain/summary بلا رمز = 401
  ✓ اجتياز المسار محجوب
  ✓ عقل العملاء (سارة) يجيب من قاعدة المعرفة [نغطّي البعنة...]
📸 لقطات الصفحات → ./_shots:
  ✓ لقطة track.html بلا أخطاء JS
  ✓ لقطة dashboard.html (بعد حقن الرمز)
  ✓ لقطة kiosk.html (بعد حقن الرمز)
🟢 كل الفحوص نجحت. اللقطات في ./_shots/
```

Screenshots land in **`server/_shots/`**: `track.png` (public page + سارة), `dashboard.png`
(ops room), `kiosk.png` (Executive Brain face + KPIs). **Open them** — a good run shows the
rendered UI, not the login gate.

Override the port / PIN / output dir via env:

```bash
PORT=8500 PIN=2580 SHOTS=/tmp/shots node .claude/skills/run-dyar-gateway/driver.mjs
```

## Run (human path)

Plain boot serves the dashboard at `/` and the pages under their names. Useless headless
(the pages need a browser), and it blocks the terminal — Ctrl-C to stop:

```bash
cd server
OPS_PIN=1234 DYAR_PIN=1234 PORT=8080 node src/server.js
# → http://localhost:8080/  (dashboard, PIN 1234) · /kiosk.html · /driver.html · /track.html
```

## Test

The repo's own scenario tests live in the scratchpad, not the tree; the driver's API
smoke is the fast in-tree check. To exercise dispatch/priority logic against a running
server, boot it (human path) and hit the REST API, e.g.:

```bash
curl -s localhost:8080/api/health
curl -s -X POST localhost:8080/api/v1/dispatch -H 'x-api-key: test-key' \
  -H 'content-type: application/json' -d '{"ref":"9001","title":"t","dest":{"lat":32.94,"lng":35.27}}'
```

## Gotchas

- **Boot must not wait on the ops-room.** With `BRAIN_PANEL_URL` set, `boot()` awaits a
  state-restore fetch (up to 8s) **before** `listen`. The driver forces `BRAIN_PANEL_URL=`
  and `BRAIN_WEBHOOK_URL=` empty so it comes up in ~1s. Do the same for local runs.
- **Dashboard & kiosk are PIN-gated.** They render a login gate until a PIN is present.
  The driver gets past it by injecting storage then reloading: dashboard reads
  `sessionStorage.pin`, kiosk reads `localStorage.kiosk_pin` (driver PIN → `nd`/`gate`).
  A screenshot without the injection just shows the gate.
- **Map tiles are blank in headless.** dashboard/kiosk use MapLibre + OpenStreetMap tiles;
  the tile fetches fail with no network and the map stays dark. That is **not** an error —
  the driver filters `maplibre`/`Failed to fetch` from the JS-error check.
- **playwright-core ≠ pre-installed Chromium version.** A fresh `npm i playwright-core`
  expects a newer Chromium than the container's `chromium-1194`. The driver auto-discovers
  the real binary under `/opt/pw-browsers/chromium-*/chrome-linux/chrome` and passes it as
  `executablePath`. Never run `npx playwright install` (blocked/pointless here).
- **Two separate PINs.** `OPS_PIN` gates the panel/kiosk/brain; `DYAR_PIN` gates devices
  (driver page + WS device enrollment). If `OPS_PIN` is unset it falls back to `DYAR_PIN`.
  `/api/health` reports `pins.opsPinDistinct` — false means they share a secret.
- **Backgrounding servers gets reaped in this container.** Prefer the driver (it spawns +
  kills its own child in one process). For a persistent server, use the harness's
  `run_in_background`, not a shell `&`.

## Troubleshooting

- `browserType.launch: Executable doesn't exist at …chrome-headless-shell` → playwright-core
  version drift; the driver's `findChrome()` handles it. If you call chromium yourself, pass
  `executablePath` from `/opt/pw-browsers/chromium-*/chrome-linux/chrome`.
- Driver prints `✗ الخادم لم يُقلع` → read the `[srv]` lines above it; usually a port already
  in use (`PORT=…` to change) or a syntax error from a bad edit (`node --check src/server.js`).
- `✗ playwright-core غير مثبّت` → run the `npm i --no-save playwright-core` line above.
</content>
