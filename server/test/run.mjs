#!/usr/bin/env node
// 🧪 مُشغّل فحوص ديار: يُقلع خادماً نظيفاً لكل حزمة على منفذ مستقل ثم يشغّلها — فلا تلوّث حزمةٌ حزمة
// (حدود المعدل، الطلبات المكرّرة، الموصلون الوهميون…). يفشل بأول حزمة تفشل — للتشغيل محلياً وفي CI.
// التشغيل من مجلّد الوحدة (server/):  npm test
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

const SUITES = [
  { file: 'test/routes-matrix.mjs', name: 'مصفوفة عقود المسارات' },
  { file: 'test/agents-test.mjs', name: 'طبقة الوكلاء (سقوط آمن)' },
  { file: 'test/finance-drill.mjs', name: 'المحاسبة والديمومة' },
  { file: 'test/full-drill.mjs', name: 'البروفة الشاملة' },
];
let port = 8460 + Math.floor(Math.random() * 300);
let failed = 0;

for (const s of SUITES) {
  const P = String(++port);
  const stateDir = mkdtempSync(join(tmpdir(), 'dyar-state-'));
  console.log(`\n════ ${s.name} (منفذ ${P}) ════`);
  const srv = spawn('node', ['src/server.js'], {
    env: { ...process.env, PORT: P, OPS_PIN: '1234', DYAR_PIN: '1234', BRAIN_API_KEY: 'dyar-brain-key',
      PIN_MAX_PER_MIN: '100000', BRAIN_PANEL_URL: '', BRAIN_WEBHOOK_URL: '', ANTHROPIC_API_KEY: '',
      STATE_DIR: stateDir },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  srv.stderr.on('data', (d) => process.stderr.write('[srv] ' + d));
  let up = false;
  for (let i = 0; i < 50 && !up; i++) {
    try { up = (await fetch(`http://127.0.0.1:${P}/api/health`)).ok; } catch {}
    if (!up) await sleep(300);
  }
  if (!up) { console.error('✗ الخادم لم يُقلع'); srv.kill('SIGKILL'); failed++; break; }
  const code = await new Promise((res) => {
    const t = spawn('node', [s.file], { env: { ...process.env, DP: P, STATE_DIR: stateDir }, stdio: 'inherit' });
    t.on('exit', res); t.on('error', () => res(1));
  });
  srv.kill('SIGKILL');
  rmSync(stateDir, { recursive: true, force: true });
  if (code !== 0) { failed++; console.error(`✗ فشلت حزمة: ${s.name}`); break; }   // فشل واحد يكفي لإيقاف CI
}

console.log(failed ? '\n🔴 الفحوص فشلت' : '\n🟢 كل الحزم نجحت');
process.exit(failed ? 1 : 0);
