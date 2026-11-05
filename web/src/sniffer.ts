import * as signalR from '@microsoft/signalr';
import { decodeEnvelope } from './contract/decode';
import type { LiveEnvelope } from './contract/types';

/**
 * Listens on the real dev SignalR as its own user, so the real frontend's join/leave calls never touch
 * its group membership. Shows every live message that reaches this connection.
 */

const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const userInput = $<HTMLInputElement>('user');
const devicesInput = $<HTMLInputElement>('devices');
const connectBtn = $<HTMLButtonElement>('connect');
const rejoinBtn = $<HTMLButtonElement>('rejoin');
const leaveBtn = $<HTMLButtonElement>('leave');
const disconnectBtn = $<HTMLButtonElement>('disconnect');
const showTelemetry = $<HTMLInputElement>('showTelemetry');
const stateEl = $<HTMLSpanElement>('state');
const logEl = $<HTMLTableSectionElement>('log');

const STORAGE_KEY = 'sniffer.inputs';
try {
  const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '{}') as { user?: string; devices?: string };
  userInput.value = saved.user ?? 'mock-debug';
  devicesInput.value = saved.devices ?? '';
} catch {
  userInput.value = 'mock-debug';
}

let hub: signalR.HubConnection | null = null;

const userId = () => userInput.value.trim() || 'mock-debug';
const deviceGuids = () =>
  devicesInput.value.split(',').map(s => s.trim().toLowerCase()).filter(Boolean);

function setState(state: string) {
  stateEl.textContent = state;
  const connected = state === 'connected';
  connectBtn.disabled = hub !== null;
  rejoinBtn.disabled = !connected;
  leaveBtn.disabled = !connected;
  disconnectBtn.disabled = hub === null;
}

function row(kind: 'live' | 'end' | 'sys' | '', message: string, session: string, device: string, info: string, raw?: unknown) {
  const tr = document.createElement('tr');
  if (kind) tr.className = kind;
  const cells = [new Date().toISOString().slice(11, 23), message, session, device, info];
  for (const text of cells) {
    const td = document.createElement('td');
    td.textContent = text;
    tr.appendChild(td);
  }
  if (raw !== undefined) {
    const details = document.createElement('details');
    const summary = document.createElement('summary');
    summary.textContent = 'raw';
    const pre = document.createElement('pre');
    pre.textContent = JSON.stringify(raw, null, 2);
    details.append(summary, pre);
    tr.lastElementChild!.appendChild(details);
  }
  logEl.prepend(tr);
}

const sys = (text: string) => row('sys', 'system', '', '', text);

async function negotiator(path: string, json?: unknown): Promise<unknown> {
  const headers: Record<string, string> = { 'x-user-id': userId() };
  if (json !== undefined) headers['content-type'] = 'application/json';
  const res = await fetch(`/neg/api/${path}`, {
    method: 'POST',
    headers,
    body: json === undefined ? undefined : JSON.stringify(json),
  });
  const body = await res.json().catch(() => null);
  if (!res.ok) throw new Error(`${path} → ${res.status} ${JSON.stringify(body)}`);
  return body;
}

async function joinDevices() {
  for (const guid of deviceGuids()) {
    await negotiator(`join-device?deviceId=${encodeURIComponent(guid)}`);
    sys(`join-device ${guid} as '${userId()}' → OK`);
  }
}

// Same call the frontend's list-page cleanup makes (unSubscribeDeviceBatchTelemetery).
async function leaveDevices() {
  const ids = deviceGuids();
  await negotiator('leave-devices', { deviceIds: ids });
  sys(`leave-devices [${ids.length} ids] as '${userId()}' → OK`);
}

async function connect() {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ user: userInput.value, devices: devicesInput.value }));
  } catch { /* storage blocked: inputs just aren't remembered */ }

  setState('negotiating');
  // The negotiator serializes the envelope in PascalCase (Data.Url / Data.AccessToken).
  type Info = { url?: string; Url?: string; accessToken?: string; AccessToken?: string };
  const body = (await negotiator('negotiate')) as { data?: Info; Data?: Info };
  const info = body?.data ?? body?.Data;
  const url = info?.url ?? info?.Url;
  const accessToken = info?.accessToken ?? info?.AccessToken;
  if (!url || !accessToken) throw new Error(`negotiate returned no url/token (keys: ${Object.keys(body ?? {}).join(', ')})`);
  sys(`negotiate as '${userId()}' → ${new URL(url).host}`);

  hub = new signalR.HubConnectionBuilder()
    .withUrl(url, { accessTokenFactory: () => accessToken })
    .withAutomaticReconnect()
    .configureLogging(signalR.LogLevel.Warning)
    .build();

  hub.on('liveTelemetryUpdate', async (envelope: LiveEnvelope) => {
    try {
      const { batch, envelopeBytes, decodedBytes } = await decodeEnvelope(envelope);
      const first = batch.entries[0]?.timestampUtc ?? '';
      const last = batch.entries[batch.entries.length - 1]?.timestampUtc ?? '';
      row('live', 'liveTelemetryUpdate', batch.sessionId, batch.deviceGuid,
        `${batch.entries.length} entries ${first.slice(11, 19)}→${last.slice(11, 19)} · ${envelopeBytes}B→${decodedBytes}B`, batch);
    } catch (error) {
      row('end', 'liveTelemetryUpdate', '', '', `DECODE FAILED: ${String(error)}`, envelope);
    }
  });

  for (const name of ['liveSessionEnded', 'liveSessionExtended']) {
    hub.on(name, async (envelope: LiveEnvelope & Record<string, unknown>) => {
      // Same envelope as live batches when compressed; logged as-is otherwise.
      let payload: Record<string, unknown> = envelope;
      if (envelope?.encoding === 'gzip+base64') {
        payload = (await decodeEnvelope(envelope)).batch as unknown as Record<string, unknown>;
      }
      row('end', name, String(payload.sessionId ?? ''), String(payload.deviceGuid ?? ''),
        String(payload.reason ?? payload.stopReason ?? payload.expiresUtc ?? ''), payload);
    });
  }

  hub.on('telemetryUpdate', (msg: { deviceGuid?: string; timestampUtc?: string; meta?: { health?: string } }) => {
    if (!showTelemetry.checked) return;
    row('', 'telemetryUpdate', '', msg.deviceGuid ?? '', `${msg.timestampUtc ?? ''} health=${msg.meta?.health ?? ''}`, msg);
  });

  hub.onreconnecting(() => { setState('reconnecting'); sys('reconnecting…'); });
  hub.onreconnected(async () => {
    setState('connected');
    sys('reconnected');
    // Membership is per user and survives reconnects; re-joining is harmless and rules it out.
    await joinDevices().catch(e => sys(`re-join failed: ${String(e)}`));
  });
  hub.onclose(error => {
    hub = null;
    setState('disconnected');
    sys(`closed${error ? `: ${String(error)}` : ''}`);
  });

  await hub.start();
  setState('connected');
  sys('connected');

  // negotiate auto-joins telemetry:all; leave it so this tab doesn't double the fleet's message bill.
  await negotiator('leave-all');
  sys(`leave-all → OK (telemetry:all dropped; events:all + dashboard:stats stay)`);

  await joinDevices();
}

connectBtn.onclick = () => {
  connect().catch(error => {
    sys(`connect failed: ${String(error)}`);
    void hub?.stop();
    hub = null;
    setState('disconnected');
  });
};
rejoinBtn.onclick = () => void joinDevices().catch(e => sys(`join failed: ${String(e)}`));
leaveBtn.onclick = () => void leaveDevices().catch(e => sys(`leave failed: ${String(e)}`));
disconnectBtn.onclick = () => void hub?.stop();
$<HTMLButtonElement>('clear').onclick = () => { logEl.innerHTML = ''; };

setState('disconnected');
