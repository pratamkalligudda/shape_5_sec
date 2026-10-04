import * as signalR from '@microsoft/signalr';
import { config } from '../config';
import type { LiveEnvelope } from '../contract/types';

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting';

/** No auth: the negotiator takes the caller's id from x-user-id (spec §5.3). */
export const USER_ID = 'local';

async function negotiator(path: string): Promise<Response> {
  const res = await fetch(`${config.negotiatorUrl}/api/${path}`, { method: 'POST', headers: { 'x-user-id': USER_ID } });
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return res;
}

/** negotiate → connect → join-device(deviceGuid); re-joins after every automatic reconnect (spec §8.1 step 2). */
export async function openLiveConnection(
  onEnvelope: (envelope: LiveEnvelope) => void,
  onState: (state: ConnectionState) => void,
): Promise<signalR.HubConnection> {
  const info = (await (await negotiator('negotiate')).json()) as { url: string; accessToken: string };
  const connection = new signalR.HubConnectionBuilder()
    .withUrl(info.url, { accessTokenFactory: () => info.accessToken })
    .withAutomaticReconnect()
    .configureLogging(signalR.LogLevel.Warning)
    .build();
  const join = () => negotiator(`join-device?deviceId=${encodeURIComponent(config.deviceGuid)}`);

  connection.on('liveTelemetryUpdate', onEnvelope);
  connection.onreconnecting(() => onState('reconnecting'));
  connection.onreconnected(async () => {
    await join();
    onState('connected');
  });
  await connection.start();
  await join();
  onState('connected');
  return connection;
}
