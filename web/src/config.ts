const env = import.meta.env;

export const config = {
  backendUrl: env.VITE_BACKEND_URL ?? 'http://localhost:5080',
  negotiatorUrl: env.VITE_NEGOTIATOR_URL ?? 'http://localhost:7071',
  simulatorUrl: env.VITE_SIMULATOR_URL ?? 'http://localhost:5100',
  deviceSerial: env.VITE_DEVICE_SERIAL ?? 'PUMP-00417',
  /** Configured, because no live-session response carries it (findings F1). */
  deviceGuid: env.VITE_DEVICE_GUID ?? '9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284',
  deviceName: env.VITE_DEVICE_NAME ?? 'PREC-NEO30-MARYL-07',
};
