import { CHROME_UA } from './constants';

export function getRelayBaseUrl(): string | null {
  const relayUrl = process.env.WS_RELAY_URL;
  if (!relayUrl) return null;
  return relayUrl.replace(/^ws(s?):\/\//, 'http$1://').replace(/\/$/, '');
}

/**
 * Headers for the ACLED egress relay.
 *
 * Deliberately NOT getRelayHeaders(): that helper also sets
 * `Authorization: Bearer <relay secret>`, which would overwrite the ACLED
 * bearer token the upstream actually needs. The relay authenticates on
 * x-relay-key alone and strips that header before forwarding.
 */
export function getAcledRelayHeaders(): Record<string, string> {
  const secret = process.env.RELAY_SHARED_SECRET;
  return secret ? { 'x-relay-key': secret } : {};
}

export function getRelayHeaders(extra: Record<string, string> = {}): Record<string, string> {
  const headers: Record<string, string> = {
    Accept: 'application/json',
    'User-Agent': CHROME_UA,
    ...extra,
  };
  const relaySecret = process.env.RELAY_SHARED_SECRET;
  if (!relaySecret) return headers;
  const relayHeader = (process.env.RELAY_AUTH_HEADER || 'x-relay-key').toLowerCase();
  headers[relayHeader] = relaySecret;
  // Only add a separate Authorization: Bearer header when relayHeader is not 'authorization'.
  // If RELAY_AUTH_HEADER=Authorization, both keys normalize to the same HTTP header and
  // Undici merges them into "secret, Bearer secret", which breaks the relay's direct-compare check.
  if (relayHeader !== 'authorization') {
    headers.Authorization = `Bearer ${relaySecret}`;
  }
  return headers;
}
