/**
 * Premium RPC paths that require either an API key or a Pro session.
 *
 * Single source of truth consumed by both the server gateway (auth enforcement)
 * and the web client runtime (token injection).
 */
// Self-hosted fork: no route requires authentication. The operator is the
// only user and accepts the LLM/API spend on all routes.
export const PREMIUM_RPC_PATHS = new Set<string>([
  // (intentionally empty)
]);
