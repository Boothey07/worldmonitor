// Ported from the iOS client's ConflictModels.swift (ConflictSourceOutcome /
// recordingLagDays). Deliberately string-free: formatting happens in the
// component layer where the translations live — see UcdpEventsPanel.

/**
 * What one conflict source actually did on the last fetch.
 *
 * `empty` and `unavailable` are deliberately separate states. On 2026-08-25 the
 * ACLED route returned HTTP 200 carrying `{"events":[]}` — a successful response
 * with nothing in it, because the backend's upstream fetch was Cloudflare-blocked
 * and its error path resolved to an empty array. Collapsing that into "no events"
 * lets a broken integration read as a quiet period; collapsing it into "error"
 * cries wolf over a legitimately quiet dataset. Neither endpoint sends a
 * freshness field (the envelope is a bare `{"events":[…]}`), so currency must be
 * derived from the records themselves.
 */
export interface ConflictSourceOutcome {
  /** `records` = the source returned rows; `empty` = it answered and had none; `unavailable` = the fetch itself failed. */
  readonly status: 'records' | 'empty' | 'unavailable';
  readonly count: number;
  /** Epoch ms of the freshest record, taken from the data — never asserted by the server. */
  readonly freshestMs?: number;
}

/**
 * Build the outcome from the fetched rows' occurrence timestamps. A `null`
 * argument means the fetch threw; an empty array means it answered with nothing.
 */
export function deriveSourceOutcome(occurredAtMs: ReadonlyArray<number | undefined> | null): ConflictSourceOutcome {
  if (occurredAtMs === null) return { status: 'unavailable', count: 0 };
  const dated = occurredAtMs.filter((ms): ms is number => typeof ms === 'number');
  return {
    status: occurredAtMs.length > 0 ? 'records' : 'empty',
    count: occurredAtMs.length,
    freshestMs: dated.length > 0 ? Math.max(...dated) : undefined,
  };
}

/** Whole days between the freshest record and `now`; guarded against future-dated rows reading negative. */
export function recordingLagDays(freshestMs: number | undefined, now: number = Date.now()): number | undefined {
  if (freshestMs === undefined) return undefined;
  return Math.max(0, Math.floor((now - freshestMs) / 86_400_000));
}
