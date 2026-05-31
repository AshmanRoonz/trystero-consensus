// trystero-consensus
// ---------------------------------------------------------------------------
// Owner-free, server-free consensus helpers for peer-to-peer web apps.
// Extracted and generalized from the netcode of Last Ship Sailing
// (https://lss.fractalreality.ca), originally built on Trystero.
//
// These helpers are TRANSPORT AGNOSTIC. They make decisions; you do the
// sending and receiving (Trystero actions, a WebSocket, BroadcastChannel,
// anything). That keeps the consensus logic testable in plain Node and
// reusable outside any one networking library.
//
// Three patterns, each independent:
//   1. electProposer    deterministic, self-healing "who acts" with no server
//   2. ConvergentState  shared values that blend toward a mesh consensus
//   3. VoteRegister     claim / vote / resolve for validating events
//
// MIT licensed. See LICENSE.
// ---------------------------------------------------------------------------

const defaultNow = () =>
  typeof performance !== 'undefined' && performance.now
    ? performance.now()
    : Date.now();

// ===========================================================================
// 1. Proposer election (deterministic leader, zero coordination)
// ===========================================================================
//
// Some actions need exactly one peer to perform them: seeding a shared random
// map, scheduling a launch countdown, spawning a pickup everyone must agree
// on. Running a server just to pick that peer defeats the point of P2P.
//
// Instead, every peer sorts the full id set (self plus current peers) with the
// same comparator and takes index 0. Everyone arrives at the same winner with
// no messages exchanged. When a peer joins or drops, the winner recomputes on
// its own, so the mesh self-heals with no handover protocol.
//
// Guarding incoming proposals: when you receive a "do X" message that claims
// to come from the proposer, verify it with
// electProposer(selfId, peerIds) === message.proposerId before honoring it.

export function electProposer(selfId, peerIds, {compare} = {}) {
  const cmp = compare || ((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  return [selfId, ...peerIds].sort(cmp)[0];
}

export function isProposer(selfId, peerIds, opts) {
  return electProposer(selfId, peerIds, opts) === selfId;
}

// ===========================================================================
// 2. ConvergentState (owner-free shared values)
// ===========================================================================
//
// The problem: in a 3+ peer mesh with no server, who owns the round timer, the
// scores, the clock? If you elect one owner and that owner lags or drops, the
// whole match stutters.
//
// The pattern: nobody owns it. Every peer ticks its own copy on its own clock
// and broadcasts a snapshot a few times a second. Every peer also merges the
// snapshots it receives back into its local copy:
//   - continuous values (timers, elapsed time) take the AVERAGE across peers
//   - forward-only values (scores, round number) take the MAX, so the peer who
//     first saw an increment pulls everyone forward and nobody regresses
// Stale snapshots (a peer that went quiet) are excluded automatically, so one
// laggy or dropped peer cannot drag the consensus. A solo / isolated peer just
// keeps ticking on its own.

export const reducers = {
  average: values => values.reduce((a, b) => a + b, 0) / values.length,
  max: values => values.reduce((a, b) => (b > a ? b : a), values[0]),
  min: values => values.reduce((a, b) => (b < a ? b : a), values[0]),
  // "last write wins" by arrival; useful for non-numeric fields. Pass the
  // freshest peer value through (the merge() caller supplies values already
  // ordered local-first, so we take the last contributor).
  latest: values => values[values.length - 1]
};

// fields: { fieldName: 'average' | 'max' | 'min' | 'latest' | (values) => any }
export function createConvergentState({
  fields,
  staleMs = 2000,
  now = defaultNow
} = {}) {
  if (!fields || typeof fields !== 'object') {
    throw new Error('createConvergentState requires a `fields` map');
  }

  // peerId -> { values: {field: value}, at: timestamp }
  const peers = new Map();

  const reducerFor = rule => {
    if (typeof rule === 'function') return rule;
    if (reducers[rule]) return reducers[rule];
    throw new Error(`Unknown merge rule for field: ${String(rule)}`);
  };

  const isLive = (snap, at) => at - snap.at <= staleMs;

  return {
    // Record (or replace) a peer's latest snapshot. Timestamped on arrival so
    // it can later go stale. Call this from your network receive handler.
    ingest(peerId, values, at = now()) {
      peers.set(peerId, {values: values || {}, at});
      return this;
    },

    // Forget a peer at once (e.g. on a clean disconnect). Optional; stale
    // eviction already handles silent drops.
    drop(peerId) {
      peers.delete(peerId);
      return this;
    },

    // The peer ids currently contributing (non-stale). Handy for debugging /
    // showing "synced with N peers".
    contributors(at = now()) {
      const live = [];
      for (const [pid, snap] of peers) if (isLive(snap, at)) live.push(pid);
      return live;
    },

    // Blend `local` (this peer's current values) with every fresh peer
    // snapshot and RETURN the consensus values. Pure: it does not mutate
    // `local`. Assign the result back yourself, e.g.
    //   Object.assign(game, state.merge(game))
    // A field only changes when at least one fresh peer reported it (local
    // plus one peer = 2 samples); otherwise that field echoes local back, so
    // an isolated peer keeps its own value and never snaps to zero.
    merge(local, at = now()) {
      const out = {};
      for (const field of Object.keys(fields)) {
        const reduce = reducerFor(fields[field]);
        const values = [];
        if (local[field] !== undefined && local[field] !== null) {
          values.push(local[field]);
        }
        for (const snap of peers.values()) {
          if (!isLive(snap, at)) continue;
          const v = snap.values[field];
          if (v !== undefined && v !== null) values.push(v);
        }
        out[field] = values.length >= 2 ? reduce(values) : local[field];
      }
      return out;
    },

    // Drop stale peers from memory. Optional housekeeping; safe to call on a
    // timer or never (merge already ignores stale entries).
    prune(at = now()) {
      for (const [pid, snap] of peers) if (!isLive(snap, at)) peers.delete(pid);
      return this;
    }
  };
}

// ===========================================================================
// 3. VoteRegister (claim / vote / resolve)
// ===========================================================================
//
// For discrete events that one peer asserts and others must validate: "I hit
// you", "I picked up the item", "this round is over". The claimant broadcasts
// a claim with a unique id. Every peer checks it against its own view of the
// world and broadcasts a yes/no vote. A claim resolves when a quorum of
// yes (confirmed) or no (rejected) votes arrives, or when it times out.
//
// Default quorum is a simple majority: ceil(total / 2), where total counts
// self plus current peers. You can pass a custom `quorum` for other rules; the
// classic example is 2-player "target authority", where the target's own vote
// decides (see README).
//
// This register only DECIDES. Pair it with optimistic local application for
// responsiveness (apply the effect immediately, let a rejection roll it back),
// and always clamp / sanity-check claim payloads from the wire before acting
// on them; peers can be buggy or hostile.

export function createVoteRegister({
  getPeerCount,
  quorum,
  timeoutMs = 3000,
  now = defaultNow,
  onResolved,
  setTimer = (fn, ms) => setTimeout(fn, ms),
  clearTimer = h => clearTimeout(h)
} = {}) {
  if (typeof getPeerCount !== 'function') {
    throw new Error('createVoteRegister requires a `getPeerCount` function');
  }

  // claimId -> { meta, votes: Map<voterId, bool>, openedAt, timer }
  const pending = new Map();

  const majority = (yes, no, total) => {
    const needed = Math.ceil(total / 2);
    if (yes >= needed) return 'confirmed';
    if (no >= needed) return 'rejected';
    return null;
  };
  const decide = quorum || majority;

  const tally = entry => {
    let yes = 0;
    let no = 0;
    for (const v of entry.votes.values()) v ? yes++ : no++;
    return {yes, no};
  };

  function open(claimId, meta = null) {
    if (pending.has(claimId)) return;
    const entry = {meta, votes: new Map(), openedAt: now(), timer: null};
    if (timeoutMs > 0) {
      entry.timer = setTimer(() => resolve(claimId, 'timeout'), timeoutMs);
    }
    pending.set(claimId, entry);
  }

  function resolve(claimId, outcome) {
    const entry = pending.get(claimId);
    if (!entry) return;
    if (entry.timer != null) clearTimer(entry.timer);
    pending.delete(claimId);
    if (onResolved) {
      onResolved(claimId, outcome, {...tally(entry), meta: entry.meta});
    }
  }

  function recordVote(claimId, voterId, valid, meta = null) {
    if (!pending.has(claimId)) open(claimId, meta);
    const entry = pending.get(claimId);
    entry.votes.set(voterId, !!valid);

    const total = getPeerCount() + 1; // +1 for self
    const {yes, no} = tally(entry);
    const outcome = decide(yes, no, total, entry);
    if (outcome === 'confirmed' || outcome === 'rejected') {
      resolve(claimId, outcome);
    }
  }

  return {
    open,
    recordVote,
    // Force-resolve a claim as rejected (e.g. the claimant left).
    cancel: claimId => resolve(claimId, 'rejected'),
    has: claimId => pending.has(claimId),
    pendingCount: () => pending.size
  };
}

export default {
  electProposer,
  isProposer,
  reducers,
  createConvergentState,
  createVoteRegister
};
