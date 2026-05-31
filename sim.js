// sim.js
// Runnable, dependency-free simulation + smoke test for trystero-consensus.
// It fakes a mesh of peers in memory (no network, no browser) so the
// consensus logic can be exercised and asserted in plain Node:
//
//   node sim.js
//
// Exits non-zero if any assertion fails.

import {
  electProposer,
  isProposer,
  createConvergentState,
  createVoteRegister
} from './index.js';

let passed = 0;
let failed = 0;

function ok(label, cond) {
  if (cond) {
    passed++;
    console.log('  PASS  ' + label);
  } else {
    failed++;
    console.log('  FAIL  ' + label);
  }
}

const approx = (a, b, eps = 1e-9) => Math.abs(a - b) <= eps;

// ---------------------------------------------------------------------------
console.log('\n[1] Proposer election (deterministic, self-healing)');
// ---------------------------------------------------------------------------
ok('lowest id wins regardless of order', electProposer('c', ['a', 'b']) === 'a');
ok('self is proposer when lowest', isProposer('a', ['b', 'c']) === true);
ok('self is not proposer otherwise', isProposer('b', ['a', 'c']) === false);
ok(
  'winner recomputes when the proposer drops (self-heal)',
  electProposer('b', ['c']) === 'b'
);

// ---------------------------------------------------------------------------
console.log('\n[2] ConvergentState (average + max merge, stale eviction)');
// ---------------------------------------------------------------------------
{
  const clock = {t: 1000};
  const now = () => clock.t;
  const state = createConvergentState({
    fields: {timer: 'average', score: 'max', round: 'max'},
    staleMs: 2000,
    now
  });

  const local = {timer: 10, score: 2, round: 1};
  state.ingest('p1', {timer: 8, score: 5, round: 1});
  state.ingest('p2', {timer: 12, score: 2, round: 2});

  const merged = state.merge(local);
  ok('continuous field averages: (10+8+12)/3 = 10', approx(merged.timer, 10));
  ok('forward-only field takes the max: 5', merged.score === 5);
  ok('round advances to the furthest peer: 2', merged.round === 2);
  ok('merge does not mutate local', local.timer === 10);

  // Stale eviction: p2 goes quiet, time passes beyond staleMs.
  clock.t += 5000;
  state.ingest('p1', {timer: 9, score: 5, round: 2}); // p1 still fresh
  const merged2 = state.merge({timer: 11, score: 2, round: 2});
  ok(
    'stale peer (p2) excluded from average: (11+9)/2 = 10',
    approx(merged2.timer, 10)
  );
  ok('only fresh contributor counted', state.contributors().length === 1);
}

// ---------------------------------------------------------------------------
console.log('\n[2b] Full mesh converges in one all-to-all round');
// ---------------------------------------------------------------------------
{
  // Four peers, each with a slightly different local clock. One synchronous
  // broadcast + merge round should pull every peer to the shared mean.
  const ids = ['p0', 'p1', 'p2', 'p3'];
  const locals = {p0: 10.0, p1: 10.6, p2: 9.4, p3: 10.2};
  const mean = (10.0 + 10.6 + 9.4 + 10.2) / 4;

  const states = {};
  for (const id of ids) {
    states[id] = createConvergentState({fields: {t: 'average'}, staleMs: 5000});
  }
  // Everyone ingests everyone else's snapshot.
  for (const id of ids) {
    for (const other of ids) {
      if (other !== id) states[id].ingest(other, {t: locals[other]});
    }
  }
  const next = {};
  for (const id of ids) next[id] = states[id].merge({t: locals[id]}).t;

  const spread = Math.max(...Object.values(next)) - Math.min(...Object.values(next));
  ok('all peers land on the global mean after one round', spread < 1e-9);
  ok('that value is the true average', approx(next.p0, mean));
}

// ---------------------------------------------------------------------------
console.log('\n[3] VoteRegister (simple majority)');
// ---------------------------------------------------------------------------
{
  const outcomes = [];
  const reg = createVoteRegister({
    getPeerCount: () => 2, // 3 peers total (self + 2)
    timeoutMs: 0, // no timeout for this test
    onResolved: (id, outcome) => outcomes.push([id, outcome])
  });

  reg.open('h1');
  reg.recordVote('h1', 'self', true);
  ok('not resolved at 1/2 yes (needs ceil(3/2)=2)', reg.has('h1'));
  reg.recordVote('h1', 'p1', true);
  ok('confirmed at 2 yes votes', outcomes.some(([id, o]) => id === 'h1' && o === 'confirmed'));
  ok('resolved claim is cleared', !reg.has('h1'));

  reg.recordVote('h2', 'self', false); // auto-opens on first vote
  reg.recordVote('h2', 'p1', false);
  ok('rejected at 2 no votes', outcomes.some(([id, o]) => id === 'h2' && o === 'rejected'));
}

// ---------------------------------------------------------------------------
console.log('\n[3b] VoteRegister (custom 2-player target authority)');
// ---------------------------------------------------------------------------
{
  // In a 2-player match a "majority" is just the two of you, so let the target
  // of the claim be the authority on whether they were hit.
  const outcomes = [];
  const reg = createVoteRegister({
    getPeerCount: () => 1, // 2 peers total
    timeoutMs: 0,
    quorum: (yes, no, total, entry) => {
      if (total === 2 && entry.meta && entry.meta.targetId) {
        const v = entry.votes.get(entry.meta.targetId);
        if (v === true) return 'confirmed';
        if (v === false) return 'rejected';
        return null;
      }
      const needed = Math.ceil(total / 2);
      if (yes >= needed) return 'confirmed';
      if (no >= needed) return 'rejected';
      return null;
    },
    onResolved: (id, outcome) => outcomes.push([id, outcome])
  });

  reg.open('hit-7', {targetId: 'victim'});
  reg.recordVote('hit-7', 'shooter', true); // shooter's vote alone should not decide
  ok('shooter vote alone does not resolve', reg.has('hit-7'));
  reg.recordVote('hit-7', 'victim', true); // target confirms
  ok('target authority confirms the hit', outcomes.some(([id, o]) => id === 'hit-7' && o === 'confirmed'));
}

// ---------------------------------------------------------------------------
console.log('\n[3c] VoteRegister (timeout via injected timer)');
// ---------------------------------------------------------------------------
{
  const timers = [];
  const outcomes = [];
  const reg = createVoteRegister({
    getPeerCount: () => 4, // 5 peers; quorum of 3 never reached here
    timeoutMs: 3000,
    setTimer: fn => {
      timers.push(fn);
      return timers.length - 1;
    },
    clearTimer: () => {},
    onResolved: (id, outcome) => outcomes.push([id, outcome])
  });

  reg.open('slow');
  reg.recordVote('slow', 'self', true); // only 1 yes, no quorum
  ok('still pending before timeout fires', reg.has('slow'));
  timers.forEach(fn => fn()); // simulate the clock reaching timeoutMs
  ok('resolves as timeout', outcomes.some(([id, o]) => id === 'slow' && o === 'timeout'));
}

// ---------------------------------------------------------------------------
console.log(`\n${failed === 0 ? 'ALL GREEN' : 'FAILURES'}: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
