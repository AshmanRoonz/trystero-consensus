# trystero-consensus

Owner-free, server-free consensus helpers for peer-to-peer web apps.

Extracted and generalized from the netcode of [Last Ship Sailing](https://lss.fractalreality.ca), a serverless multiplayer game built on [Trystero](https://github.com/dmotz/trystero). The helpers are **transport agnostic**: they make the decisions, you do the sending and receiving (Trystero actions, a WebSocket, `BroadcastChannel`, anything). That keeps the consensus logic testable in plain Node and reusable outside any one networking library.

No dependencies. ~200 lines. MIT.

```sh
npm i trystero-consensus
```

```js
import {
  electProposer,
  createConvergentState,
  createVoteRegister
} from 'trystero-consensus'
```

## Why

In a mesh of 3+ peers with no server, the awkward questions are: who owns the clock and the score? Who decides whether a hit landed? Who picks the random seed everyone has to agree on? Electing a single "host" peer reintroduces a single point of failure: if that peer lags or drops, everyone stutters.

These three patterns answer those questions without a host and without a server. They are independent; use one or all three.

## 1. Proposer election

Some actions need exactly one peer to perform them: seeding a shared random map, scheduling a countdown, spawning a pickup everyone must agree on. Instead of running a server to pick that peer, every peer sorts the full id set (self plus current peers) the same way and takes index 0. Everyone arrives at the same winner with zero messages exchanged, and when a peer joins or drops the winner recomputes on its own, so the mesh self-heals with no handover protocol.

```js
import {electProposer, isProposer} from 'trystero-consensus'

const peerIds = () => Object.keys(room.getPeers()) // Trystero

// Only the proposer seeds the world; everyone else waits for the broadcast.
if (isProposer(selfId, peerIds())) {
  sendStart({map: pickMap(), seed: (Math.random() * 2 ** 32) >>> 0})
}

// When you RECEIVE a proposal, verify it really came from the proposer
// before trusting it (guards against a stale or malicious sender):
onStart((cfg, fromPeerId) => {
  if (electProposer(selfId, peerIds()) !== fromPeerId) return
  buildWorld(cfg.map, cfg.seed)
})
```

`electProposer(selfId, peerIds, {compare})` returns the winning id. `isProposer(...)` is the boolean shortcut. Pass a custom `compare` if your ids are not plain sortable strings.

## 2. Convergent state

Nobody owns the shared values. Every peer ticks its own copy on its own clock and broadcasts a snapshot a few times a second; every peer also merges the snapshots it receives back into its local copy:

- continuous values (timers, elapsed time) take the **average** across peers
- forward-only values (scores, round number) take the **max**, so the peer who first saw an increment pulls everyone forward and nobody regresses

Stale snapshots (a peer that went quiet past `staleMs`) are excluded automatically, so one laggy or dropped peer cannot drag the consensus. A solo or isolated peer just keeps ticking on its own.

```js
import {createConvergentState} from 'trystero-consensus'

const clock = createConvergentState({
  fields: {
    roundTimer: 'average',
    elapsed:    'average',
    scoreA:     'max',
    scoreB:     'max',
    round:      'max'
  },
  staleMs: 2000
})

const [sendSync, onSync] = room.makeAction('sync')
onSync((snap, peerId) => clock.ingest(peerId, snap))
room.onPeerLeave = id => clock.drop(id)

// 5 Hz: broadcast my snapshot, then pull my local state toward consensus.
setInterval(() => {
  const local = {
    roundTimer: game.roundTimer,
    elapsed:    game.elapsed,
    scoreA:     game.scoreA,
    scoreB:     game.scoreB,
    round:      game.round
  }
  sendSync(local)
  Object.assign(game, clock.merge(local)) // merge is pure; assign it back
}, 200)
```

Built-in reducers: `average`, `max`, `min`, `latest`. Or pass your own `(values) => result`. `merge(local)` returns the consensus object without mutating `local`; a field only changes when at least one fresh peer reported it, otherwise it echoes `local` back.

Methods: `ingest(peerId, values)`, `drop(peerId)`, `merge(local)`, `contributors()`, `prune()`.

## 3. Vote register

For discrete events one peer asserts and others must validate: "I hit you", "I grabbed the item", "this round is over". The claimant broadcasts a claim with a unique id; every peer checks it against its own view and broadcasts a yes/no vote; the claim resolves when a quorum of yes (confirmed) or no (rejected) arrives, or when it times out. Default quorum is a simple majority, `ceil(total / 2)`.

```js
import {createVoteRegister} from 'trystero-consensus'

const peerIds = () => Object.keys(room.getPeers())
const [sendClaim, onClaim] = room.makeAction('claim')
const [sendVote,  onVote]  = room.makeAction('vote')

const hits = createVoteRegister({
  getPeerCount: () => peerIds().length,   // count of OTHER peers; +1 self added internally
  timeoutMs: 1500,
  onResolved: (hitId, outcome, info) => {
    if (outcome === 'rejected') rollbackHit(hitId) // you applied it optimistically
  }
})

onClaim((claim, fromPeerId) => {
  hits.open(claim.id, claim)
  const valid = validateHitLocally(claim) // your spatial / line-of-fire check
  sendVote({id: claim.id, valid})
  hits.recordVote(claim.id, selfId, valid, claim)
})

onVote((v, fromPeerId) => hits.recordVote(v.id, fromPeerId, v.valid))
```

Two field-tested cautions baked into the design: apply the effect **optimistically** on the target side for responsiveness and let a rejection roll it back, and always **clamp / sanity-check** claim payloads off the wire (reject non-finite or negative damage, cap maximums) before acting; peers can be buggy or hostile.

### Custom quorum: 2-player target authority

In a 2-player match a "majority" is just the two of you, so a common rule is to let the target of the claim be the authority on whether they were hit:

```js
const hits = createVoteRegister({
  getPeerCount: () => peerIds().length,
  quorum: (yes, no, total, entry) => {
    if (total === 2 && entry.meta?.targetId) {
      const v = entry.votes.get(entry.meta.targetId)
      return v === true ? 'confirmed' : v === false ? 'rejected' : null
    }
    const needed = Math.ceil(total / 2)
    return yes >= needed ? 'confirmed' : no >= needed ? 'rejected' : null
  },
  onResolved: (id, outcome) => { /* ... */ }
})

hits.open('hit-7', {targetId: victimId})
```

## Using request/response (Trystero 0.25+)

The vote register hand-rolls broadcast plus tally so it works on any transport. If you are on Trystero 0.25 or later, its request/response actions can collect the votes for you, and you feed the results to whatever quorum rule you like:

```js
const hitVote = room.makeAction('hitvote', {
  kind: 'request',
  onRequest: claim => validateHitLocally(claim) // each peer answers true/false
})

async function claimHit(claim) {
  const results = await hitVote.requestMany(claim, {
    targets: peerIds(),
    timeoutMs: 1000
  })
  const yes = 1 + results.filter(r => r.status === 'fulfilled' && r.value).length // +1 self
  const confirmed = yes >= Math.ceil((peerIds().length + 1) / 2)
  if (!confirmed) rollbackHit(claim.id)
}
```

## Run the simulation

`sim.js` fakes a mesh of peers in memory (no network, no browser) and asserts every behavior above: election self-heal, average/max merge, stale eviction, full-mesh convergence, majority and target-authority voting, and timeout.

```sh
node sim.js
```

## Provenance

These patterns were developed for [Last Ship Sailing](https://lss.fractalreality.ca) and generalized here. The tiered broadcast model they slot into (high-rate position/state, foveal aim/projectiles between engaged players, event-driven consensus-validated kills and round events) is described in the game's netcode. If you build something with these, or fold the ideas into a library, an attribution back is appreciated but not required.

Built on the shoulders of [Trystero](https://github.com/dmotz/trystero) by Dan Motzenbecker.
