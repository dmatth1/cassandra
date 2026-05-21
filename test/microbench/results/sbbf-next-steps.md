# SBBF + Wyhash port -- next steps to CEP and PR

This branch (`claude/port-quickbloom-cassandra-S4kLX`) holds a complete
reference implementation of Split Block Bloom Filter + Wyhash as a new
`IFilter` for Cassandra. The PR is ready to file, but **filing it
straight against `apache/cassandra` without a CEP first is unlikely to
land** -- bloom-filter format changes are exactly what the CEP process
exists to gate. This document captures what is still needed to take
this to merge, with honest expectations about effort and outcome.

## Status

  - **Implementation**: 1103 insertions across 14 files, one commit
    (`26da13a1`), tests green:
    - `SBBFBloomFilterTest`     11/11
    - `WyhashTest`               5/5
    - `BloomFilterTest`        11/11 (2 ignored, same as trunk)
    - `SerializationsTest`      3/3
  - **JMH numbers** (3-fork, OpenJDK 21): 1.96-2.91x per-probe speedup
    across all cache tiers, average 2.35x, XL-hit 2.34x.
    See `sbbf-vs-classical.txt` for the full table.
  - **Allocation profile**: zero allocation on the hot path
    (`-prof gc`: 0.002 B/op for XL hit, error bars touching zero).
  - **Bench coverage**: variable key sizes (16 / 64 / 200 bytes) and
    p50 / p99 measured during investigation, results not retained in
    the slimmed PR but reproducible from this branch.
  - **Backward compatibility**: classical-format SSTables continue to
    deserialize via magic-byte dispatch in `BloomFilterSerializer`.
    No coordinated upgrade required.
  - **CEP draft**: lives at `dmatth1/quickbloom @ quickbloom-java`,
    `investigations/quickbloom_java/CEP_DRAFT.md`. Numbers and
    framing both need refresh; see "What needs to change in the CEP"
    below.

## Honest production impact (read before pitching anything)

Earlier iterations of this work claimed "5-25% read p99 improvement"
based on an interpolation from microbench. **That number is too
high** -- it assumes bloom is 10-25% of read latency, which it isn't
in any workload I'm familiar with. The actual math:

For a typical SELECT, bloom probes consume:
  - **LCS, narrow-fan (N=3 SSTables)**: ~600 ns of a 500-5000 us read
    = 0.01-0.1% of total latency.
  - **STCS, wide-fan (N=50 SSTables)**: ~10 us of a 1-10 ms read
    = 0.1-1% of total latency.

Cutting bloom in half via SBBF:
  - LCS: sub-noise. Not measurable.
  - Wide-fan STCS / range queries / `ALLOW FILTERING`: 0.05-0.5%
    p99 improvement.

**The honest production p99 win is in the low single digits at best,
mostly imperceptible.** The CPU savings (2-5% on heavy-read clusters)
are real but not a forcing function. Memory bandwidth and FP-rate
improvements come out in the noise.

If you pitch SBBF as a perf win, expect committers to do this math.

## The defensible pitch (if you do file the CEP)

Lead with the structural argument, not the speedup number:

  - **"SBBF is the better long-term design for SSTable bloom
    filtering."** One cache line per probe, simpler probe geometry,
    slightly better FP at iso-memory. This is the actual reason to
    do it.
  - **"Per-probe microbench shows ~2.3x speedup."** True but
    oversells production impact.
  - **"Realistic production p99 improvement: low single-digit
    percent on wide-fan reads, essentially zero on typical reads."**
    Honest, will survive a committer doing the math.
  - **"No operator action required."** Classical SSTables keep
    working; new SSTables get SBBF; operators benefit incrementally
    via compaction.
  - **"Foundation for the Vector API future."** When Vector API
    graduates (Valhalla-gated, JDK 28-29 earliest -- 2027+) and
    Cassandra enables it, the same SBBF data structure unlocks
    ~5x more. PR #1 is laying that foundation.

## What's needed to file the CEP

In rough order:

### 1. End-to-end cassandra-stress bench (biggest credibility add)

This is the single most useful thing to do before filing. The
microbench numbers we have are solid; what's missing is whether
they translate to user-visible improvements in a real cluster.

  - 3-node cluster, classical vs SBBF on identical hardware
  - cassandra-stress read workload, capture p50/p95/p99 read latency
  - Test both narrow-fan (LCS, ~3 SSTables/read) and wide-fan
    (STCS, ~20 SSTables/read)
  - Replace the CEP's interpolated claim with measured cluster
    numbers
  - Expect the answer to be small. That's still a useful data
    point: better to discover during your own bench than during
    CEP discussion.

Effort: 1-2 days for someone familiar with cassandra-stress.

### 2. JDK 11 / JDK 17 parity bench

Only JDK 21 was benched here. Cassandra supports 11, 17, 21.
`VarHandle` performance and `Math.multiplyHigh` intrinsic
availability differ across JDK 11 vs 21. A regression on JDK 11
would be a hard blocker.

Effort: 1 day.

### 3. CEP draft refresh

The existing CEP draft is in `quickbloom @ quickbloom-java`.
Changes needed:

  - **Author field** -- currently "TBD". Needs a real human (you,
    or a co-author).
  - **Numbers** -- replace the 2.13-4.11x claims (those compared
    against a re-implementation of Cassandra's bloom, not trunk)
    with the measured 1.96-2.91x against actual trunk.
  - **p99 framing** -- drop the "5-25% read p99 improvement"
    interpolation; replace with the cassandra-stress measurement
    (or remove the claim entirely if you don't run the stress
    bench).
  - **Rejected alternatives**: add Binary Fuse Filter (xorfuse)
    explicitly. Per quickbloom's own data, BFF/xorfuse uses ~9
    bits/key (vs SBBF's ~21) but probes 3 cache lines per contains
    (vs SBBF's 1), so it loses on query latency. SBBF is the
    right choice for the SSTable workload where every SELECT pays
    per-probe latency.
  - **Decomposition data**: the off-heap vs on-heap analysis (off-
    heap costs ~0.2-0.5x in cache-resident tiers, nothing in DRAM,
    not worth abandoning the OffHeapBitSet pattern). Reviewers
    will ask.
  - **PR followups** explicitly listed (dtests, upgradesstables
    test, etc., see below).

### 4. Find a committer sponsor (the real gate)

This is the political layer. Without an Apache Cassandra committer
willing to engage with the CEP on the mailing list, the CEP can
sit dormant indefinitely. Pre-pitching to one or two committers
before filing, getting their +1 or preliminary feedback, is what
experienced contributors do.

Plausible candidates:

  - Anyone working on storage-engine performance
  - DataStax engineers (many committers, would benefit Astra
    directly since Astra already ships JVector with the flag)
  - Read-path maintainers

Without a sponsor: expect to repeat the case many times. With
one: 3-6 months from CEP filing to merge is the normal Cassandra
cadence for this kind of structural optimization.

### 5. File CEP on dev@cassandra.apache.org

Once 1-4 are in shape:

  - Subject: `[CEP] CEP-XX: SBBF Bloom filter for SSTable filtering`
  - Body: the refreshed CEP draft
  - Reference this branch as the implementation
  - Expect 1-3 months of mailing-list discussion
  - Be prepared to defend the format-version bump cost and the
    FP-rate-at-iso-memory semantic change (see "Risks" below)

## What's needed for the PR after CEP approval

Once the CEP is approved, the PR work that's not yet done:

  - **JIRA ticket** (`CASSANDRA-XXXXX`), referenced in commit
    message and `CHANGES.txt`. Update both before opening the PR.
  - **Mixed-cluster upgrade dtest**: three-node cluster, upgrade
    one node to the new format, verify reads / streaming / repair
    work cross-version. This is the standard Cassandra dtest
    pattern for format-version bumps.
  - **`upgradesstables` test**: verify operators can rewrite
    classical-format SSTables to SBBF format incrementally and
    that read behaviour is unchanged after rewrite.
  - **`perfasm` audit**: confirm JIT-emitted assembly matches
    expectations. Requires a host with `perf` and
    `kernel.perf_event_paranoid <= 1`. Not runnable in this
    sandbox.
  - **Open PR against `apache/cassandra`** with the JIRA ticket.
    Branch can be cherry-picked from `dmatth1/cassandra @
    claude/port-quickbloom-cassandra-S4kLX`.

## Risks / open questions

These will come up during CEP discussion. Have answers ready:

  - **"Is 2.3x microbench enough to justify a format-version
    bump?"** -- The format bump is well-trodden (oa -> ob style).
    Operators get SBBF incrementally as compaction churns; no
    forced rewrite. So the cost to operators is essentially zero.
    But reviewers may still push for explicit measurement of the
    end-to-end win.
  - **"Why a new hash family?"** -- Wyhash is faster than Murmur3
    on the SBBF probe path (~3-5 ns vs ~12-25 ns on 16-byte keys),
    and the SBBF probe doesn't need Murmur3's 128-bit output.
    Counter-argument: another hash family to maintain, test,
    review. Possible compromise: ship Wyhash as Path 2 separately
    from SBBF as Path 1. The CEP draft already discusses this.
  - **"FP-rate at iso-memory is a behaviour change."** SBBF K=8
    gives ~0.4% actual FP for a 1% target, vs classical's ~1%
    actual. Strictly better for users but operators have been
    setting `bloom_filter_fp_chance=0.01` for years and getting
    1%. Worth documenting clearly in NEWS.txt (already done in
    this PR).
  - **"Why not Binary Fuse Filter / Ribbon Filter?"** -- See
    "rejected alternatives" above. BFF loses on query latency
    (3 cache lines vs SBBF's 1). Ribbon is more complex to
    build and not significantly better than SBBF on the metrics
    Cassandra cares about. SBBF wins for this workload.
  - **"Why not Vector API now?"** Apache Cassandra's default
    `jvm-*-server.options` files don't pass `--add-modules
    jdk.incubator.vector`, even though JVector is in trunk for SAI.
    Adding the SBBF Vector API path is gated on either flipping
    that flag (a separate political discussion) or doing the
    MRJAR-style dispatch that mirrors JVector. The CEP draft
    proposes ship-scalar-first; Vector API as a follow-up CEP
    once it graduates.
  - **"Why off-heap instead of on-heap int[]?"** Matches the
    OffHeapBitSet pattern that classical BloomFilter uses for
    accounting and tear-down. On-heap would be marginally faster
    on cache-resident tiers (decomposition bench showed
    ~0.2-0.5x) but loses the OffHeapBitSet accounting consistency.
    Not worth the deviation.

## Why this may not be worth pursuing

Honest reasons to walk away:

  1. **The production impact is small.** Low-single-digit p99
     improvement on wide-fan reads, imperceptible on typical
     reads. Users won't notice.
  2. **CEP process is heavy.** 3-6 months minimum from filing
     to merge, longer without a committer sponsor. The
     opportunity cost of that time is real.
  3. **The committers are busy.** Accord and CMS are absorbing
     review attention. A performance optimization that doesn't
     address an active pain point can languish.
  4. **Bloom is not currently a pain point.** Operator complaints
     about Cassandra are about correctness, repair, compaction,
     and operational ergonomics -- not bloom filter latency.

If you decide not to push this through, the work isn't wasted:
the branch stays as a reference implementation, the analysis is
in this doc, and anyone who picks it up later has the full
context. The right thing might just be to leave it here.
