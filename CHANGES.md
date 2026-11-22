# Changes by Team Wix

Relative to Team 67's Express Lane at commit `1cda738`:

- Added `README.md`, `THIRD_PARTY_NOTICES.md` and this file.
- Removed the upstream `AGENTS.md` and `CLAUDE.md` and the trailing notice comments (comments only; no code changed).
- `src/v3b.rs`: the default cap on event partitions is 2^10 instead of 2^12
  (the default of the `XL_V3M1` setting). Inputs up to about 20M events are
  unaffected, because they already used 2^10. At 60M events the parser fills
  1024 event partitions instead of 2048, which measured about 2% faster on the
  scoring hardware (1074 ms against 1096 ms, median of 4 interleaved runs).
  The answer is unchanged: checked against an independent pipeline on 60M
  event datasets (17 seeds) and by an output digest on the scoring machine.
- `src/v3b.rs`: the target size of a user partition is 768 KB instead of 384 KB
  (the default of the `XL_V3T2` setting). At about 20M events this gives 1024
  user partitions instead of 2048, which measured about 1% faster there
  (358 ms against 362 ms, median of 12 interleaved runs). At 60M events the
  count stays 2048, so that case runs exactly as before.
- `src/v3b.rs`: the dedup stage writes its output with one copy. Each user
  partition has one 64-byte slot (the `SB` type, used for both the short-ID and
  the long-ID lane); a full slot is streamed straight into the partition's
  4 KiB chunk as two 256-bit stores, so the stage still uses no 512-bit
  registers. Before, every record went through a 16 or 32 word run buffer and
  then a 24 word stage. The chunk layout and the per-user reader are unchanged.
  Measured on the scoring hardware at 60M events: 1073 ms against 1100 ms
  (median of 6 interleaved runs); the dedup stage itself 195 ms against 215 to
  234 ms. About 1% at 20M events. Output unchanged: checked against the
  independent pipeline at 5M, 20M and 60M events and by output digest.
