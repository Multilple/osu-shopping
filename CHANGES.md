# Changes by Team Wix

Relative to Team 67's Express Lane at commit `1cda738`:

- Added `README.md`, `THIRD_PARTY_NOTICES.md` and this file.
- Removed the upstream `AGENTS.md` and `CLAUDE.md` and the trailing notice comments (comments only; no code changed).
- `src/v3b.rs`: the default cap on event partitions is 2^10 instead of 2^12
  (the default of the `XL_V3M1` setting). Inputs up to about 20M events are
  unaffected, because they already used 2^10. At 60M events the parser fills
  1024 event partitions instead of 2048, which measured about 2% faster on the
  scoring hardware (1074 ms against 1096 ms, median of 4 interleaved runs).
  The answer is unchanged: checked against an independent pipeline on a 60M
  event dataset.
