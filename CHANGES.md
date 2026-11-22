# Changes by Team Wix

Relative to Team 67's Express Lane at commit `3493fdd`:

- Added `README.md`, `THIRD_PARTY_NOTICES.md` and this file.
- Removed the upstream `AGENTS.md` and `CLAUDE.md` and the trailing notice comments (comments only; no code changed).
- `src/complex.rs`: the events of each shard of linked persons are put in order by one pass over their nearly sorted stretches instead of a general sort, and a long stretch of one user is handed over in bulk. Same output; `WX_CHECK=1` verifies the order against the general sort at run time, and `cargo test wx_tests` tests it. Measured on the scorer at 60M events with a whale user on this path: 611 ms to 589 ms.
