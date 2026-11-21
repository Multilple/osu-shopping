# Click kernel notes (fork C2)

- Kernel: `src/dense_ck.rs` (`clicks_dense`), called from `dense::parse_clicks`; per-run tables in `Dn::ckx`
  (`dense_ck::Cx`, built after `ctx.aux` is known), per-thread date memo in `DOut::ckd`.
- `SS_CKVERIFY=1`: every line the kernel takes is compared with the plain code's record (panics on a difference).
- Measure: `/home/claude/ss/cgfun.sh /home/claude/ss/target-c/release/ss` and sum `dense_ck::*`, `click_record`,
  the interning under it; divide by 255,280 dev click lines.

| commit | what | Ir per click line (kernel / all-in) |
|---|---|---|
| step 1 | fork C's table-driven kernel ported, newline check fixed | 196.6 / 215 |
