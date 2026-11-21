#!/bin/bash
# cgp.sh [ENV=..]... : callgrind on dev with one thread; prints the kernel's own
# instructions and simulated mispredictions per event line (events_dense and
# everything it calls, the plain code for the lines it leaves included).
BIN=${BIN:-/home/claude/ss/target-p/release/ss}
LINES=${LINES:-1072086}
rm -rf /tmp/pcg && mkdir /tmp/pcg
env "$@" NCPU=1 valgrind --tool=callgrind --branch-sim=yes --dump-instr=yes --collect-atstart=no \
  --toggle-collect='*dense_ev*events_dense*' --callgrind-out-file=/tmp/p_cg.out \
  $BIN /home/claude/ss/data/dev /tmp/pcg 2>&1 | grep -E "Collected" | \
  awk -v n=$LINES '{printf "Ir %.1fM  Bc %.2fM  Bcm %.3fM  Bi %.2fM Bim %.3fM | per line: Ir %.1f  Bcm+Bim %.3f  cost(Ir+20*(Bcm+Bim)) %.1f\n", $4/1e6, $5/1e6, $6/1e6, $7/1e6, $8/1e6, $4/n, ($6+$8)/n, ($4+20*($6+$8))/n}'
