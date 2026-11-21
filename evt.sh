#!/bin/bash
# evt.sh N DATA [ENV=..]... : the kernel's own CPU time (SS_EVTIME), N runs each of the
# assembly kernel and the Rust one, interleaved; prints min and median ms.
N=$1; D=$2; shift 2
BIN=${BIN:-/home/claude/ss/target-p/release/ss}
for i in $(seq $N); do for k in asm rs; do rm -rf /tmp/evt_o && mkdir /tmp/evt_o
  echo "$k $(env "$@" SS_KERNEL=$k SS_EVTIME=1 NCPU=${NCPU:-1} $BIN /home/claude/ss/data/$D /tmp/evt_o 2>&1 | grep evtime | tail -1 | awk '{print $2}')"
done; done | python3 -c "
import sys,statistics,collections
v=collections.defaultdict(list)
for l in sys.stdin:
    p=l.split()
    if len(p)==2: v[p[0]].append(float(p[1]))
for k in sorted(v): print(f'{k}: min {min(v[k]):.1f} ms  med {statistics.median(v[k]):.1f} ms  n={len(v[k])}')
"
