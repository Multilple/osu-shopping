#!/bin/bash
# vp.sh [quick]: checks of the kernel build in target-p (SS_KERNEL=rs ./vp.sh for the Rust kernel).
#  dev against the kit's answer; the kernel's records against the plain code's (SS_EVCHECK)
#  on dev, on sets with dirt in the events (tools/mkdirt.py: generator-like dirt in pdirt1..3,
#  single-byte edits in pdirt4..5; tools/mkedge.py: made-up edge lines in pedge1..2) and on
#  seeds; seeds and a dirt set against SS_DENSE=0.
cd /home/claude/ss
BIN=${BIN:-/home/claude/ss/target-p/release/ss}
K=kit/starter-duckdb/starter-duckdb
cmp() { python3 $K/scripts/compare.py "$1" "$2" | tail -1; }
ev() { rm -rf /tmp/vpe && mkdir /tmp/vpe && timeout 600 env SS_EVCHECK=1 NCPU=${2:-2} $BIN data/$1 /tmp/vpe 2>&1 | grep evcheck | tail -1; }
rm -rf /tmp/vpo && mkdir /tmp/vpo && NCPU=2 $BIN data/dev /tmp/vpo >/dev/null 2>&1 && echo "dev vs kit answer: $(cmp /tmp/vpo kit/dev-answer/dev-answer)"
for d in dev pdirt1 pdirt5 pedge1; do echo "evcheck $d: $(ev $d)"; done
echo "evcheck pdirt2 (1 thread): $(ev pdirt2 1)"
[ "$1" = quick ] && exit
for d in pdirt3 pdirt4 pedge2 t2 t5; do echo "evcheck $d: $(ev $d)"; done
for t in t2 t5 t2028 pdirt3; do
  rm -rf /tmp/vpo /tmp/vps && mkdir /tmp/vpo /tmp/vps && timeout 300 env NCPU=2 $BIN data/$t /tmp/vpo >/dev/null 2>&1; timeout 300 env SS_DENSE=0 NCPU=2 $BIN data/$t /tmp/vps >/dev/null 2>&1; echo "$t vs SS_DENSE=0: $(cmp /tmp/vpo /tmp/vps)"
done
rm -rf /tmp/vpo && mkdir /tmp/vpo && NCPU=2 $BIN data/s7_20m /tmp/vpo >/dev/null 2>&1 && echo "20M vs reference: $(cmp /tmp/vpo out20)"
