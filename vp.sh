#!/bin/bash
# vp.sh [quick]: checks of the kernel build in target-p.
#  dev against the kit's answer; the kernel's records against the plain code's
#  (SS_EVCHECK) on dev, on two sets with generator-like dirt in the events
#  (tools/mkdirt.py) and on a seed; seeds and dirt sets against SS_DENSE=0.
cd /home/claude/ss
BIN=${BIN:-/home/claude/ss/target-p/release/ss}
K=kit/starter-duckdb/starter-duckdb
cmp() { python3 $K/scripts/compare.py "$1" "$2" | tail -1; }
ev() { rm -rf /tmp/vpe && mkdir /tmp/vpe && timeout 300 env SS_EVCHECK=1 NCPU=${2:-2} $BIN data/$1 /tmp/vpe 2>&1 | grep evcheck | tail -1; }
rm -rf /tmp/vpo && mkdir /tmp/vpo && NCPU=2 $BIN data/dev /tmp/vpo >/dev/null 2>&1 && echo "dev vs kit answer: $(cmp /tmp/vpo kit/dev-answer/dev-answer)"
echo "evcheck dev:      $(ev dev)"
echo "evcheck pdirt1:   $(ev pdirt1)"
echo "evcheck pdirt2:   $(ev pdirt2 1)"
echo "evcheck pdirt3:   $(ev pdirt3)"
[ "$1" = quick ] && exit
echo "evcheck t2:       $(ev t2)"
for t in t2 t5 t2028 pdirt3; do
  rm -rf /tmp/vpo /tmp/vps && mkdir /tmp/vpo /tmp/vps && timeout 300 env NCPU=2 $BIN data/$t /tmp/vpo >/dev/null 2>&1; timeout 300 env SS_DENSE=0 NCPU=2 $BIN data/$t /tmp/vps >/dev/null 2>&1; echo "$t vs SS_DENSE=0: $(cmp /tmp/vpo /tmp/vps)"
done
