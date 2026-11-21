#!/usr/bin/env python3
# mcatl.py [cpu] [iter]: for /tmp/mca_in.s (made by mca.py), when each instruction of one
# steady-state iteration is dispatched, starts and ends executing (cycles from the iteration's first dispatch).
import re, subprocess, sys
cpu = sys.argv[1] if len(sys.argv) > 1 else "sapphirerapids"
it = int(sys.argv[2]) if len(sys.argv) > 2 else 12
r = subprocess.run(["llvm-mca", f"-mcpu={cpu}", f"-iterations={it+3}", "-timeline", f"-timeline-max-iterations={it+2}", "-timeline-max-cycles=4000", "/tmp/mca_in.s"], capture_output=True, text=True)
rows = []
for l in r.stdout.split("\n"):
    m = re.match(r"\[(\d+),(\d+)\](.*)$", l)
    if m and int(m.group(1)) in (it, it + 1):
        tl = m.group(3)
        d = tl.index("D"); rr = tl.index("R", d); seg = tl[d:rr + 1]
        e1 = d + seg.index("E"); e0 = d + (seg.index("e") if "e" in seg else seg.index("E"))
        rows.append((int(m.group(1)), int(m.group(2)), d, e0, e1, rr, tl[rr + 1:].lstrip(" .-")))
base = min(x[2] for x in rows if x[0] == it)
nxt = min(x[2] for x in rows if x[0] == it + 1)
print(f"iteration {it}: next iteration's first dispatch at +{nxt - base}")
for i, k, d, e0, e1, rr, ins in rows:
    if i == it:
        print(f"{k:3d}  disp {d - base:3d}  exec {e0 - base:3d}..{e1 - base:3d}  ret {rr - base:3d}  wait {e0 - d:3d}  {ins}")
