#!/usr/bin/env python3
# mca.py [FILE.rs] [cpu ...]: the assembly fast path of dense_ka.rs (or of FILE.rs)
# as llvm-mca sees it: simulated cycles per line on a few processors, with the
# glue the compiler puts around the block. A proxy for real time when the
# machine is too busy to measure.
import re, subprocess, sys
src = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1].endswith(".rs") else "/home/claude/ss/wt-p/src/dense_ka.rs"
cpus = [a for a in sys.argv[1:] if not a.endswith(".rs")] or ["sapphirerapids", "skylake-avx512", "znver3"]
text = open(src).read()
a = text.index("asm!(")
b = text.index('".pushsection', a)
lines = re.findall(r'^\s*"([^"\\]*(?:\\.[^"\\]*)*)",', text[a:b], re.M)
out = [".intel_syntax noprefix", ".Lx:", "prefetcht0 byte ptr [rbp + 4096]", "prefetcht0 byte ptr [rbp + 4160]", "mov rdi, rbx", "mov rsi, rbp"]
k = 0
for l in lines:
    l = l.replace('\\"', '"')
    if re.fullmatch(r"\d+:", l): continue
    l = re.sub(r"\b(j\w+)\s+\d+[fb]", r"\1 .Lx", l)
    def off(m):
        global k
        k += 1
        return str(64 + 32 * (k % 60))
    # A constant read through rdi is ready as soon as the pass starts: for the
    # simulator, which charges a load's latency on every path through the
    # instruction, it is a register that nothing writes (rbx, ymm12).
    l = re.sub(r"ymmword ptr \[rdi \+ \{\w+\}\]", "ymm12", l)
    l = re.sub(r"xmmword ptr \[rdi \+ \{\w+\}\]", "xmm12", l)
    l = re.sub(r"qword ptr \[rdi \+ \{\w+\}\]", "rbx", l)
    l = re.sub(r"dword ptr \[rdi \+ \{\w+\}\]", "ebx", l)
    l = re.sub(r"\{\w+\}", off, l)
    out.append(l)
out += ["mov r14, rsi", "test rax, rax", "jne .Lx", "mov rax, rbx", "mov rsi, qword ptr [rax + 48]", "mov rax, qword ptr [rsi + 8*r8]",
        "test eax, 16383", "je .Lx", "mov qword ptr [rax], rdx", "mov qword ptr [rax + 8], rcx", "add rax, 16", "mov qword ptr [rsi + 8*r8], rax", "mov rbp, r14",
        "cmp r14, rbx", "jb .Lx"]
open("/tmp/mca_in.s", "w").write("\n".join(out) + "\n")
print(f"{len(out) - 2} instructions per line")
for cpu in cpus:
    r = subprocess.run(["llvm-mca", f"-mcpu={cpu}", "-iterations=300", "/tmp/mca_in.s"], capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stderr[:2000]); sys.exit(1)
    cyc = int(re.search(r"Total Cycles:\s+(\d+)", r.stdout).group(1)) / 300
    rt = re.search(r"Block RThroughput:\s+([\d.]+)", r.stdout).group(1)
    pr = re.search(r"Resource pressure per iteration:\n.*\n(.*)\n", r.stdout).group(1).split()
    print(f"{cpu:16s} {cyc:6.1f} cycles per line   (port bound {rt})")
