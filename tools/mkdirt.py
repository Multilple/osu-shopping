#!/usr/bin/env python3
# mkdirt.py SRC DST [seed]: a data set like SRC (same lines, same files) whose event
# files carry generator-like dirt (bad timestamp forms, bad types and prices,
# case variants, odd IDs, malformed lines) and random byte damage (UTF8=1:
# none that makes a string unreadable), at several
# rates and quoting levels. Everything but the events is linked from SRC.
import os, random, sys, json, glob, datetime

src, dst = sys.argv[1], sys.argv[2]
seed = int(sys.argv[3]) if len(sys.argv) > 3 else 1
per_file = int(sys.argv[4]) if len(sys.argv) > 4 else 1
rng = random.Random(seed)

def parse_utc(s):
    return int(datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)

man = json.load(open(os.path.join(src, "manifest.json")))
ws, we = parse_utc(man["window_start"]), parse_utc(man["window_end"])

def civil(ms):
    t = datetime.datetime.fromtimestamp(ms // 1000, datetime.timezone.utc)
    return t.year, t.month, t.day, t.hour, t.minute, t.second

def utc(ms, sep="T"):
    try:
        y, m, d, hh, mm, ss = civil(ms)
    except (OverflowError, ValueError, OSError):
        return "9999-12-31%s23:59:59" % sep
    return "%04d-%02d-%02d%s%02d:%02d:%02d" % (y, m, d, sep, hh, mm, ss)

def with_offset(ms, minutes):
    local = ms + minutes * 60000
    sign = "-" if minutes < 0 else "+"
    return "%s%s%02d:%02d" % (utc(local), sign, abs(minutes) // 60, abs(minutes) % 60)

BAD_TYPES = ["Purchase", "PURCHASE", "View", "cart ", "click", "refund", "", "views", "VIEW", "car", "purchas", "purchase ", "viewx", "cartview", "vie", "CART"]
PRICES = ["19.99", "-5", "+500", "0", "", "10000001", "250000000", "1e3", "00", "10000000", "1", "0250", "2918", "351", "99999999", "000000000001", "0000000010000000", "1 ", " 1", "12a"]
OFFSETS = [-600, -480, -300, -210, 0, 60, 330, 540, 720, 345, -570, 1439, -1439]

def ts_field(ms):
    secs = ms - ms % 1000
    y, m, d, hh, mm, ss = civil(secs)
    k = rng.randrange(40)
    if k <= 5: return utc(secs) + "Z"
    if k <= 8: return with_offset(secs, rng.choice(OFFSETS))
    if k <= 13: return str(ms)
    if k == 14: return ""
    if k == 15: return "%s.%03dZ" % (utc(secs), ms % 1000)
    if k == 16: return utc(secs) + "z"
    if k == 17: return utc(secs, " ") + "Z"
    if k == 18: return utc(secs)
    if k == 19: return "%d.0" % ms
    if k == 20: return "%d.%02de12" % (ms // 10**12, ms // 10**10 % 100)
    if k == 21: return "-%d" % (1 + rng.randrange(9))
    if k == 22: return "%04d-13-%02dT%02d:%02d:%02dZ" % (y, d, hh, mm, ss)
    if k == 23: return "%04d-02-30T%02d:%02d:%02dZ" % (y, hh, mm, ss)
    if k == 24: return "%04d-%02d-%02dT24:%02d:%02dZ" % (y, m, d, mm, ss)
    if k == 25: return "%04d-%02d-%02dT%02d:%02d:60Z" % (y, m, d, hh, mm)
    if k == 26: return utc(secs, "t") + "Z"
    if k == 27: return "%04d-02-29T%02d:%02d:%02dZ" % (y + 1 if y % 4 == 0 else y, hh, mm, ss)
    if k == 28:
        t = rng.choice([ws - 1000, ws, ws + 1000, we - 1000, we, we + 1000, ws - 86400000 * 3, we + 86400000 * 2, ws - 1, we - 1, ws + 1])
        j = rng.randrange(3)
        if j == 0: return utc(t - t % 1000) + "Z"
        if j == 1: return with_offset(t - t % 1000, rng.choice(OFFSETS))
        return str(t)
    if k == 29: return str(ms // 10)
    if k == 30: return "000%d" % ms
    if k == 31: return utc(secs) + "+24:00"
    if k == 32: return utc(secs) + "-01:60"
    if k == 33: return (utc(secs) + "Z").replace(":", ".")
    if k == 34: return "%020d" % ms                      # 20 digits: a number all the same
    if k == 35: return "%025d" % ms                      # 25 digits
    if k == 36: return utc(secs) + rng.choice(["+0:00", "+00:0", "+00-00", " 00:00", "+0a:00", "-00:0x", "+00:00Z", "Z+00:00"])
    if k == 37: return "%04d-%02d-00T%02d:%02d:%02dZ" % (y, m, hh, mm, ss)
    if k == 38: return "%04d-00-%02dT%02d:%02d:%02dZ" % (y, d, hh, mm, ss)
    return rng.choice(["%013d" % (ms // 1000), "0" * 13, "9" * 13, "1" * 18, "1" * 19, "%sZ" % utc(secs).replace("-", "/"), utc(secs)[:-1] + "xZ", "1829024a43000"])

def ingest_field(ms):
    secs = ms - ms % 1000
    ceiling = ms if secs == ms else secs + 1000
    k = rng.randrange(26)
    if k == 0: t = ms + 86400000
    elif k == 1: t = secs + 86400000
    elif k == 2: t = secs + 86401000
    elif k == 3: t = ceiling + 86400000
    elif k == 4: t = ceiling + 86401000 + 1000 * rng.randrange(400000)
    elif k == 5: t = secs - 1000 * (1 + rng.randrange(5))
    elif k == 6: return ""
    elif k == 7: return with_offset(secs, 60)
    elif k == 8: return str(ms)
    elif k == 9: return utc(secs)
    elif k == 10: return utc(secs) + "z"
    elif k == 11: t = -2208988800000 + 1000 * rng.randrange(1000)
    elif k == 12: t = 253402300799000
    elif k == 13: return "2027-02-30T10:00:00Z"
    elif k == 14: return "2027-12-19T24:00:00Z"
    elif k == 15: return "0000-01-01T00:00:00Z"
    elif k == 16: t = 4102444800000 + 86400000 * 366 * (40 + rng.randrange(40))
    elif k == 17: return rng.choice(["1969-12-31T23:59:59Z", "1970-01-01T00:00:00Z", "2038-01-19T03:14:08Z", "2100-02-29T00:00:00Z", "2096-02-29T12:00:00Z", "1900-01-01T00:00:00Z", "9999-12-31T23:59:59Z"])
    elif k == 18: return utc(secs) + rng.choice(["", "ZZ", " Z", "Y", "+00:00"])
    else: t = ceiling + 1000 * rng.randrange(600)
    return utc(t) + "Z"

def b36(v):
    s = ""
    while True:
        s = "0123456789abcdefghijklmnopqrstuvwxyz"[v % 36] + s
        v //= 36
        if v == 0: return s

files = sorted(glob.glob(os.path.join(src, "events", "*.csv")))
biggest = max(files, key=os.path.getsize)

def fields_of(line):
    parts = line.rstrip(b"\n").decode("latin-1").split(",")
    if len(parts) != 7: return None
    return [p[1:-1] if len(p) >= 2 and p[0] == '"' and p[-1] == '"' else p for p in parts]

# UTF8=1 also keeps the answer defined: copies of an event with the same ingest
# time must be identical, so lines of events that have copies stay as they are
# and no ID is used twice.
answer = bool(os.environ.get("UTF8"))
copies = set()
if answer:
    seen = set()
    for path in files:
        with open(path, "rb") as fh:
            for line in fh:
                e = line.split(b",", 1)[0].strip(b'"')
                (copies if e in seen else seen).add(e)
    del seen
used = set()

def fresh(v, old):
    """`v`, or `old` if `v` was given out before (when the answer must stay defined)."""
    if not answer: return v
    if v in used or v.encode("latin-1") in copies: return old
    used.add(v)
    return v

# A pool of real IDs to swap in.
rows = []
with open(files[0], "rb") as fh:
    for i, line in enumerate(fh):
        if i == 0: continue
        f = fields_of(line)
        if f: rows.append(f)
        if len(rows) >= 20000: break

def ms_of(ts):
    try:
        if len(ts) == 13 and ts.isdigit(): return int(ts)
        if len(ts) == 20: return parse_utc(ts)
        if len(ts) == 25:
            base = parse_utc(ts[:19] + "Z")
            off = (int(ts[20:22]) * 60 + int(ts[23:25])) * 60000
            return base - off if ts[19] == "+" else base + off
    except ValueError:
        pass
    return None

ALPHABET = list(b"\",\n\"0123456789abcdefghijklmnopqrstuvwxyzEUPZT:-+. \r\x00\xc3\"\",,")
if os.environ.get("UTF8"):
    # (so that the answers can be read back and compared)
    ALPHABET = [c for c in ALPHABET if 0 < c < 128]
os.makedirs(os.path.join(dst, "events"), exist_ok=True)
for name in os.listdir(src):
    if name != "events" and not os.path.lexists(os.path.join(dst, name)):
        os.symlink(os.path.abspath(os.path.join(src, name)), os.path.join(dst, name))
for old in glob.glob(os.path.join(dst, "events", "*.csv")):
    os.remove(old)

# BYTES=1: no field dirt; half the lines get one or two single-byte edits.
bytes_only = bool(os.environ.get("BYTES"))
EDIT = list(b'",,,"""\n\n0123456789abcdefghijklmnopqrstuvwxyz0019-:TZ+--::  \r\x00/.zZtTeupP')
if answer:
    EDIT = [c for c in EDIT if 0 < c < 128]
plans = [(0.0, 0.02, 0.0), (0.2, 0.02, 0.0), (0.2, 0.2, 0.01), (0.5, 0.2, 0.0), (1.0, 0.02, 0.0), (0.2, 0.6, 0.1), (0.5, 0.05, 0.01), (0.0, 0.2, 0.0), None, None]
total = dirty = 0
for fi, path in enumerate(files):
    plan = plans[rng.randrange(len(plans))]
    if path == biggest:
        plan = (0.2, 0.02, 0.0)       # the file the keys are taken from stays mostly clean
    buf = bytearray()
    with open(path, "rb") as fh:
        lines = fh.readlines()
    if plan is None or per_file == 0:
        buf += b"".join(lines)
        total += len(lines) - 1
    else:
        pq, field_dirt, byte_dirt = plan
        buf += lines[0]
        for ln, line in enumerate(lines[1:]):
            f = fields_of(line)
            ms = ms_of(f[2]) if f else None
            # (the head of the biggest file is what the keys are recovered from)
            if ms is None or (path == biggest and ln < 4000) or rng.random() > 0.5 or (answer and f[0].encode("latin-1") in copies):
                buf += line if line.endswith(b"\n") else line + b"\n"
                total += 1
                continue
            dirty += 1
            if bytes_only:
                # One or two edits anywhere in the line as it is, structure bytes favoured.
                b = bytearray(line.rstrip(b"\n"))
                for _ in range(1 if rng.random() < 0.8 else 2):
                    if not b: break
                    pos = rng.randrange(len(b))
                    c = rng.choice(EDIT)
                    k = rng.randrange(4)
                    if k <= 1: b[pos] = c
                    elif k == 2: b.insert(pos, c)
                    else: del b[pos]
                if b.count(b"\n") == 0 or rng.random() < 0.5:
                    buf += b + b"\n"
                    total += 1
                    continue
                buf += b + b"\n"
                total += 1
                continue
            if rng.random() < 0.3: ms += rng.randrange(1000)
            if rng.random() < 0.5: f[2] = ts_field(ms) if rng.random() < max(field_dirt, 0.05) * 3 else rng.choice([utc(ms - ms % 1000) + "Z", with_offset(ms - ms % 1000, rng.choice(OFFSETS)), str(ms)])
            if rng.random() < field_dirt: f[3] = ingest_field(ms)
            if rng.random() < field_dirt: f[4] = rng.choice(BAD_TYPES)
            elif rng.random() < 0.3: f[4] = rng.choice(["view", "cart", "purchase"])
            if rng.random() < 0.4: f[6] = rng.choice(PRICES) if rng.random() < max(field_dirt, 0.1) else str(1 + rng.randrange(10000000))
            if rng.random() < field_dirt:
                f[5] = rng.choice(["", "P%d" % (1 + rng.randrange(50000)), "p%d" % (50001 + rng.randrange(49999)), "p0", "p0%d" % rng.randrange(1000), "p%d" % rng.randrange(10), "p", "p123456789012345678", "p12x", "p%d" % (4194300 + rng.randrange(10)), "p99999999", "q%d" % rng.randrange(100)])
            if rng.random() < field_dirt:
                u = f[1]
                f[1] = rng.choice(["", u.upper(), "uzzzzzzzz", "u0" + u[1:8], "u", "x" + u[1:], u + "0", "u0", "u%s" % b36(rng.randrange(36 * 36)), "u" + b36(rng.randrange(1 << 40)), "u" + b36((1 << 40) + rng.randrange(1 << 30)), "ue9s2vt1v", "u" + "z" * (1 + rng.randrange(12)), rng.choice(rows)[1], "u" + b36(rng.randrange(36 ** 7))])
            if rng.random() < field_dirt:
                e = f[0]
                f[0] = fresh(rng.choice(["", e.upper(), "ezzzzzzzz", "e0" + e[1:8], "e", "u" + e[1:], e + "0", "e0", "e" + b36(rng.randrange(36 ** 3)), "e" + b36(rng.randrange(1 << 40)), "e" + b36((1 << 40) + rng.randrange(1 << 30)), "ee9s2vt1v", "e" + "y" * (1 + rng.randrange(12)), rng.choice(rows)[0], "e" + b36(rng.randrange(36 ** 7))]), e)
            at = len(buf)
            for i, v in enumerate(f):
                if i: buf += b","
                q = rng.random() < pq
                if q: buf += b'"'
                buf += v.encode("latin-1")
                if q: buf += b'"'
            if rng.random() < field_dirt * 0.2:
                k = rng.randrange(11)
                if k == 0:
                    last = buf.rfind(b",")
                    if last >= at: del buf[last:]
                elif k == 1: buf += b",x"
                elif k == 2: buf.insert(at, 0x22)
                elif k == 3: buf += b'"'
                elif k == 4:
                    first = buf.find(b",", at)
                    del buf[at:first]
                elif k == 5: buf += b"\n"
                elif k == 6: buf += b"\r"
                elif k == 7: buf += b",,,,,,,,,,,,"
                elif k == 8: buf += b"x" * rng.randrange(200)
                elif k == 9:
                    pos = at + rng.randrange(max(1, len(buf) - at))
                    buf.insert(pos, 0x22)
                else:
                    qs = [i for i in range(at, len(buf)) if buf[i] == 0x22]
                    if qs: del buf[rng.choice(qs)]
            if byte_dirt > 0 and rng.random() < byte_dirt * 5:
                for _ in range(1 + rng.randrange(3)):
                    n = len(buf) - at
                    if n == 0: break
                    pos = at + rng.randrange(n)
                    c = rng.choice(ALPHABET)
                    k = rng.randrange(3)
                    if k == 0: buf[pos] = c
                    elif k == 1: buf.insert(pos, c)
                    else: del buf[pos]
            buf += b"\n"
            total += 1
        if fi % 3 == 2 and buf.endswith(b"\n"):
            buf.pop()  # the last line has no newline
    with open(os.path.join(dst, "events", os.path.basename(path)), "wb") as out:
        out.write(buf)
print("wrote", total, "lines,", dirty, "of them remade, in", len(files), "files to", dst)
