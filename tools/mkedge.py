#!/usr/bin/env python3
# mkedge.py SRC DST [seed] [lines]: SRC's data set plus one event file of made-up lines
# that walk the kernel's edges: every quoting of every field, ID lengths, line and tail
# lengths around the windows' limits, times at the window's and the day's edges, dates at
# month and leap edges, prices and types of every kind.
import os, random, sys, json, glob, datetime
src, dst = sys.argv[1], sys.argv[2]
seed = int(sys.argv[3]) if len(sys.argv) > 3 else 1
count = int(sys.argv[4]) if len(sys.argv) > 4 else 300000
rng = random.Random(seed)
def parse_utc(s):
    return int(datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
man = json.load(open(os.path.join(src, "manifest.json")))
ws, we = parse_utc(man["window_start"]), parse_utc(man["window_end"])
def utc(ms):
    t = datetime.datetime.fromtimestamp(ms // 1000, datetime.timezone.utc)
    return "%04d-%02d-%02dT%02d:%02d:%02d" % (t.year, t.month, t.day, t.hour, t.minute, t.second)
def with_offset(ms, minutes):
    sign = "-" if minutes < 0 else "+"
    return "%s%s%02d:%02d" % (utc(ms + minutes * 60000), sign, abs(minutes) // 60, abs(minutes) % 60)
files = sorted(glob.glob(os.path.join(src, "events", "*.csv")))
eids, uids = [], []
with open(files[0], "rb") as fh:
    for i, line in enumerate(fh):
        if i == 0: continue
        p = line.decode("latin-1").split(",")
        if len(p) == 7:
            eids.append(p[0].strip('"')); uids.append(p[1].strip('"'))
        if len(eids) >= 60000: break
B36 = "0123456789abcdefghijklmnopqrstuvwxyz"
def b36(v):
    s = ""
    while True:
        s = B36[v % 36] + s; v //= 36
        if v == 0: return s
DAY = 86400000
def an_id(prefix, pool):
    k = rng.randrange(20)
    if k < 12: return rng.choice(pool)
    if k == 12: return prefix + b36(rng.randrange(36 ** rng.randrange(1, 9)))          # any length
    if k == 13: return prefix + "0" + b36(rng.randrange(36 ** rng.randrange(1, 8)))    # leading zero
    if k == 14: return prefix + b36((1 << 40) - 1 - rng.randrange(3))                  # just below 2^40
    if k == 15: return prefix + b36((1 << 40) + rng.randrange(3))                      # just above
    if k == 16: return prefix + b36(36 ** 8 - 1 - rng.randrange(1000))                 # the largest 8 digits
    if k == 17: return prefix + B36[rng.randrange(36)]                                 # one digit
    if k == 18: return rng.choice(pool)[:-1] + rng.choice("AZ_-. {|`/:@[")             # a byte next to the digits and letters
    return prefix + b36(rng.randrange(36 ** 9))                                        # nine digits
def a_ts():
    k = rng.randrange(40)
    edge = rng.choice([ws - 1000, ws, ws + 1000, we - 1000, we, we + 1000, ws + rng.randrange(we - ws)])
    t = edge if k < 8 else ws + rng.randrange(we - ws)
    t -= t % 1000
    if k % 4 == 0: return str(t + rng.choice([0, 0, 1, 999, rng.randrange(1000)])), t
    if k % 4 == 1: return with_offset(t, rng.choice([-720, -600, -570, -1, 0, 1, 60, 330, 345, 840, 1439, -1439])), t
    if k == 2: return utc(t)[:10] + "T24:00:00Z", t
    if k == 3: return utc(t)[:17] + "60Z", t
    if k == 6: return utc(t)[:14] + "60:00Z", t
    if k == 7: return "%s-02-%02dT12:00:00Z" % (rng.choice(["2027", "2028", "2100", "2000", "1900"]), rng.choice([28, 29, 30])), t
    if k == 10: return "%s-%02d-%02dT00:00:00Z" % (utc(t)[:4], rng.randrange(0, 14), rng.randrange(0, 33)), t
    if k == 11: return utc(t)[:19] + rng.choice(["z", "", " ", "+00:00Z", "+0000", "+24:00", "-00:60", "+1:000", "Y"]), t
    if k == 14: return "%0*d" % (rng.choice([12, 14, 18, 19, 20, 25]), t), t
    if k == 15: return utc(t).replace("T", rng.choice(" t")) + "Z", t
    return utc(t) + "Z", t
def an_ingest(t):
    k = rng.randrange(30)
    if k == 0: return utc(t + DAY) + "Z"
    if k == 1: return utc(t + DAY + 1000) + "Z"
    if k == 2: return utc(t + DAY - 1000) + "Z"
    if k == 3: return utc(t - 1000 * rng.randrange(1, 100000)) + "Z"
    if k == 4: return utc(t + 1000 * rng.randrange(0, 2 * 86400)) + "Z"
    if k == 5: return rng.choice(["1970-01-01T00:00:00Z", "1969-12-31T23:59:59Z", "2099-12-31T23:59:59Z", "2100-01-01T00:00:00Z", "0000-01-01T00:00:00Z", "9999-12-31T23:59:59Z", "2028-02-29T00:00:00Z", "2027-02-29T00:00:00Z"])
    if k == 6: return utc(t) + rng.choice(["", "z", "ZZ", "+00:00"])
    if k == 7: return str(t)
    if k == 8: return utc(t)[:11] + rng.choice(["24:00:00", "23:60:00", "23:59:60", "2:00:000", "aa:bb:cc"]) + "Z"
    return utc(t + 1000 * rng.randrange(0, 900)) + "Z"
TYPES = ["view"] * 8 + ["cart"] * 6 + ["purchase"] * 4 + ["View", "CART", "purchase ", "viewx", "vie", "", "cartt", "purchas", "views", "refund"]
def a_product():
    k = rng.randrange(16)
    if k == 0: return ""
    if k == 1: return "p" + "1" * rng.randrange(1, 24)       # long: the tail's length walks past 32
    if k == 2: return "p"
    return "p%d" % rng.randrange(1, 50000)
def a_price():
    k = rng.randrange(16)
    if k < 6: return ""
    if k == 6: return rng.choice(["0", "00", "10000000", "10000001", "099", "1e3", "-5", "+5", "1.5", " 1", "1 ", "000000001", "0000000010000000", "99999999", "100000000"])
    if k == 7: return "1" * rng.randrange(1, 20)
    return str(rng.randrange(1, 10000001))
os.makedirs(os.path.join(dst, "events"), exist_ok=True)
for name in os.listdir(src):
    if name != "events" and not os.path.lexists(os.path.join(dst, name)):
        os.symlink(os.path.abspath(os.path.join(src, name)), os.path.join(dst, name))
for f in files:
    link = os.path.join(dst, "events", os.path.basename(f))
    if not os.path.lexists(link): os.symlink(os.path.abspath(f), link)
# Files smaller than the source's biggest (the dense plan reads the head of the biggest).
per = max(1000, int(max(os.path.getsize(f) for f in files) * 0.8 / 90))
lens = {}
done = 0
part = 0
while done < count:
    buf = bytearray(b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n")
    for _ in range(min(per, count - done)):
        ts, t = a_ts()
        f = [an_id("e", eids), an_id("u", uids), ts, an_ingest(t), rng.choice(TYPES), a_product(), a_price()]
        qmask = rng.choice([0, 0, rng.randrange(128), rng.randrange(128), 127])
        at = len(buf)
        for i, v in enumerate(f):
            if i: buf += b","
            if qmask >> i & 1: buf += b'"' + v.encode("latin-1") + b'"'
            else: buf += v.encode("latin-1")
        k = rng.randrange(200)
        if k == 0: buf += b"\r"
        elif k == 1: buf.insert(at + rng.randrange(len(buf) - at + 1), 0x22)
        elif k == 2: buf.insert(at + rng.randrange(len(buf) - at + 1), 0x2C)
        elif k == 3:
            qs = [i for i in range(at, len(buf)) if buf[i] == 0x22]
            if qs: del buf[rng.choice(qs)]
        elif k == 4: buf.insert(at + rng.randrange(len(buf) - at + 1), 0x0A)
        lens[len(buf) - at] = lens.get(len(buf) - at, 0) + 1
        buf += b"\n"
        done += 1
    if (seed + part) % 2: buf.pop()
    open(os.path.join(dst, "events", "part-9%04d.csv" % part), "wb").write(buf)
    part += 1
print("wrote", count, "lines in", part, "files; lengths", min(lens), "to", max(lens), "; at 40..42:", [lens.get(k, 0) for k in (40, 41, 42)], "at 103..106:", [lens.get(k, 0) for k in (103, 104, 105, 106)])
