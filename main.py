import csv
from datetime import datetime, timezone
from typing import Optional, Iterator, Tuple

def build_idx_from_header(header: list[str]) -> dict[str, int]:
    names = [c.strip() for c in header]
    pos = {name: i for i, name in enumerate(names)}

    required = ["# Timestamp", "MMSI", "Latitude", "Longitude"]
    missing = [c for c in required if c not in pos]
    if missing:
        raise ValueError(f"Missing columns: {missing}\nHeader: {names}")

    idx = {
        "ts": pos["# Timestamp"],
        "mmsi": pos["MMSI"],
        "lat": pos["Latitude"],
        "lon": pos["Longitude"],
    }

    if "SOG" in pos:
        idx["sog"] = pos["SOG"]
    if "Draught" in pos:
        idx["draught"] = pos["Draught"]

    return idx

INVALID_MMSI = {
    0,
    111111111,
    123456789,
    222222222,
    999999999,
}

def safe_int(s: str) -> Optional[int]:
    try:
        return int(s)
    except Exception:
        return None

def safe_float(s: str) -> Optional[float]:
    try:
        return float(s)
    except Exception:
        return None

def parse_ts_utc_seconds(s: str) -> Optional[int]:
    if not s:
        return None
    s = s.strip()
    if not s:
        return None

    try:
        dt = datetime.strptime(s, "%d/%m/%Y %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None

def mmsi_valid(m: Optional[int]) -> bool:
    if m is None:
        return False
    if m in INVALID_MMSI:
        return False
    if m < 100000000 or m > 999999999:
        return False
    mid = m // 1_000_000
    if mid < 200 or mid > 799:
        return False
    if m % 111111111 == 0:
        return False
    return True

def coord_valid(lat: Optional[float], lon: Optional[float]) -> bool:
    if lat is None or lon is None:
        return False
    if lat == 0.0 and lon == 0.0:
        return False
    if lat < -90.0 or lat > 90.0:
        return False
    if lon < -180.0 or lon > 180.0:
        return False
    return True

Row = Tuple[int, int, float, float, Optional[float], Optional[float]]
# (mmsi, ts_epoch_s, lat, lon, sog, draught)

def iter_rows_csv(path: str) -> Iterator[Row]:
    with open(path, "rt", encoding="utf-8", newline="") as f:
        r = csv.reader(f)

        header = next(r)
        idx = build_idx_from_header(header)
        for row in r:
            try:
                m = safe_int(row[idx["mmsi"]])
                if not mmsi_valid(m):
                    continue

                ts = parse_ts_utc_seconds(row[idx["ts"]])
                if ts is None:
                    continue

                lat = safe_float(row[idx["lat"]])
                lon = safe_float(row[idx["lon"]])
                if not coord_valid(lat, lon):
                    continue

                sog = safe_float(row[idx["sog"]]) if "sog" in idx else None
                dr = safe_float(row[idx["draught"]]) if "draught" in idx else None

                yield (m, ts, float(lat), float(lon), sog, dr)
            except Exception:
                continue
            
import os
from typing import Optional

OUT_DIR = "work_mmsi_parts"
PARTS = 64            
FLUSH_LINES = 200_000

os.makedirs(OUT_DIR, exist_ok=True)

def partition_by_mmsi_csv(
    path: str,
    out_dir: str,
    parts: int = 64,
    flush_lines: int = 200_000,
) -> None:
    """
    Reads one CSV stream.
    Writes TSV shard files:
      mmsi  ts_epoch  lat  lon  sog  draught

    Shard id = mmsi % parts
    Small in-memory buffers per shard
    Flush buffers to disk in batches
    """

    os.makedirs(out_dir, exist_ok=True)

    fps = [
        open(os.path.join(out_dir, f"part_{i:03d}.tsv"), "wt", encoding="utf-8", newline="")
        for i in range(parts)
    ]
    bufs = [[] for _ in range(parts)]

    def flush(i: int) -> None:
        if not bufs[i]:
            return
        fps[i].write("".join(bufs[i]))
        bufs[i].clear()

    n_in = 0
    n_out = 0

    try:
        for (m, ts, lat, lon, sog, dr) in iter_rows_csv(path):
            n_in += 1

            pid = m % parts

            sog_s = "" if sog is None else f"{sog:.3f}"
            dr_s = "" if dr is None else f"{dr:.3f}"

            bufs[pid].append(f"{m}\t{ts}\t{lat:.6f}\t{lon:.6f}\t{sog_s}\t{dr_s}\n")
            n_out += 1

            if len(bufs[pid]) >= flush_lines:
                flush(pid)

            # Periodic flush across all shards for long runs
            if n_in % 5_000_000 == 0:
                for i in range(parts):
                    flush(i)

    finally:
        for i in range(parts):
            flush(i)
        for f in fps:
            f.close()

    print("input_rows_streamed=", n_in)
    print("output_rows_written=", n_out)
    print("shards=", parts)
    print("out_dir=", out_dir)
    

path = "Datasets/aisdk-2026-01-24.csv"

if __name__ == "__main__":
    partition_by_mmsi_csv(path, OUT_DIR, parts=PARTS, flush_lines=FLUSH_LINES)