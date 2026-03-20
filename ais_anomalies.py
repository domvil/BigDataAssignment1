"""
Anomalies detected
------------------
A  Going Dark       AIS gap > 4 h where distance implies ship kept moving
B  Loitering        Two vessels < 500 m apart, SOG < 1 kn, for > 2 h
C  Draft Change     Draught changes > 5 % during AIS blackout > 2 h
D  Identity Cloning Same MMSI pings from locations requiring speed > 60 kn

DFSI formula (from assignment image)
--------------------------------------
DFSI = (max_gap_hours / 2) + (total_impossible_distance_nm / 10) + (C x 1.5)
"""

import os
import gc
import csv
import math
import heapq
import argparse
import logging
import multiprocessing as mp
from collections import defaultdict
from itertools import combinations
from typing import Iterator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

from config import (
    RESULTS_DIR, NUM_WORKERS,
    GAP_HOURS, MOVING_SPEED_MIN,
    LOITER_SPEED_MAX, LOITER_HOURS_MIN, LOITER_DIST_M,
    DRAFT_CHANGE_PCT, DRAFT_BLACKOUT_HRS,
    CLONE_SPEED_KN,
)


# ----------------------------------------------
# Geometry helpers
# ----------------------------------------------

from helper import haversine_nm, haversine_m, implied_speed_kn


# ----------------------------------------------
# Shard reading - streams one vessel at a time
# ----------------------------------------------

def iter_vessels_from_shard(shard_path: str) -> Iterator[tuple[str, list[dict]]]:
    """
    Yield (mmsi, sorted_pings) for each vessel in a shard file.
    The shard is already sorted by timestamp (from pipeline reduce phase),
    so we just group consecutive rows by MMSI.

    Memory: holds one vessel's rows at a time.
    """
    current_mmsi  = None
    current_rows: list[dict] = []

    with open(shard_path, encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 6:
                continue
            try:
                mmsi    = parts[0]
                ts      = int(parts[1])
                lat     = float(parts[2])
                lon     = float(parts[3])
                sog     = float(parts[4]) if parts[4] else 0.0
                draught = float(parts[5]) if parts[5] else 0.0
            except (ValueError, IndexError):
                continue

            row = {"mmsi": mmsi, "ts": ts, "lat": lat,
                   "lon": lon, "sog": sog, "draught": draught}

            if mmsi != current_mmsi:
                if current_mmsi is not None and current_rows:
                    yield current_mmsi, current_rows
                current_mmsi = mmsi
                current_rows = [row]
            else:
                current_rows.append(row)

    if current_mmsi is not None and current_rows:
        yield current_mmsi, current_rows


# ----------------------------------------------
# Anomaly A - Going Dark
# ----------------------------------------------

def detect_A(mmsi: str, pings: list[dict]) -> list[dict]:
    """
    Find gaps > GAP_HOURS where the implied speed between last seen and
    reappearance is > MOVING_SPEED_MIN (ship was moving, not anchored).

    Also flags if the vessel reappears more than 1 nm from where it vanished,
    even if the implied average speed looks low - this catches slow drift
    across a long gap that still implies movement.
    """
    events = []
    for i in range(1, len(pings)):
        p0, p1 = pings[i-1], pings[i]
        dt_h = (p1["ts"] - p0["ts"]) / 3600.0
        if dt_h < GAP_HOURS:
            continue
        dist_nm = haversine_nm(p0["lat"], p0["lon"], p1["lat"], p1["lon"])
        spd = dist_nm / dt_h if dt_h > 0 else 0.0
        # Flag if moved more than 1 nm OR implied speed > threshold
        if dist_nm < 1.0 and spd < MOVING_SPEED_MIN:
            continue
        events.append({
            "mmsi":       mmsi,
            "gap_hours":  round(dt_h, 2),
            "dist_nm":    round(dist_nm, 2),
            "implied_kn": round(spd, 2),
            "lat_before": p0["lat"], "lon_before": p0["lon"],
            "ts_before":  p0["ts"],
            "lat_after":  p1["lat"], "lon_after":  p1["lon"],
            "ts_after":   p1["ts"],
        })
    return events


# ----------------------------------------------
# Anomaly C - Draft Change at Sea
# ----------------------------------------------

def detect_C(mmsi: str, pings: list[dict]) -> list[dict]:
    """
    Find pairs where draught is valid (> 0.5 m) on both sides of an AIS
    blackout > DRAFT_BLACKOUT_HRS and the change exceeds DRAFT_CHANGE_PCT.
    Uses 0.5 m minimum to filter out transponders that report 0.0 as default.
    """
    events = []
    for i in range(1, len(pings)):
        p0, p1 = pings[i-1], pings[i]
        d0, d1 = p0["draught"], p1["draught"]
        # Require meaningful draught values on both sides
        if d0 < 0.5 or d1 < 0.5:
            continue
        dt_h = (p1["ts"] - p0["ts"]) / 3600.0
        if dt_h < DRAFT_BLACKOUT_HRS:
            continue
        change = abs(d1 - d0) / d0
        if change < DRAFT_CHANGE_PCT:
            continue
        events.append({
            "mmsi":           mmsi,
            "gap_hours":      round(dt_h, 2),
            "draught_before": d0,
            "draught_after":  d1,
            "change_pct":     round(change * 100, 2),
            "ts_before":      p0["ts"],
            "ts_after":       p1["ts"],
            "lat":            p0["lat"],
            "lon":            p0["lon"],
        })
    return events


# ----------------------------------------------
# Anomaly D - Identity Cloning / Teleportation
# ----------------------------------------------

def detect_D(mmsi: str, pings: list[dict]) -> list[dict]:
    """
    Identity cloning detection.
    Find consecutive pings with implied speed > CLONE_SPEED_KN.
    """
    events = []
 
    # ── Stage 1: speed violations ──────────────────────────────
    in_run = False
    best   = None
    
    for i in range(1, len(pings)):
        p0, p1 = pings[i-1], pings[i]
 
        # Same timestamp, different coordinates after deduplication.
        # Two possibilities:
        #   1. Coordinate noise: two base stations received the same broadcast
        #      and decoded the GPS with slightly different floating point precision.
        #      Typical separation: < 0.1 nm (185 m) — same physical location.
        #   2. Genuine cloning: two ships broadcasting the same MMSI simultaneously
        #      from genuinely different locations. Separation: >> 1 nm.
        #
        # Threshold: 0.5 nm (925 m). Below this = base station noise, skip.
        # Above this = two ships cannot be at those positions simultaneously.
        if p1["ts"] == p0["ts"]:
            same_pos = (p0["lat"] == p1["lat"] and p0["lon"] == p1["lon"])
            if not same_pos:
                dist_nm = haversine_nm(p0["lat"], p0["lon"], p1["lat"], p1["lon"])
                if dist_nm >= 0.5:   # genuine separation, not receiver noise
                    events.append({
                        "mmsi":            mmsi,
                        "event_type":      "simultaneous_transmission",
                        "speed_kn":        float("inf"),
                        "dist_nm":         round(dist_nm, 2),
                        "dt_seconds":      0,
                        "reported_sog":    round((p0["sog"] + p1["sog"]) / 2.0, 2),
                        "lat1": p0["lat"], "lon1": p0["lon"], "ts1": p0["ts"],
                        "lat2": p1["lat"], "lon2": p1["lon"], "ts2": p1["ts"],
                        "gps_error":       False,
                        "cluster_dist_nm": None,
                    })
            continue
 
        spd = implied_speed_kn(p0["lat"], p0["lon"], p0["ts"],
                               p1["lat"], p1["lon"], p1["ts"])
        if spd >= CLONE_SPEED_KN:
            dist_nm = haversine_nm(p0["lat"], p0["lon"], p1["lat"], p1["lon"])
            avg_sog = (p0["sog"] + p1["sog"]) / 2.0
 
            candidate = {
                "mmsi":         mmsi,
                "event_type":   "speed_violation",
                "speed_kn":     round(spd, 1),
                "dist_nm":      round(dist_nm, 2),
                "dt_seconds":   p1["ts"] - p0["ts"],
                "reported_sog": round(avg_sog, 2),
                "lat1": p0["lat"], "lon1": p0["lon"], "ts1": p0["ts"],
                "lat2": p1["lat"], "lon2": p1["lon"], "ts2": p1["ts"],
                "gps_error":    False,
                "cluster_dist_nm": None,
            }
            # Keep the jump with the largest dist_nm — this drives DFSI scoring
            if not in_run or dist_nm > best["dist_nm"]:
                best = candidate
            in_run = True
        else:
            if in_run and best is not None:
                events.append(best)
            in_run = False
            best   = None
 
    if in_run and best is not None:
        events.append(best)
 
    return events

# ----------------------------------------------
# Worker: process one shard for A, C, D
# ----------------------------------------------

def worker_shard_ACD(shard_path: str) -> dict:
    """
    Runs on one shard file. Returns all A/C/D events found,
    plus per-vessel stats needed for DFSI.
    Loads one vessel at a time - no full shard in RAM.
    """
    events_A, events_C, events_D = [], [], []
    vessel_stats: dict[str, dict] = {}

    for mmsi, pings in iter_vessels_from_shard(shard_path):
        # Sort by timestamp just in case shard wasn't fully sorted
        pings.sort(key=lambda r: r["ts"])

        a = detect_A(mmsi, pings)
        c = detect_C(mmsi, pings)
        d = detect_D(mmsi, pings)

        events_A.extend(a)
        events_C.extend(c)
        events_D.extend(d)

        # Collect per-vessel stats for DFSI
        # Only count D events that are NOT GPS errors in the impossible
        # distance total. simultaneous_transmission events always count
        # (gps_error=False) and use dist_nm for scoring.
        max_gap = max((e["gap_hours"] for e in a), default=0.0)
        total_impossible_nm = sum(
            e["dist_nm"] for e in d
            if not e.get("gps_error", False) and e.get("dist_nm") is not None
        )
        count_C = len(c)

        if a or c or d:
            vessel_stats[mmsi] = {
                "max_gap_hours":         max_gap,
                "total_impossible_nm":   total_impossible_nm,
                "count_C":               count_C,
            }

    return {
        "A": events_A,
        "C": events_C,
        "D": events_D,
        "stats": vessel_stats,
    }


# ----------------------------------------------
# Anomaly B - Loitering / Ship-to-Ship Transfer
# ----------------------------------------------

def extract_loiter_candidates(shard_path: str) -> list[dict]:
    """
    Pass 1: scan one shard, find vessels with sustained low speed.
 
    A loiter window is a CONSECUTIVE run of slow pings (SOG < threshold)
    with no fast ping interrupting the run. The moment a fast ping appears
    the run ends, even if the vessel slows down again later.
    """
    candidates = []
 
    for mmsi, pings in iter_vessels_from_shard(shard_path):
        pings.sort(key=lambda r: r["ts"])
 
        current_run: list[dict] = []
 
        def _finalise_run(run: list[dict], fast_ping: dict | None) -> None:
            """
            Finalise a slow run and optionally extend the time window using
            the first fast ping that ended the run.
 
            Why include the fast ping's timestamp:
                A vessel that was slow 10:00-10:55, went dark, and reappeared
                at 14:00 doing 10 kn has a run ending at 10:55. Without the
                fast ping, the candidate window is 10:00-10:55 - too short or
                exactly 2h - and the actual loiter period during the gap is
                invisible to Pass 2's time-overlap check.
                Including the fast ping extends ts_end to 14:00, allowing
                Pass 2 to find other vessels whose slow windows overlap that
                broader period.
 
            Why NOT include the fast ping's position in avg_lat/lon:
                The vessel went dark, but it was moving at that point. Its reappearance location
                is where it ended up, not where it was during the slow period (we can't know for certain).
                avg_lat/lon stays anchored to the slow pings so the spatial
                join in Pass 2 reflects where the vessel actually loitered. 
                This is done also in case of GPS malfunctions.
 
            Normal case (no gap): fast_ping arrives a few minutes after the
                last slow ping at nearly the same location. ts_end extends by
                a few minutes - negligible effect on overlap detection.
            Gap case: ts_end extends by hours - unlocks detection of
                loitering-during-blackout scenarios.
            """
            if len(run) < 2:
                return
            dt_h = (run[-1]["ts"] - run[0]["ts"]) / 3600.0
            if dt_h < LOITER_HOURS_MIN:
                # Before giving up, check if including the fast ping's
                # timestamp makes the window long enough
                if fast_ping is None:
                    return
                dt_h_extended = (fast_ping["ts"] - run[0]["ts"]) / 3600.0
                if dt_h_extended < LOITER_HOURS_MIN:
                    return
                # Use the extended end time
                ts_end = fast_ping["ts"]
            else:
                ts_end = run[-1]["ts"]
 
            # Position average from slow pings only
            avg_lat = sum(p["lat"] for p in run) / len(run)
            avg_lon = sum(p["lon"] for p in run) / len(run)
            d_start = next((p["draught"] for p in run
                            if p["draught"] >= 0.5), None)
            d_end   = next((p["draught"] for p in reversed(run)
                            if p["draught"] >= 0.5), None)
            candidates.append({
                "mmsi":          mmsi,
                "ts_start":      run[0]["ts"],
                "ts_end":        ts_end,
                "avg_lat":       avg_lat,
                "avg_lon":       avg_lon,
                "duration_h":    round((ts_end - run[0]["ts"]) / 3600.0, 2),
                "draught_start": d_start,
                "draught_end":   d_end,
            })
 
        for p in pings:
            sog = p.get("sog")
            if sog is not None and sog < LOITER_SPEED_MAX:
                current_run.append(p)
            else:
                _finalise_run(current_run, fast_ping=p)
                current_run = []
 
        _finalise_run(current_run, fast_ping=None)
 
    return candidates


def _draught_confidence(c1: dict, c2: dict) -> str:
    """
    Assess draught-based evidence for a loitering pair.

    Returns one of four confidence labels:
      proximity_only    - no usable draught data for either vessel
      draught_unchanged - all available data shows no meaningful change
      draught_partial   - one vessel genuinely missing data, other changed
      draught_transfer  - one vessel lost draught, other gained it
    """
    # 5 cm absolute threshold 
    # AIS draught is reported in 0.1m steps, so anything >= 0.1m is a real
    # change. Using 0.05m gives one step of tolerance for rounding.
    ABS_THRESH = 0.05

    def direction(d_start, d_end):
        """
        Return 'loss', 'gain', 'stable', or None.
          None   = data missing or a transponder default (< 0.01 m)
          stable = data present, change below 5 cm (sensor noise / rounding)
          loss   = draught decreased by >= 5 cm (vessel got lighter)
          gain   = draught increased by >= 5 cm (vessel got heavier)
        """
        if d_start is None or d_end is None:
            return None
        if d_start < 0.01 or d_end < 0.01:
            return None
        if abs(d_end - d_start) < ABS_THRESH:
            return "stable"
        if d_end < d_start:
            return "loss"
        return "gain"

    d1 = direction(c1.get("draught_start"), c1.get("draught_end"))
    d2 = direction(c2.get("draught_start"), c2.get("draught_end"))

    # No usable data for either vessel
    if d1 is None and d2 is None:
        return "proximity_only"

    # Strongest signal: one gained, one lost
    if {d1, d2} == {"loss", "gain"}:
        return "draught_transfer"

    # One side has a confirmed change, other side is missing data
    if (d1 in ("loss", "gain") and d2 is None) or (d2 in ("loss", "gain") and d1 is None):
        return "draught_partial"

    # One side stable (zero change confirmed), other missing - no transfer evidence or both stable
    return "draught_unchanged"


def detect_B_from_candidates(candidates: list[dict]) -> list[dict]:
    """
    Pass 2: spatial join on the candidate list with draught confidence scoring.

    Each event gets a 'confidence' field:
      draught_transfer  - one vessel lighter, one heavier -> actual transfer
      draught_partial   - one vessel changed, limited data
      draught_unchanged - both vessels stable -> port / anchoring
      proximity_only    - no draught data, spatial match only
    """
    cands  = sorted(candidates, key=lambda c: c["ts_start"])
    events: dict[tuple, dict] = {}

    for i, c1 in enumerate(cands):
        for c2 in cands[i+1:]:
            if c2["ts_start"] > c1["ts_end"]:
                break
            if c1["mmsi"] == c2["mmsi"]:
                continue

            pair_key = (min(c1["mmsi"], c2["mmsi"]),
                        max(c1["mmsi"], c2["mmsi"]))

            latest_start = max(c1["ts_start"], c2["ts_start"])
            earliest_end = min(c1["ts_end"],   c2["ts_end"])
            if latest_start >= earliest_end:
                continue
            overlap_h = (earliest_end - latest_start) / 3600.0
            if overlap_h < LOITER_HOURS_MIN:
                continue

            dist_m = haversine_m(c1["avg_lat"], c1["avg_lon"],
                                 c2["avg_lat"], c2["avg_lon"])
            if dist_m > LOITER_DIST_M:
                continue

            confidence = _draught_confidence(c1, c2)

            # Draught delta values for the output
            def delta_str(c):
                ds, de = c.get("draught_start"), c.get("draught_end")
                if ds is not None and de is not None:
                    return round(de - ds, 2)
                return ""

            candidate_event = {
                "mmsi_1":         c1["mmsi"],
                "mmsi_2":         c2["mmsi"],
                "dist_m":         round(dist_m, 1),
                "overlap_h":      round(overlap_h, 2),
                "lat":            round((c1["avg_lat"] + c2["avg_lat"]) / 2, 6),
                "lon":            round((c1["avg_lon"] + c2["avg_lon"]) / 2, 6),
                "ts_start":       latest_start,
                "ts_end":         earliest_end,
                "confidence":     confidence,
                "draught_delta_1": delta_str(c1),
                "draught_delta_2": delta_str(c2),
            }
            if pair_key not in events or dist_m < events[pair_key]["dist_m"]:
                events[pair_key] = candidate_event

    return list(events.values())


# ----------------------------------------------
# DFSI calculation
# ----------------------------------------------

def compute_dfsi(stats: dict) -> float:
    """
    DFSI = (max_gap_hours / 2) + (total_impossible_distance_nm / 10) + (C x 1.5)
    """
    return (
        stats["max_gap_hours"]       / 2.0
        + stats["total_impossible_nm"] / 10.0
        + stats["count_C"]             * 1.5
    )


# ----------------------------------------------
# CSV writers
# ----------------------------------------------

def write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        log.info("No results for %s", path)
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    log.info("Wrote %d rows -> %s", len(rows), path)


# ----------------------------------------------
# Main
# ----------------------------------------------

def run_anomaly_detection(shards_dir: str, num_workers: int = NUM_WORKERS) -> None:
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Collect all merged shard files
    shard_files = sorted([
        os.path.join(shards_dir, f)
        for f in os.listdir(shards_dir)
        if f.startswith("shard_") and f.endswith(".tsv")
    ])

    if not shard_files:
        log.error("No shard TSV files found in %s", shards_dir)
        return

    log.info("Found %d shard files. Running A/C/D in parallel …", len(shard_files))

    # -- Phase A/C/D: parallel over shards --
    with mp.Pool(processes=num_workers) as pool:
        results = pool.map(worker_shard_ACD, shard_files)

    all_A: list[dict] = []
    all_C: list[dict] = []
    all_D: list[dict] = []
    vessel_stats: dict[str, dict] = {}

    for r in results:
        all_A.extend(r["A"])
        all_C.extend(r["C"])
        all_D.extend(r["D"])
        for mmsi, stats in r["stats"].items():
            if mmsi not in vessel_stats:
                vessel_stats[mmsi] = stats
            else:
                # Merge stats across shards (shouldn't happen - each MMSI is
                # in exactly one shard - but guard anyway)
                vs = vessel_stats[mmsi]
                vs["max_gap_hours"]       = max(vs["max_gap_hours"], stats["max_gap_hours"])
                vs["total_impossible_nm"] += stats["total_impossible_nm"]
                vs["count_C"]             += stats["count_C"]

    log.info("A: %d events | C: %d events | D: %d events",
             len(all_A), len(all_C), len(all_D))

    # -- Phase B: two-pass loitering detection --
    log.info("Running Anomaly B pass 1 (candidate extraction) …")
    with mp.Pool(processes=num_workers) as pool:
        candidate_lists = pool.map(extract_loiter_candidates, shard_files)

    all_candidates = [c for lst in candidate_lists for c in lst]
    log.info("Anomaly B: %d loiter candidates found", len(all_candidates))

    log.info("Running Anomaly B pass 2 (spatial join) …")
    all_B = detect_B_from_candidates(all_candidates)
    log.info("Anomaly B: %d loitering pairs found", len(all_B))

    # Add B vessels to stats (both MMSIs get flagged)
    for event in all_B:
        for key in ("mmsi_1", "mmsi_2"):
            m = event[key]
            if m not in vessel_stats:
                vessel_stats[m] = {"max_gap_hours": 0.0,
                                   "total_impossible_nm": 0.0,
                                   "count_C": 0}

    # -- DFSI --
    dfsi_rows = []
    for mmsi, stats in vessel_stats.items():
        score = compute_dfsi(stats)
        dfsi_rows.append({
            "mmsi":                 mmsi,
            "dfsi":                 round(score, 3),
            "max_gap_hours":        stats["max_gap_hours"],
            "total_impossible_nm":  stats["total_impossible_nm"],
            "count_C":              stats["count_C"],
        })

    dfsi_rows.sort(key=lambda r: r["dfsi"], reverse=True)
    top5 = dfsi_rows[:5]

    # -- Write results --
    write_csv(f"{RESULTS_DIR}/anomalies_A.csv", all_A)
    write_csv(f"{RESULTS_DIR}/anomalies_B.csv", all_B)
    write_csv(f"{RESULTS_DIR}/anomalies_C.csv", all_C)
    write_csv(f"{RESULTS_DIR}/anomalies_D.csv", all_D)
    write_csv(f"{RESULTS_DIR}/dfsi.csv",   dfsi_rows)

    log.info("--- Top 5 vessels by DFSI ---")
    for row in top5:
        log.info("  MMSI %-12s  DFSI=%.2f  gap=%.1fh  impossible=%.1fnm  C=%d",
                 row["mmsi"], row["dfsi"], row["max_gap_hours"],
                 row["total_impossible_nm"], row["count_C"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AIS anomaly detection")
    parser.add_argument("--shards-dir", default="work_mmsi_parts",
                        help="Directory containing merged shard TSV files")
    parser.add_argument("--workers",    type=int, default=NUM_WORKERS)
    args = parser.parse_args()
    run_anomaly_detection(args.shards_dir, args.workers)