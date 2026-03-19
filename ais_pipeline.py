import csv
import io
import os
import gc
import time
import logging
import multiprocessing as mp
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

# ----------------------------------------------
# Configuration
# ----------------------------------------------

COL_MMSI      = "MMSI"
COL_TIMESTAMP = "# Timestamp"
COL_LAT       = "Latitude"
COL_LON       = "Longitude"
COL_SOG       = "SOG"
COL_DRAUGHT   = "Draught"
COL_SHIP_TYPE = "Ship type"
COL_NAME      = "Name"
COL_DEST      = "Destination"

INVALID_MMSI_EXACT: set[int] = {
    0, 111111111, 123456789, 222222222, 999999999,
}
MMSI_MIN    = 200_000_000
MMSI_MAX    = 999_999_999

NUM_SHARDS  = 32
CHUNK_SIZES = [10_000, 50_000, 100_000]
NUM_WORKERS = min(12, os.cpu_count() or 1)
WORK_DIR    = "work_mmsi_parts"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ----------------------------------------------
# Validation
# ----------------------------------------------

def mmsi_valid(m: int) -> bool:
    if m in INVALID_MMSI_EXACT:
        return False
    return MMSI_MIN <= m <= MMSI_MAX


def coord_valid(lat: float, lon: float) -> bool:
    if lat == 0.0 and lon == 0.0:
        return False
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0


def parse_ts(s: str) -> Optional[int]:
    """'DD/MM/YYYY HH:MM:SS' -> UTC epoch seconds. None on failure."""
    try:
        dt = datetime.strptime(s.strip(), "%d/%m/%Y %H:%M:%S")
        return int(dt.replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return None


def parse_row_to_tsv(row: dict) -> Optional[tuple[int, str]]:
    """
    Validate a raw CSV row and return (mmsi_int, tsv_line) or None.
    TSV format: mmsi\tts_epoch\tlat\tlon\tsog\tdraught\tship_type\tname\tdest
    """
    try:
        m = int(row.get(COL_MMSI, "").strip())
    except (ValueError, AttributeError):
        return None
    if not mmsi_valid(m):
        return None

    ts = parse_ts(row.get(COL_TIMESTAMP, ""))
    if ts is None:
        return None

    try:
        lat = float(row[COL_LAT])
        lon = float(row[COL_LON])
    except (ValueError, KeyError):
        return None
    if not coord_valid(lat, lon):
        return None

    try:
        sog = float(row.get(COL_SOG) or 0)
    except ValueError:
        sog = 0.0
    try:
        draught = float(row.get(COL_DRAUGHT) or 0)
    except ValueError:
        draught = 0.0

    ship_type = row.get(COL_SHIP_TYPE, "").strip().replace("\t", " ")
    name      = row.get(COL_NAME,      "").strip().replace("\t", " ")
    dest      = row.get(COL_DEST,      "").strip().replace("\t", " ")

    tsv = (
        f"{m}\t{ts}\t{lat:.6f}\t{lon:.6f}\t"
        f"{sog:.3f}\t{draught:.3f}\t{ship_type}\t{name}\t{dest}\n"
    )
    return (m, tsv)


# ----------------------------------------------
# Task 1 – File segmentation
# ----------------------------------------------

def get_header(filepath: str) -> str:
    with open(filepath, encoding="utf-8", errors="replace") as fh:
        return fh.readline()


def compute_segments(filepath: str, num_segments: int) -> list[tuple[int, int]]:
    """Split file into num_segments byte ranges snapped to newline boundaries."""
    file_size   = os.path.getsize(filepath)
    approx_size = file_size // num_segments
    segments    = []
    with open(filepath, "rb") as fh:
        start = 0
        for i in range(num_segments):
            if i == num_segments - 1:
                segments.append((start, file_size))
                break
            fh.seek(start + approx_size)
            fh.readline()
            end = fh.tell()
            segments.append((start, end))
            start = end
    return segments


# ----------------------------------------------
# Task 2 – Worker: segment -> chunk loop -> disk flush
# ----------------------------------------------

def worker_segment_to_disk(args: tuple) -> list[str]:
    """
    MAP worker. Reads its byte-range segment in fixed chunks, writes valid
    rows to TSV shard files keyed by (mmsi % num_shards). Returns non-empty paths.
    """
    worker_id, filepath, start, end, header, chunk_size, work_dir, num_shards = args
    FLUSH_LINES = 50_000

    os.makedirs(work_dir, exist_ok=True)

    shard_paths = [
        os.path.join(work_dir, f"worker_{worker_id:03d}_part_{s:03d}.tsv")
        for s in range(num_shards)
    ]
    shard_files = [open(p, "w", encoding="utf-8", newline="") for p in shard_paths]
    shard_bufs: list[list[str]] = [[] for _ in range(num_shards)]
    shard_seen: list[set]       = [set() for _ in range(num_shards)]

    def flush_shard(s: int) -> None:
        if shard_bufs[s]:
            shard_files[s].write("".join(shard_bufs[s]))
            shard_bufs[s].clear()
            shard_seen[s].clear()

    try:
        with open(filepath, "rb") as raw_fh:
            raw_fh.seek(start)
            if start > 0:
                raw_fh.readline()

            current_pos = raw_fh.tell()

            while current_pos < end:
                raw_lines: list[bytes] = []
                raw_fh.seek(current_pos)
                for _ in range(chunk_size):
                    if raw_fh.tell() >= end:
                        break
                    line = raw_fh.readline()
                    if not line:
                        break
                    raw_lines.append(line)

                current_pos = raw_fh.tell()
                if not raw_lines:
                    break

                chunk_text = header.encode() + b"".join(raw_lines)
                reader = csv.DictReader(
                    io.StringIO(chunk_text.decode("utf-8", errors="replace"))
                )
                for raw_row in reader:
                    result = parse_row_to_tsv(raw_row)
                    if result is None:
                        continue
                    m, tsv_line = result
                    sid   = m % num_shards
                    parts = tsv_line.split("\t", 4)
                    dedup_key = hash((
                        parts[0], parts[1],
                        parts[2] if len(parts) > 2 else "",
                        parts[3] if len(parts) > 3 else "",
                    ))
                    if dedup_key in shard_seen[sid]:
                        continue
                    shard_seen[sid].add(dedup_key)
                    shard_bufs[sid].append(tsv_line)
                    if len(shard_bufs[sid]) >= FLUSH_LINES:
                        flush_shard(sid)

                del raw_lines, chunk_text
                gc.collect()

    finally:
        for s in range(num_shards):
            flush_shard(s)
            shard_files[s].close()

    return [p for p in shard_paths if os.path.getsize(p) > 0]


# ----------------------------------------------
# Shard merge
# ----------------------------------------------

def _sort_dedup_and_write(
    source_paths: list[str],
    out_path:     str,
    shard_id:     int,
) -> None:
    """
    Read source_paths, sort by (mmsi, ts), deduplicate on (mmsi, ts, lat, lon),
    write raw unmodified rows. No metadata enrichment - preserves anomaly signal.
    """
    rows: list[tuple[str, int, str]] = []

    for path in source_paths:
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                parts = line.split("\t", 2)
                if len(parts) < 2:
                    continue
                try:
                    mmsi = parts[0]
                    ts   = int(parts[1])
                except ValueError:
                    continue
                rows.append((mmsi, ts, line))

    rows.sort(key=lambda r: (r[0], r[1]))

    if not rows:
        return

    deduped_count = 0
    with open(out_path, "w", encoding="utf-8") as out_fh:
        prev_key = None
        for mmsi, ts, line in rows:
            parts     = line.split("\t", 3)
            lat_s     = parts[2] if len(parts) > 2 else ""
            lon_s     = parts[3].split("\t")[0] if len(parts) > 3 else ""
            dedup_key = (mmsi, ts, lat_s, lon_s)
            if dedup_key == prev_key:
                continue
            prev_key = dedup_key
            out_fh.write(line)
            deduped_count += 1

    log.info("  shard %03d -> %s (%d rows, from %d source files)",
             shard_id, out_path, deduped_count, len(source_paths))


def _merge_one_worker_shard(args: tuple) -> str:
    """Worker function: merge one shard group and return the output path."""
    sid, source_paths, out_dir = args
    out_path = os.path.join(out_dir, f"shard_{sid:03d}.tsv")
    _sort_dedup_and_write(source_paths, out_path, sid)
    return out_path


def merge_worker_shards(
    all_worker_paths: list[str],
    num_shards:       int,
    out_dir:          str,
) -> list[str]:
    """
    Merge worker_WWW_part_SSS.tsv -> shard_SSS.tsv in parallel.
    Each shard group is independent.
    """
    os.makedirs(out_dir, exist_ok=True)

    shard_groups: dict[int, list[str]] = defaultdict(list)
    for path in all_worker_paths:
        basename = os.path.basename(path)
        try:
            sid = int(basename.split("_part_")[1].split(".")[0])
        except (IndexError, ValueError):
            continue
        shard_groups[sid].append(path)

    merge_args = [
        (sid, shard_groups[sid], out_dir)
        for sid in sorted(shard_groups)
    ]

    with mp.Pool(processes=min(NUM_WORKERS, len(merge_args))) as pool:
        final_paths = pool.map(_merge_one_worker_shard, merge_args)

    return [p for p in final_paths if os.path.exists(p)]


def _merge_one_final_shard(args: tuple) -> str | None:
    """Worker function: merge one final shard from multiple per-file dirs."""
    sid, per_file_dirs, out_dir = args
    shard_name = f"shard_{sid:03d}.tsv"
    out_path   = os.path.join(out_dir, shard_name)
    sources    = [
        os.path.join(d, shard_name)
        for d in per_file_dirs
        if os.path.exists(os.path.join(d, shard_name))
    ]
    if not sources:
        return None
    _sort_dedup_and_write(sources, out_path, sid)
    return out_path


def merge_final_shards(
    per_file_dirs: list[str],
    num_shards:    int,
    out_dir:       str,
) -> list[str]:
    """Merge shard_NNN.tsv files from multiple per-file dirs in parallel."""
    os.makedirs(out_dir, exist_ok=True)

    merge_args = [
        (sid, per_file_dirs, out_dir)
        for sid in range(num_shards)
    ]

    with mp.Pool(processes=min(NUM_WORKERS, num_shards)) as pool:
        results = pool.map(_merge_one_final_shard, merge_args)

    final_paths = [p for p in results if p is not None]
    log.info("Combined %d per-file dirs -> %d shards in %s",
             len(per_file_dirs), len(final_paths), out_dir)
    return final_paths


def cleanup_dir(work_dir: str) -> None:
    """Remove all TSV files in work_dir then the directory itself."""
    if not os.path.isdir(work_dir):
        return
    for f in os.listdir(work_dir):
        if f.endswith(".tsv"):
            try:
                os.remove(os.path.join(work_dir, f))
            except OSError:
                pass
    try:
        os.rmdir(work_dir)
    except OSError:
        pass


# ----------------------------------------------
# Single-file pipeline
# ----------------------------------------------

def run_pipeline(
    filepath:       str,
    chunk_size:     int = 100_000,
    num_workers:    int = NUM_WORKERS,
    shards_out_dir: str = WORK_DIR,
) -> tuple[float, list[str]]:
    """
    Process one CSV file -> 32 shard_NNN.tsv files on disk.
    Returns (elapsed_seconds, shard_paths).
    Shards are always kept - ais_anomalies.py reads them directly.
    """
    t0 = time.perf_counter()
    log.info("Pipeline | workers=%d | chunk_size=%d | shards=%d",
             num_workers, chunk_size, NUM_SHARDS)

    header   = get_header(filepath)
    segments = compute_segments(filepath, num_workers)

    worker_work_dir = os.path.join(shards_out_dir, "worker_parts")
    worker_args = [
        (i, filepath, start, end, header, chunk_size, worker_work_dir, NUM_SHARDS)
        for i, (start, end) in enumerate(segments)
    ]

    t2 = time.perf_counter()
    with mp.Pool(processes=num_workers) as pool:
        shard_path_lists = pool.map(worker_segment_to_disk, worker_args)
    log.info("Phase 2 (parallel write) done in %.1f s", time.perf_counter() - t2)

    all_worker_paths = [p for paths in shard_path_lists for p in paths]

    t2b = time.perf_counter()
    log.info("Merging worker shards into %d final shards …", NUM_SHARDS)
    final_shard_paths = merge_worker_shards(
        all_worker_paths, NUM_SHARDS, shards_out_dir
    )
    log.info("Shard merge done in %.1f s", time.perf_counter() - t2b)

    cleanup_dir(worker_work_dir)

    elapsed = time.perf_counter() - t0
    log.info("Pipeline complete: %.2f s | %d shards", elapsed, len(final_shard_paths))
    return elapsed, final_shard_paths

# ----------------------------------------------
# Multi-file pipeline
# ----------------------------------------------

def run_pipeline_multi(
    filepaths:      list[str],
    chunk_size:     int = 100_000,
    num_workers:    int = NUM_WORKERS,
    shards_out_dir: str = WORK_DIR,
) -> tuple[float, list[str]]:
    """
    Process multiple CSV files into one shared shard set.
    Returns (elapsed_seconds, shard_paths).
    Each file is processed into its own subdir then merged together,
    so vessel timelines that cross midnight are correctly unified.
    """
    if len(filepaths) == 1:
        return run_pipeline(
            filepaths[0],
            chunk_size=chunk_size,
            num_workers=num_workers,
            shards_out_dir=shards_out_dir,
        )

    t0 = time.perf_counter()
    log.info("Multi-file pipeline: %d files -> %s", len(filepaths), shards_out_dir)

    per_file_dirs: list[str] = []

    for idx, filepath in enumerate(filepaths):
        log.info("-- File %d/%d: %s --", idx+1, len(filepaths), filepath)
        file_shard_dir = os.path.join(shards_out_dir, f"file_{idx:03d}")
        run_pipeline(
            filepath,
            chunk_size=chunk_size,
            num_workers=num_workers,
            shards_out_dir=file_shard_dir,
        )
        per_file_dirs.append(file_shard_dir)

    log.info("Combining %d per-file shard sets …", len(per_file_dirs))
    final_paths = merge_final_shards(per_file_dirs, NUM_SHARDS, shards_out_dir)

    for d in per_file_dirs:
        cleanup_dir(d)

    elapsed = time.perf_counter() - t0
    log.info("Multi-file pipeline complete: %.2f s | %d shards",
             elapsed, len(final_paths))
    return elapsed, final_paths


# ----------------------------------------------
# Entry point
# ----------------------------------------------

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python ais_pipeline.py <csv1> [csv2 ...] [chunk_size] "
              "[--shards-dir=DIR]")
        sys.exit(1)

    flags      = [a for a in sys.argv[1:] if a.startswith("--")]
    positional = [a for a in sys.argv[1:] if not a.startswith("--")]

    csv_files  = [a for a in positional if a.endswith(".csv")]
    non_csv    = [a for a in positional if not a.endswith(".csv")]
    chunk_size = int(non_csv[0]) if non_csv else 100_000

    if not csv_files:
        print("ERROR: no .csv files specified")
        sys.exit(1)

    shards_dir = WORK_DIR
    for f in flags:
        if f.startswith("--shards-dir="):
            shards_dir = f.split("=", 1)[1]

    elapsed, shard_paths = run_pipeline_multi(
        csv_files,
        chunk_size=chunk_size,
        shards_out_dir=shards_dir,
    )

    print(f"\n{'='*52}")
    print(f"  Files processed : {len(csv_files)}")
    print(f"  Time            : {elapsed:.2f} s")
    print(f"  Shards saved to : {shards_dir}/")
    print(f"{'='*52}\n")