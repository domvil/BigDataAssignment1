# Maritime Shadow Fleet Detection
 
Baltic Sea AIS data processing and anomaly detection using parallel computing.
 
**Dataset:** Danish Maritime Authority AIS Data - January 24-25, 2026  
**Source:** http://web.ais.dk/aisdata/
 
---
 
## Requirements
 
```
pip install matplotlib memory_profiler
pip install global-land-mask
```
 
Python 3.11+ required.
 
---

## How to Run
 
### Step 1 - Process the raw CSV files into shards

```bash
python ais_pipeline.py <csv1> [csv2 ...] [chunk_size] [--shards-dir=DIR]
```
 
| Argument | Default | Description |
|---|---|---|
| `chunk_size` | `100000` | Rows read per worker per iteration |
| `--shards-dir` | `work_mmsi_parts` | Output directory for shard files |
 
Example call:
```bash
python ais_pipeline.py aisdk-2026-01-24.csv aisdk-2026-01-25.csv 100000
```
This produces 64 sorted TSV shard files in `work_mmsi_parts/`.

### Step 2 - Detect anomalies
 
```bash
python ais_anomalies.py --shards-dir work_mmsi_parts
```
 
Results are written to `results/`:
 
| File | Contents |
|---|---|
| `anomalies_A.csv` | Going Dark events |
| `anomalies_B.csv` | Loitering / ship-to-ship transfer pairs |
| `anomalies_C.csv` | Draft change events |
| `anomalies_D.csv` | Identity cloning events |
| `dfsi.csv` | All flagged vessels ranked by DFSI score |
 
### Step 3 - Benchmark
 
```bash
python ais_benchmark.py aisdk-2026-01-24.csv aisdk-2026-01-25.csv
```

Produces `benchmark_results.png`, `benchmark_chunk_sizes.csv`, and `benchmark_worker_counts.csv`.

### Memory profiling
 
```bash
mprof run --multiprocess python ais_pipeline.py aisdk-2026-01-24.csv aisdk-2026-01-25.csv 100000
mprof plot --output profile_pipeline.png
```
 
---


## Architecture
 
### Task 1 & 2 - Low-memory parallel partitioning (`ais_pipeline.py`)
 
The core challenge is parallelising a 2 GB CSV file without loading it into memory. The solution avoids sending raw data between processes entirely.
 
**Byte-range segmentation.** `compute_segments()` splits the file into N byte ranges by measuring file size and snapping each boundary to the nearest newline. Each worker receives only two integers (a start byte and an end byte) and opens the file itself using `seek()`.
 
**Chunk loop inside each worker.** Workers read their segment in `chunk_size` batches using Python's native `csv.DictReader`. Each batch is processed and freed before the next is read.
 
**MMSI sharding.** Each valid row is written to one of 64 shard buffer files based on `mmsi % 64`. This guarantees every ping for a given vessel always lands in the same file, so the anomaly detector never needs to look in more than one place per vessel.
 
**Dirty data filtering.** The following are silently discarded:
- MMSI not in the range 200,000,000–999,999,999
- Known invalid exact values: 0, 111111111, 123456789, 222222222, 999999999
- Coordinates at null island (0.0, 0.0) or outside valid lat/lon range
- Rows with unparseable timestamps
- Duplicate pings identified by matching `(mmsi, ts, lat, lon)`
 
Duplicates arise because multiple AIS base stations receive the same vessel broadcast simultaneously. A hash set finds duplicates for each workers own segment. Then, during merging, the data is sorted so rows from different segments end up next to each other.
 
**Parallel merge.** After all workers finish, `pool.map()` merges each shard group in parallel. Each group is sorted by `(mmsi, timestamp)` and deduplicated. Both steps, worker processing and merging, use `multiprocessing.Pool` because the job is heavy on computation, parsing, hashing, and sorting. Python threads do not speed up this kind of work much because of the GIL, so separate processes are used instead.
 
**Multi-file pipeline.** If two CSV files are given, each file is first processed separately and saved in its own folder. After that, `merge_final_shards()` combines the results, so if a vessel appears in both files, its full two-day track ends up together in one shard.
 
### Task 3 - Anomaly detection (`ais_anomalies.py`)
 
The anomaly detector reads shards directly. `iter_vessels_from_shard()` yields one vessel's complete ping history at a time, holding only one vessel in RAM at any moment.
 
Anomalies A, C, and D run in a single parallel pass: `pool.map(worker_shard_ACD, shard_files)` processes all 64 shards simultaneously. Each shard is independent = no vessel appears in more than one shard - so workers never communicate.
 
**Anomaly A - Going Dark** (`GAP_HOURS = 4.0 h, MOVING_SPEED_MIN = 0.1 kn`)

Look at each pair of consecutive pings. Measure how much time passed and how far the vessel moved. If the gap is longer than 4 hours, flag it when the vessel either moved more than 1 nautical mile or its estimated speed during the gap was above 0.1 knots. The distance rule helps catch slow movement over a long missing period, even when the estimated speed stays low.

**Anomaly B - Loitering / Ship-to-Ship Transfer** (`LOITER_SPEED_MAX = 1.0 kn, LOITER_HOURS_MIN = 2.0 h, LOITER_DIST_M = 500 m`)
 
Two-step algorithm. 

In the first step, the program looks for periods where a vessel keeps moving slowly, below 1 knot, without a faster ping breaking the sequence. If this slow period lasts at least 2 hours, it becomes a candidate event. If the vessel went dark during this period, the first ping after it returns is also noted, so the full event is not missed. The average position is calculated only from the slow pings.

In the second step, the program checks the smaller list of candidate events one by one. For each vessel pair, only the closest encounter is kept. Each matched pair is then given a confidence label based on draught changes:
- `draught_transfer`, one vessel's draught increased and the other's decreased. This is the strongest sign of a transfer.
- `draught_partial`, only one vessel shows a draught change, while the other has no draught data.
- `draught_unchanged`, both draught values stayed the same, which is more likely a port stop or anchoring.
- `proximity_only`, no draught data is available, so the match is based only on vessels being close together.
 
**Anomaly C - Draft Change** (`DRAFT_CHANGE_PCT = 5%, DRAFT_BLACKOUT_HRS = 2.0 h`)
 
Both pings must have draught above 0.5 m (filters default of 0.0). Gap must exceed 2 hours. Change must exceed 5%. Flags illegal loading or unloading during the blackout.
 
**Anomaly D - Identity Cloning** (`CLONE_SPEED_KN = 60.0 kn`)

This anomaly is detected in two ways. First, the program looks for consecutive pings that would require the vessel to travel faster than 60 knots. When several such pings happen in a row, they are grouped into one event. The largest distance jump is kept, because distance is used later in the DFSI calculation. Second, the program looks for cases where the same MMSI sends signals at the exact same time from different places at least 0.5 nautical miles apart. This suggests two different ships were using the same MMSI at the same time. Pings that place a vessel on land are treated as GPS errors. They are still flagged, but they are not included in the DFSI score.

**DFSI Formula**
 
```
DFSI = (max_gap_hours / 2) + (total_impossible_nm / 10) + (count_C × 1.5)
```
 
Only D events where `gps_error = False` contribute to `total_impossible_nm`.

### Task 4 - Benchmark (`ais_benchmark.py`)
 
Sweeps three chunk sizes (10k, 50k, 100k rows) at maximum worker count, and sweeps worker counts (1 through machine maximum) at chunk size 100k. The workers=1 run is the sequential baseline. RAM measured via `mprof --include-children` subprocess per chunk size. Amdahl's Law curve fitted from observed speedup at highest worker count.
 
