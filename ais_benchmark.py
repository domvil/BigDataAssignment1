import os
import sys
import time
import subprocess
import logging
import csv as csv_module

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from ais_pipeline import run_pipeline, run_pipeline_multi, cleanup_dir, NUM_WORKERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

CHUNK_SIZES   = [10_000, 50_000, 100_000]
WORKER_COUNTS = [1, 2, 4, 6, 8, max(8, NUM_WORKERS)]
WORKER_COUNTS = sorted(set(WORKER_COUNTS))


# ----------------------------------------------
# Peak RAM measurement via mprof subprocess
# ----------------------------------------------

def measure_peak_ram_mib(filepaths: list[str], chunk_size: int, num_workers: int) -> tuple[float, float]:
    """
    Run the pipeline under mprof and return (elapsed_seconds, peak_ram_mib).
    """
    dat_file = f"mprof_bench_{chunk_size}_{num_workers}.dat"
    cmd = [
        "mprof", "run",
        "--include-children",
        f"--output={dat_file}",
        sys.executable, "ais_pipeline.py",
    ] + filepaths + [str(chunk_size), f"--workers={num_workers}"]
    print(cmd)
    t0 = time.perf_counter()
 
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        log.warning("mprof not available - skipping RAM measurement")
        return -1.0, -1.0
    elapsed = time.perf_counter() - t0
 
    peak = -1.0
    try:
        with open(dat_file) as f:
            for line in f:
                if line.startswith("MEM"):
                    val = float(line.split()[1])
                    if val > peak:
                        peak = val
        os.remove(dat_file)
    except Exception:
        pass
 
    return elapsed, peak


# ----------------------------------------------
# Benchmark
# ----------------------------------------------

def benchmark_chunk_sizes(filepaths: list[str]) -> list[dict]:
    """
    For each chunk size: measure time and peak RAM in a single run.
    """
    rows = []
    for cs in CHUNK_SIZES:
        log.info("-- chunk_size=%d --", cs)
        t, ram = measure_peak_ram_mib(filepaths, cs, NUM_WORKERS)
        if t < 0:
            # mprof unavailable
            ram = -1.0
        rows.append({
            "chunk_size":    cs,
            "time_s":        round(t, 2),
            "peak_ram_mib":  round(ram, 1),
        })
        log.info("  time=%.2f s  RAM=%.0f MiB", t, ram)
    return rows


def benchmark_worker_counts(filepaths: list[str]) -> list[dict]:
    """
    For each worker count: measure time, RAM, and compute speedup.
    The workers=1 run is the sequential baseline.
    Fixed chunk size = 100_000.
    """
    counts = sorted(set([1] + WORKER_COUNTS))

    rows = []
    t_seq = None

    for nw in counts:
        log.info("-- workers=%d --", nw)
        t, ram = measure_peak_ram_mib(filepaths, 100_000, nw)

        if t_seq is None:
            t_seq = t
            log.info("Sequential baseline (1 worker): %.2f s", t_seq)

        s = t_seq / t if t > 0 else 0.0
        rows.append({
            "workers":       nw,
            "time_s":        round(t, 2),
            "speedup":       round(s, 3),
            "t_seq":         round(t_seq, 2),
            "peak_ram_mib":  round(ram, 1),
        })
        log.info("  time=%.2f s  speedup=%.2fx  RAM=%.0f MiB", t, s, ram)
    return rows


# ----------------------------------------------
# Amdahl helpers
# ----------------------------------------------

def amdahl(p: float, n: int) -> float:
    return 1.0 / ((1 - p) + p / n)

def estimate_p(speedup: float, n: int) -> float:
    inv_s = 1.0 / speedup
    denom = 1.0 / n - 1.0
    if denom == 0:
        return 1.0
    return max(0.0, min(1.0, (inv_s - 1.0) / denom))

# ----------------------------------------------
# Plotting
# ----------------------------------------------

def plot_all(chunk_rows: list[dict], worker_rows: list[dict]) -> None:
    cs        = [r["chunk_size"]   for r in chunk_rows]
    times     = [r["time_s"]       for r in chunk_rows]
    rams      = [r["peak_ram_mib"] for r in chunk_rows]
    cs_labels = [f"{c//1000}k"     for c in cs]

    wc       = [r["workers"] for r in worker_rows]
    speedups = [r["speedup"] for r in worker_rows]
    wtimes   = [r["time_s"]  for r in worker_rows]
    t_seq    = worker_rows[0]["t_seq"]

    p_est    = estimate_p(speedups[-1], wc[-1])
    amdahl_y = [amdahl(p_est, n) for n in wc]
    perfect  = [float(n)          for n in wc]

    color_main = "#1D9E75"
    color_sec  = "#534AB7"
    color_grey = "#888780"

    # 1: time vs chunk size
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.bar(cs_labels, times, color=color_main, width=0.5)
    for i, (_, y) in enumerate(zip(cs_labels, times)):
        ax.text(i, y + 0.5, f"{y:.1f}s", ha="center", fontsize=10)
    ax.set_xlabel("Chunk size (rows)")
    ax.set_ylabel("Time (s)")
    ax.set_title("Execution time vs chunk size")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linewidth=0.4, alpha=0.5)
    plt.tight_layout()
    plt.savefig("benchmark_time_vs_chunk.png", dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved -> benchmark_time_vs_chunk.png")

    # 2: peak RAM vs chunk size
    fig, ax = plt.subplots(figsize=(7, 5))
    has_ram = any(r > 0 for r in rams)
    if has_ram:
        ax.bar(cs_labels, rams, color=color_sec, width=0.5)
        total_limit_mib = 1024 * NUM_WORKERS
        ax.axhline(total_limit_mib, color="red", linewidth=1, linestyle="--",
                   label=f"Total limit ({NUM_WORKERS} workers x 1 GB = {total_limit_mib//1024} GB)")
        for i, (_, y) in enumerate(zip(cs_labels, rams)):
            per_worker = y / NUM_WORKERS
            label = f"{y:.0f} MiB total ({per_worker:.0f}/worker)"
            ax.text(i, y + 40, label, ha="center", fontsize=9)
        ax.legend(fontsize=9)
    else:
        ax.text(0.5, 0.5, "mprof not available\n(run separately)",
                ha="center", va="center", transform=ax.transAxes,
                color=color_grey, fontsize=11)
    ax.set_xlabel("Chunk size (rows)")
    ax.set_ylabel("Peak RAM - all workers combined (MiB)")
    ax.set_title("Peak RAM vs chunk size")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linewidth=0.4, alpha=0.5)
    plt.tight_layout()
    plt.savefig("benchmark_ram_vs_chunk.png", dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved -> benchmark_ram_vs_chunk.png")

    # 3: execution time vs workers
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(wc, wtimes, "o-", color=color_main, linewidth=2, markersize=6,
            label="parallel")
    ax.axhline(t_seq, color=color_grey, linewidth=1.2, linestyle="--",
               label=f"sequential ({t_seq:.0f}s)")
    ax.set_xlabel("Worker count")
    ax.set_ylabel("Time (s)")
    ax.set_title("Execution time vs workers")
    ax.xaxis.set_major_locator(ticker.FixedLocator(wc))
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linewidth=0.4, alpha=0.5)
    plt.tight_layout()
    plt.savefig("benchmark_time_vs_workers.png", dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved -> benchmark_time_vs_workers.png")

    # 4: speedup + Amdahl
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(wc, speedups,  "o-",  color=color_main, linewidth=2,
            markersize=6, label="observed")
    ax.plot(wc, amdahl_y,  "s--", color=color_sec,  linewidth=1.5,
            markersize=5, label=f"Amdahl (p={p_est:.2f})")
    ax.plot(wc, perfect,   ":",   color=color_grey, linewidth=1.2,
            label="perfect linear")
    ax.set_xlabel("Worker count")
    ax.set_ylabel("Speedup S")
    ax.set_title("Speedup vs workers (baseline = 1 worker)")
    ax.xaxis.set_major_locator(ticker.FixedLocator(wc))
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linewidth=0.4, alpha=0.5)
    plt.tight_layout()
    plt.savefig("benchmark_speedup.png", dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved -> benchmark_speedup.png")


def save_csv_results(chunk_rows: list[dict], worker_rows: list[dict]) -> None:
    with open("benchmark_chunk_sizes.csv", "w", newline="") as f:
        w = csv_module.DictWriter(f, fieldnames=chunk_rows[0].keys())
        w.writeheader()
        w.writerows(chunk_rows)

    with open("benchmark_worker_counts.csv", "w", newline="") as f:
        w = csv_module.DictWriter(f, fieldnames=worker_rows[0].keys())
        w.writeheader()
        w.writerows(worker_rows)

    log.info("CSVs saved: benchmark_chunk_sizes.csv, benchmark_worker_counts.csv")


# ----------------------------------------------
# Entry point
# ----------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ais_benchmark.py <csv1> [csv2 ...]")
        sys.exit(1)

    filepaths = [a for a in sys.argv[1:] if a.endswith(".csv")]
    if not filepaths:
        print("ERROR: no .csv files specified")
        sys.exit(1)

    log.info("Benchmarking with %d file(s): %s", len(filepaths),
             ", ".join(filepaths))

    log.info("=== Chunk size benchmark ===")
    chunk_rows = benchmark_chunk_sizes(filepaths)

    log.info("=== Worker count benchmark ===")
    worker_rows = benchmark_worker_counts(filepaths)

    plot_all(chunk_rows, worker_rows)
    save_csv_results(chunk_rows, worker_rows)

    print("\nChunk size results:")
    print(f"  {'Chunk':<10} {'Time (s)':<12} {'Peak RAM (MiB)'}")
    for r in chunk_rows:
        print(f"  {r['chunk_size']//1000}k{'':<8} {r['time_s']:<12} {r['peak_ram_mib']}")

    print("\nWorker count results:")
    print(f"  {'Workers':<10} {'Time (s)':<12} {'Speedup'}")
    for r in worker_rows:
        print(f"  {r['workers']:<10} {r['time_s']:<12} {r['speedup']}x")