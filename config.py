import os

# ── Shared ────────────────────────────────────────────────────
NUM_WORKERS = min(12, os.cpu_count() or 1)

# ── Pipeline ──────────────────────────────────────────────────
COL_MMSI      = "MMSI"
COL_TIMESTAMP = "# Timestamp"
COL_LAT       = "Latitude"
COL_LON       = "Longitude"
COL_SOG       = "SOG"
COL_DRAUGHT   = "Draught"
COL_SHIP_TYPE = "Ship type"
COL_NAME      = "Name"
COL_DEST      = "Destination"
COL_MOBILE_TYPE = "Type of mobile"

ALLOWED_MOBILE_TYPES: frozenset[str] = frozenset({"Class A"})

INVALID_MMSI_EXACT: set[int] = {
    0, 111111111, 123456789, 222222222, 999999999,
}
MMSI_MIN    = 200_000_000
MMSI_MAX    = 999_999_999

NUM_SHARDS  = 64
CHUNK_SIZES = [10_000, 50_000, 100_000]
WORK_DIR    = "work_mmsi_parts"

# ── Anomaly detection ─────────────────────────────────────────
RESULTS_DIR = "results"

# Anomaly thresholds
GAP_HOURS          = 4.0    # Anomaly A: min gap duration hours
MOVING_SPEED_MIN   = 0.1    # knots - below this = truly anchored
LOITER_SPEED_MAX   = 1.0    # Anomaly B: SOG threshold knots
LOITER_HOURS_MIN   = 2.0    # Anomaly B: min duration hours
LOITER_DIST_M      = 500.0  # Anomaly B: max separation metres
DRAFT_CHANGE_PCT   = 0.05   # Anomaly C: 5% draught change
DRAFT_BLACKOUT_HRS = 2.0    # Anomaly C: min blackout hours
CLONE_SPEED_KN     = 60.0   # Anomaly D: impossible speed knots
