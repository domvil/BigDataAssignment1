import math
import importlib

from config import INVALID_MMSI_EXACT, MMSI_MIN, MMSI_MAX


def mmsi_valid(m: int) -> bool:
    if m in INVALID_MMSI_EXACT:
        return False
    return MMSI_MIN <= m <= MMSI_MAX


def coord_valid(lat: float, lon: float) -> bool:
    if lat == 0.0 and lon == 0.0:
        return False
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0


_globe = None
HAS_LAND_MASK = False

def is_on_land(lat: float, lon: float) -> bool:
    global _globe, HAS_LAND_MASK
    if _globe is None:
        try:
            from global_land_mask import globe as _globe
            HAS_LAND_MASK = True
        except ImportError:
            HAS_LAND_MASK = False
            return False
    if not HAS_LAND_MASK:
        return False
    try:
        return bool(_globe.is_land(lat, lon))
    except Exception:
        return False

def haversine_nm(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in nautical miles."""
    R = 3440.065  # Earth radius in nm
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in metres."""
    return haversine_nm(lat1, lon1, lat2, lon2) * 1852.0


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in kilometres."""
    return haversine_nm(lat1, lon1, lat2, lon2) * 1.852


def implied_speed_kn(lat1, lon1, ts1, lat2, lon2, ts2) -> float:
    """Speed in knots implied by two positions and their timestamps."""
    dt_hours = (ts2 - ts1) / 3600.0
    if dt_hours <= 0:
        return float("inf")
    return haversine_nm(lat1, lon1, lat2, lon2) / dt_hours


def implied_speed_ms(lat1, lon1, ts1, lat2, lon2, ts2) -> float:
    """Speed in metres per second implied by two positions and their timestamps."""
    dt_seconds = ts2 - ts1
    if dt_seconds <= 0:
        return float("inf")
    return haversine_m(lat1, lon1, lat2, lon2) / dt_seconds


def bearing_deg(lat1, lon1, lat2, lon2) -> float:
    """Initial bearing in degrees (0-360) from point 1 to point 2."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlam = math.radians(lon2 - lon1)
    x = math.sin(dlam) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlam)
    return (math.degrees(math.atan2(x, y)) + 360) % 360

