"""Geodesic + polyline geometry helpers.

All coordinate tuples in this package are (lng, lat) order unless explicitly noted.
The detector uses (lat, lng) internally — that's a localised exception, kept for
back-compatibility with the stored polyline encoding.
"""
import numpy as np
from pyproj import Geod

GEOD = Geod(ellps="WGS84")


def resample_way(coords, step_m: float):
    """Resample a (lng, lat) polyline to roughly `step_m` spacing.

    Returns (lats, lngs, cum) numpy arrays parallel to the resampled points,
    or None if the input has fewer than two valid distinct vertices.
    """
    if len(coords) < 2:
        return None
    lats = [coords[0][1]]
    lngs = [coords[0][0]]
    cum = [0.0]
    cum_total = 0.0
    for i in range(1, len(coords)):
        lng1, lat1 = coords[i - 1][0], coords[i - 1][1]
        lng2, lat2 = coords[i][0], coords[i][1]
        az, _, dist = GEOD.inv(lng1, lat1, lng2, lat2)
        if dist == 0 or not np.isfinite(dist):
            continue
        n = max(1, int(np.floor(dist / step_m)))
        for k in range(1, n + 1):
            d = dist * (k / n)
            lon_k, lat_k, _ = GEOD.fwd(lng1, lat1, az, d)
            lats.append(lat_k)
            lngs.append(lon_k)
            cum.append(cum_total + d)
        cum_total += dist
    if len(lats) < 2:
        return None
    return np.array(lats), np.array(lngs), np.array(cum)


def cumulative_distances(coords: list[tuple[float, float]]) -> list[float]:
    """Cumulative geodesic distance along (lng, lat) coords; parallel to coords."""
    cum = [0.0]
    total = 0.0
    for i in range(1, len(coords)):
        lng1, lat1 = coords[i - 1]
        lng2, lat2 = coords[i]
        _, _, dist = GEOD.inv(lng1, lat1, lng2, lat2)
        if np.isfinite(dist):
            total += dist
        cum.append(total)
    return cum


def way_length_m(coords: list[tuple[float, float]]) -> float:
    total = 0.0
    for i in range(1, len(coords)):
        lng1, lat1 = coords[i - 1]
        lng2, lat2 = coords[i]
        _, _, dist = GEOD.inv(lng1, lat1, lng2, lat2)
        if np.isfinite(dist):
            total += dist
    return total


def bearing(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Forward azimuth from a to b in degrees, normalised to [0, 360)."""
    az, _, _ = GEOD.inv(a[0], a[1], b[0], b[1])
    return az % 360.0
