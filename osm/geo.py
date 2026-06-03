"""Shared geodesic utility functions."""
from __future__ import annotations

import math

from pyproj import Geod

GEOD = Geod(ellps="WGS84")


def bearing(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    az, _, _ = GEOD.inv(lng1, lat1, lng2, lat2)
    return float(az)


def turn_angle(b1: float, b2: float) -> float:
    diff = abs(b2 - b1) % 360
    if diff > 180:
        diff = 360 - diff
    return diff


def node_dist(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    _, _, d = GEOD.inv(lng1, lat1, lng2, lat2)
    return float(d) if d and math.isfinite(d) else 0.0
