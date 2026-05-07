"""DEM sampling and elevation-profile smoothing."""
import numpy as np
from pyproj import Transformer
from scipy.ndimage import uniform_filter1d


def sample_elevation(dataset, lats: np.ndarray, lngs: np.ndarray) -> np.ndarray:
    if dataset.crs and dataset.crs.to_epsg() != 4326:
        transformer = Transformer.from_crs("EPSG:4326", dataset.crs, always_xy=True)
        xs, ys = transformer.transform(lngs.tolist(), lats.tolist())
        pts = list(zip(xs, ys))
    else:
        pts = list(zip(lngs.tolist(), lats.tolist()))
    samples = list(dataset.sample(pts))
    elev = np.array([s[0] for s in samples], dtype=float)
    nodata = dataset.nodata
    if nodata is not None:
        elev[elev == nodata] = np.nan
    elev[elev < -1000] = np.nan
    return elev


def smooth(elev: np.ndarray, window_m: float, step_m: float) -> np.ndarray:
    w = max(1, int(round(window_m / step_m)))
    if w <= 1 or w >= len(elev):
        return elev
    return uniform_filter1d(elev, size=w, mode="nearest")


def reverse_profile(lats, lngs, cum, elev):
    return (
        lats[::-1],
        lngs[::-1],
        cum[-1] - cum[::-1],
        elev[::-1],
    )
