import logging
from pathlib import Path

import psycopg2.extensions
import psycopg2.extras
import rasterio
from pyproj import Transformer
from tqdm import tqdm

log = logging.getLogger(__name__)
logging.getLogger("rasterio").setLevel(logging.WARNING)
logging.getLogger("pyproj").setLevel(logging.WARNING)

_BATCH_SIZE = 1000
_WGS84 = "EPSG:4326"


def fill_elevations(conn: psycopg2.extensions.connection, dem_path: Path):
    ds = rasterio.open(dem_path)
    transformer = (
        Transformer.from_crs(_WGS84, ds.crs, always_xy=True)
        if ds.crs and ds.crs.to_epsg() != 4326
        else None
    )
    try:
        total = 0
        with conn.cursor(name="nodes_elevation", withhold=True) as cur:
            cur.execute("SELECT COUNT(*) FROM nodes WHERE elevation IS NULL")
            total = cur.fetchone()[0]
        with conn.cursor(name="nodes_elevation", withhold=True) as cur:
            cur.execute("SELECT id, lat, lng FROM nodes WHERE elevation IS NULL")
            bar = tqdm(total=total, unit="node", desc="elevation")
            while True:
                rows = cur.fetchmany(_BATCH_SIZE)
                if not rows:
                    break
                updates = []
                for node_id, lat, lng in rows:
                    x, y = transformer.transform(lng, lat) if transformer else (lng, lat)
                    val = next(ds.sample([(x, y)]))[0]
                    if val != ds.nodata:
                        updates.append((float(val), node_id))
                if updates:
                    with conn.cursor() as upd:
                        psycopg2.extras.execute_values(
                            upd,
                            "UPDATE nodes SET elevation = data.elev FROM (VALUES %s) AS data(elev, id) WHERE nodes.id = data.id",
                            updates,
                        )
                    conn.commit()
                bar.update(len(rows))
            bar.close()
    finally:
        ds.close()
    log.info("elevations done")
