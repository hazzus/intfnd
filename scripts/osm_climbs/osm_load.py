"""Pull cyclable ways out of an OSM PBF file."""
from dataclasses import dataclass

import osmium

from .surface import classify_surface

CYCLABLE_HIGHWAYS = {
    "primary", "primary_link",
    "secondary", "secondary_link",
    "tertiary", "tertiary_link",
    "unclassified", "residential", "road",
    "cycleway", "track", "living_street",
}


@dataclass
class Way:
    id: int
    coords: list[tuple[float, float]]  # (lng, lat)
    name: str | None
    ref: str | None
    highway: str
    surface: str  # 'asphalt' or 'non_asphalt'
    tags: dict


class WayCollector(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.ways: list[Way] = []

    def way(self, w):
        tags = w.tags
        hw = tags.get("highway")
        if hw not in CYCLABLE_HIGHWAYS:
            return
        if tags.get("bicycle") == "no":
            return
        # Tunnels: DEM samples ground surface, not the tunnel floor, so elevation is bogus.
        tunnel = tags.get("tunnel")
        if tunnel and tunnel != "no":
            return
        try:
            coords = [(n.lon, n.lat) for n in w.nodes if n.location.valid()]
        except osmium.InvalidLocationError:
            return
        if len(coords) < 2:
            return
        tag_dict = {t.k: t.v for t in tags}
        self.ways.append(
            Way(
                id=w.id,
                coords=coords,
                name=tags.get("name"),
                ref=tags.get("ref"),
                highway=hw or "<none>",
                surface=classify_surface(tag_dict, hw or ""),
                tags=tag_dict,
            )
        )


def load_ways(pbf_path: str) -> list[Way]:
    handler = WayCollector()
    # locations=True caches node coords so they're attached to ways
    handler.apply_file(pbf_path, locations=True)
    return handler.ways
