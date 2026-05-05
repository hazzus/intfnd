"""OSM surface tag classification.

A way is reduced to one of two classes — 'asphalt' or 'non_asphalt' — by
inspecting its surface tag and falling back to the highway class when no
recognised surface tag is present.
"""

ASPHALT_SURFACES = {
    "asphalt", "paved", "concrete", "concrete:lanes", "concrete:plates",
    "paving_stones", "chipseal", "metal",
}
NON_ASPHALT_SURFACES = {
    "gravel", "fine_gravel", "dirt", "ground", "earth", "unpaved",
    "sand", "mud", "grass", "compacted", "wood", "woodchips",
    "pebblestone", "cobblestone", "sett",
}
HIGHWAY_DEFAULT_ASPHALT = {
    "primary", "primary_link", "secondary", "secondary_link",
    "tertiary", "tertiary_link", "unclassified", "residential",
    "living_street", "road",
}


def classify_surface(tags: dict, highway: str) -> str:
    """Return 'asphalt' or 'non_asphalt' for a way."""
    s = (tags.get("surface") or "").lower()
    if s in ASPHALT_SURFACES:
        return "asphalt"
    if s in NON_ASPHALT_SURFACES:
        return "non_asphalt"
    return "asphalt" if highway in HIGHWAY_DEFAULT_ASPHALT else "non_asphalt"
