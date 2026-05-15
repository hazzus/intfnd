
HIGHWAY_DEFAULT_PAVED = {
    "primary", "primary_link", "secondary", "secondary_link",
    "tertiary", "tertiary_link", "unclassified", "residential",
    "living_street", "road",
}


def get_surface(surface: str | None, highway: str) -> str:
    if surface is not None:
        return surface
    
    return "paved" if highway in HIGHWAY_DEFAULT_PAVED else "unpaved"
