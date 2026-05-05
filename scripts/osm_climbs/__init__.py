"""OSM climb extraction pipeline.

The package is organised as a sequence of pipeline stages, each in its own module:

    osm_load   → ways from the PBF
    chains     → ways stitched into rideable chains
    elevation  → DEM sampling and smoothing
    detect     → trough→peak climb detection on a chain's profile
    combine    → multi-chain climbs spanning junctions
    score      → quality score (intersections, turns, grade spikes)
    dedupe     → drop near-duplicate climbs by node-set overlap
    db         → row construction and upsert into the climbs table
    geojson_out→ optional debug feature collection
    debug      → --debug-way diagnostic prints (also depends on everything above)
    pipeline   → orchestrates the stages end-to-end

Supporting modules:
    geo        → geodesic + polyline geometry helpers
    surface    → surface tag classification
"""
