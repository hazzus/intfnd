"""Stitch ways into rideable chains.

A chain is a sequence of ways joined at shared endpoint nodes. Two ways pair at a
node only when their highway-group at that node has exactly two ports — so a
primary continuing through a 3-way junction with a side street still stitches,
but two primaries crossing don't (the pairing would be ambiguous).
"""
from collections import Counter, defaultdict
from dataclasses import dataclass, field

import numpy as np

from .elevation import sample_elevation
from .osm_load import Way


@dataclass
class Chain:
    way_ids: list[int]
    coords: list[tuple[float, float]]  # (lng, lat) deduped at way boundaries
    coord_way_ids: list[int]  # parallel to coords: which way each coord came from
    coord_surfaces: list[str]  # parallel to coords: resolved surface class per coord
    name: str | None
    ref: str | None
    highway: str
    surface: str  # 'asphalt' or 'non_asphalt'
    bidirectional: bool
    tags: dict = field(default_factory=dict)

    @property
    def primary_id(self) -> int:
        return self.way_ids[0]


def way_directions(tags: dict) -> set[str]:
    """Set of legal travel directions for a cyclist, relative to the way's stored coord order.

    'forward' = traversing coords[0] → coords[-1].
    'reverse' = traversing coords[-1] → coords[0].
    """
    bike = tags.get("oneway:bicycle")
    if bike == "no":
        return {"forward", "reverse"}
    if bike == "yes":
        return {"forward"}
    if bike == "-1":
        return {"reverse"}
    oneway = tags.get("oneway")
    if oneway in ("yes", "true", "1"):
        return {"forward"}
    if oneway in ("-1", "reverse"):
        return {"reverse"}
    return {"forward", "reverse"}


def build_chains(ways: list[Way]) -> list[Chain]:
    """Stitch ways into chains across shared endpoint nodes.

    Ways with start == end (closed loops) are not stitched.
    """
    if not ways:
        return []

    # node coord -> [(way_idx, 'start'|'end')]
    endpoints: dict[tuple[float, float], list[tuple[int, str]]] = defaultdict(list)
    for i, w in enumerate(ways):
        if w.coords[0] == w.coords[-1]:
            continue  # closed loop: skip endpoint registration so it stays a chain by itself
        endpoints[w.coords[0]].append((i, "start"))
        endpoints[w.coords[-1]].append((i, "end"))

    # partner[(way, port)] = (other_way, other_port) when the node has exactly
    # two ports with the same highway type (regardless of total node degree).
    partner: dict[tuple[int, str], tuple[int, str]] = {}
    for ports in endpoints.values():
        if len(ports) < 2:
            continue
        by_hw: dict[str, list[tuple[int, str]]] = defaultdict(list)
        for idx, port in ports:
            by_hw[ways[idx].highway].append((idx, port))
        for group in by_hw.values():
            if len(group) != 2:
                continue
            (i1, p1), (i2, p2) = group
            if i1 == i2:
                continue
            partner[(i1, p1)] = (i2, p2)
            partner[(i2, p2)] = (i1, p1)

    visited = [False] * len(ways)

    def walk(seed_way: int, seed_port: str, direction: str) -> list[tuple[int, bool]]:
        """Walk through partners. Returns [(way_idx, reversed_flag), ...] in walk order."""
        out: list[tuple[int, bool]] = []
        cur_way, cur_port = seed_way, seed_port
        while (cur_way, cur_port) in partner:
            next_way, next_port = partner[(cur_way, cur_port)]
            if visited[next_way]:
                break
            visited[next_way] = True
            if direction == "forward":
                rev = next_port == "end"
            else:
                rev = next_port == "start"
            out.append((next_way, rev))
            cur_way = next_way
            cur_port = "start" if next_port == "end" else "end"
        return out

    chains: list[Chain] = []
    for seed in range(len(ways)):
        if visited[seed]:
            continue
        visited[seed] = True
        forward = walk(seed, "end", "forward")
        backward = walk(seed, "start", "backward")
        ordered: list[tuple[int, bool]] = (
            list(reversed(backward)) + [(seed, False)] + forward
        )

        # Decide whether the chain's stitched order is rideable as-is, only in reverse,
        # both ways, or not at all. For each member, the chain's "forward" traversal
        # consumes the way in 'forward' direction when rev=False, else in 'reverse'.
        chain_fwd_ok = True
        chain_rev_ok = True
        for way_idx, rev in ordered:
            dirs = way_directions(ways[way_idx].tags)
            fwd_member_dir = "reverse" if rev else "forward"
            rev_member_dir = "forward" if rev else "reverse"
            if fwd_member_dir not in dirs:
                chain_fwd_ok = False
            if rev_member_dir not in dirs:
                chain_rev_ok = False
            if not chain_fwd_ok and not chain_rev_ok:
                break

        if not chain_fwd_ok and not chain_rev_ok:
            # Members disagree on direction — chain isn't rideable end-to-end.
            continue

        if not chain_fwd_ok and chain_rev_ok:
            # Flip the chain so its stored "forward" matches the legal direction.
            ordered = [(idx, not rev) for idx, rev in reversed(ordered)]

        # Concatenate coords, deduping the shared boundary node between adjacent ways.
        combined: list[tuple[float, float]] = []
        combined_way_ids: list[int] = []
        combined_surfaces: list[str] = []
        for k, (way_idx, rev) in enumerate(ordered):
            seg = ways[way_idx].coords if not rev else list(reversed(ways[way_idx].coords))
            wid = ways[way_idx].id
            surf = ways[way_idx].surface
            if k == 0:
                combined.extend(seg)
                combined_way_ids.extend([wid] * len(seg))
                combined_surfaces.extend([surf] * len(seg))
            else:
                combined.extend(seg[1:])
                combined_way_ids.extend([wid] * (len(seg) - 1))
                combined_surfaces.extend([surf] * (len(seg) - 1))

        members = [ways[i] for i, _ in ordered]
        highway = Counter(m.highway for m in members).most_common(1)[0][0]
        # Strict: any non-asphalt member taints the chain (riders care about the worst patch).
        surface = "non_asphalt" if any(m.surface == "non_asphalt" for m in members) else "asphalt"
        names = [m.name for m in members if m.name]
        seed_way = ways[seed]
        chains.append(
            Chain(
                way_ids=[ways[i].id for i, _ in ordered],
                coords=combined,
                coord_way_ids=combined_way_ids,
                coord_surfaces=combined_surfaces,
                name=seed_way.name or (names[0] if names else None),
                ref=seed_way.ref,
                highway=highway,
                surface=surface,
                bidirectional=chain_fwd_ok and chain_rev_ok,
                tags=seed_way.tags,
            )
        )
    return chains


def rotate_loop_chain(chain: Chain, dem) -> Chain:
    """If chain is a closed loop, rotate it so coords[0] is at the lowest-elevation node.

    Loops have coords[0] == coords[-1]. The detector treats coords[0] as a synthetic
    trough; if that boundary falls mid-climb, the detector misses the true ascent
    extent. Rotating to the lowest node makes the synthetic trough coincide with the
    real loop low point.
    """
    if len(chain.coords) < 3 or chain.coords[0] != chain.coords[-1]:
        return chain
    interior = chain.coords[:-1]
    interior_wids = chain.coord_way_ids[:-1]
    interior_surfs = chain.coord_surfaces[:-1]
    lngs = np.array([c[0] for c in interior])
    lats = np.array([c[1] for c in interior])
    elev = sample_elevation(dem, lats, lngs)
    if np.any(np.isnan(elev)):
        return chain
    k = int(np.argmin(elev))
    if k == 0:
        return chain
    rotated = list(interior[k:]) + list(interior[:k])
    rotated.append(rotated[0])
    rotated_wids = list(interior_wids[k:]) + list(interior_wids[:k])
    rotated_wids.append(rotated_wids[0])
    rotated_surfs = list(interior_surfs[k:]) + list(interior_surfs[:k])
    rotated_surfs.append(rotated_surfs[0])
    return Chain(
        way_ids=chain.way_ids,
        coords=rotated,
        coord_way_ids=rotated_wids,
        coord_surfaces=rotated_surfs,
        name=chain.name,
        ref=chain.ref,
        highway=chain.highway,
        surface=chain.surface,
        bidirectional=chain.bidirectional,
        tags=chain.tags,
    )


def compute_node_ways(ways: list[Way]) -> dict[tuple[float, float], set[int]]:
    """Map each node coord to the set of cyclable way ids touching it."""
    node_ways: dict[tuple[float, float], set[int]] = defaultdict(set)
    for w in ways:
        for c in w.coords:
            node_ways[c].add(w.id)
    return node_ways


def compute_node_degree(ways: list[Way]) -> dict[tuple[float, float], int]:
    """Topological port count at each node: number of incident way-edges.

    Each consecutive coord pair in a way contributes one port to each of its two
    endpoints. A plain mid-node of a single way is degree 2; an end-to-end stitch
    between two ways is degree 2; a T-junction (one way ends at the interior of
    another) is degree 3; an X-crossing (two ways both pass through) is degree 4.

    Counting distinct ways instead would miss T/X cases where OSM hasn't split the
    through-way at the junction node — both look like "two ways touch here".
    Degree >= 3 is treated as a real intersection by the scorer; degree 2 is a
    chain continuation.
    """
    deg: dict[tuple[float, float], int] = defaultdict(int)
    for w in ways:
        coords = w.coords
        for i in range(len(coords) - 1):
            a, b = coords[i], coords[i + 1]
            if a == b:
                continue
            deg[a] += 1
            deg[b] += 1
    return dict(deg)


def chain_display_name(chain: Chain) -> str:
    if chain.name:
        return str(chain.name)
    if chain.ref:
        return f"Climb on {chain.ref}"
    return "Unnamed climb"
