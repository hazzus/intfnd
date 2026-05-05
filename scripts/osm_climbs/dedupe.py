"""Drop near-duplicate climbs by node-set Jaccard overlap."""
from collections import defaultdict

from .detect import DetectedClimb


def deduplicate_climbs(
    climbs: list[DetectedClimb], max_similarity: float
) -> tuple[list[DetectedClimb], int]:
    """Drop near-duplicates: pairs whose node-sets have Jaccard overlap ≥ max_similarity.

    Score is a penalty (lower = better), so the lower-scored climb survives. Visits in
    ascending score order; each kept climb evicts its still-unprocessed neighbours.
    """
    if not climbs or max_similarity >= 1.0:
        return climbs, 0

    node_sets = [set(dc.nodes) for dc in climbs]
    node_index: dict[tuple[float, float], list[int]] = defaultdict(list)
    for i, ns in enumerate(node_sets):
        for n in ns:
            node_index[n].append(i)

    order = sorted(range(len(climbs)), key=lambda i: climbs[i].score)
    removed: set[int] = set()
    for i in order:
        if i in removed or not node_sets[i]:
            continue
        overlap: dict[int, int] = defaultdict(int)
        for n in node_sets[i]:
            for j in node_index[n]:
                if j == i or j in removed:
                    continue
                overlap[j] += 1
        size_i = len(node_sets[i])
        for j, count in overlap.items():
            union = size_i + len(node_sets[j]) - count
            if union and count / union >= max_similarity:
                removed.add(j)

    kept = [c for idx, c in enumerate(climbs) if idx not in removed]
    return kept, len(removed)
