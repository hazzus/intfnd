"""--debug-way diagnostics.

Re-runs the same per-chain pipeline as the main path, but prints a verbose trace
at each step (extrema, candidate trough→peak pairs, score breakdown, optional
elevation-profile PNG). Also runs the combination DFS and reports any
combinations that touch the requested way ids.
"""
import numpy as np
from scipy.signal import find_peaks
from tqdm import tqdm

from .chains import (
    Chain,
    rotate_loop_chain,
)
from .combine import build_combinations
from .detect import (
    DetectedClimb,
    chain_node_slice,
    detect_in_chain,
    find_extrema_with_boundary,
)
from .elevation import reverse_profile, sample_elevation, smooth
from .geo import cumulative_distances, resample_way, way_length_m
from .osm_load import Way
from .score import print_score_breakdown_lines, score_breakdown


def debug_chain(
    chain: Chain,
    ways_by_id: dict[int, Way],
    dem,
    args,
    node_degree: dict[tuple[float, float], int],
) -> None:
    print(f"\n=== chain (seed way {chain.primary_id}, {len(chain.way_ids)} way(s)) ===")
    print(f"  way_ids: {chain.way_ids}")
    print(f"  highway: {chain.highway}   name: {chain.name!r}   ref: {chain.ref!r}")
    print(f"  bidirectional: {chain.bidirectional}")
    print(f"  raw coords: {len(chain.coords)} nodes")

    # Surface verdict breakdown: a chain is non_asphalt if ANY member resolves to non_asphalt.
    print(f"  surface verdict: {chain.surface}")
    print(f"  per-member surface (tag → resolved):")
    member_lengths = [way_length_m(ways_by_id[wid].coords) for wid in chain.way_ids]
    chain_total = sum(member_lengths) or 1.0
    tainters: list[tuple[int, float, str, str, str]] = []
    for wid, length_m in zip(chain.way_ids, member_lengths):
        w = ways_by_id[wid]
        raw_tag = w.tags.get("surface") or "<none>"
        marker = "  ← TAINTS" if w.surface == "non_asphalt" else ""
        pct = 100 * length_m / chain_total
        print(
            f"    way/{wid:>11}  len={length_m:7.1f}m ({pct:5.1f}%)  "
            f"highway={w.highway:<14}  surface={raw_tag:<14}  → {w.surface}{marker}"
        )
        if w.surface == "non_asphalt":
            tainters.append((wid, length_m, w.highway, raw_tag, w.surface))
    if chain.surface == "non_asphalt":
        share = 100 * sum(t[1] for t in tainters) / chain_total
        print(f"  → marked non_asphalt because {len(tainters)}/{len(chain.way_ids)} member(s) "
              f"({share:.1f}% of length) resolved to non_asphalt")
        for wid, length_m, hw, raw_tag, _ in tainters:
            reason = (
                f"explicit surface={raw_tag}" if raw_tag != "<none>"
                else f"no surface tag, highway={hw} defaults to non_asphalt"
            )
            print(f"      way/{wid}: {reason}")

    resampled = resample_way(chain.coords, args.sample_step)
    if resampled is None:
        print("  RESAMPLE FAILED (way too short or zero-length climbs)")
        return
    lats, lngs, cum = resampled
    print(f"  after {args.sample_step}m resample: {len(lats)} points, total length {cum[-1]:.1f} m")

    if cum[-1] < args.min_length:
        print(f"  REJECTED: total length {cum[-1]:.1f} m < --min-length {args.min_length}")
        return

    elev_raw = sample_elevation(dem, lats, lngs)
    nan_count = int(np.isnan(elev_raw).sum())
    if nan_count:
        print(f"  REJECTED: {nan_count} of {len(elev_raw)} elevation samples are nodata/NaN")
        print("  → DEM probably doesn't cover this way's bbox")
        return

    elev = smooth(elev_raw, args.smooth_window, args.sample_step)
    print(f"  elevation: raw min={elev_raw.min():.1f} max={elev_raw.max():.1f}  "
          f"smoothed min={elev.min():.1f} max={elev.max():.1f}  "
          f"raw gain={elev_raw.max()-elev_raw.min():.1f} m")

    chain_cum = cumulative_distances(chain.coords)
    chain_total = chain_cum[-1] if chain_cum else 0.0

    passes = [("forward", False, lats, lngs, cum, elev)]
    if chain.bidirectional:
        rl, rln, rc, re = reverse_profile(lats, lngs, cum, elev)
        passes.append(("reverse", True, rl, rln, rc, re))

    accepted_per_pass: list[tuple[str, list]] = []
    for label, reversed_pass, p_lats, p_lngs, p_cum, p_elev in passes:
        print(f"\n  --- pass: {label} ---")
        peaks, peak_props = find_peaks(p_elev, prominence=args.prominence)
        troughs, trough_props = find_peaks(-p_elev, prominence=args.prominence)
        print(f"  find_peaks(prominence={args.prominence}m): {len(peaks)} interior peaks, {len(troughs)} interior troughs")
        for i, p in enumerate(peaks):
            print(f"    peak   #{i}: idx={p}  dist={p_cum[p]:.0f} m  elev={p_elev[p]:.1f} m  prom={peak_props['prominences'][i]:.1f} m")
        for i, t in enumerate(troughs):
            print(f"    trough #{i}: idx={t}  dist={p_cum[t]:.0f} m  elev={p_elev[t]:.1f} m  prom={trough_props['prominences'][i]:.1f} m")

        extrema = find_extrema_with_boundary(p_elev, args.prominence)
        print(f"  extrema with synthetic boundaries: {len(extrema)} ({extrema[0]} ... {extrema[-1]})")
        print(f"  candidate trough→peak pairs:")
        if len(extrema) < 2:
            print(f"    NONE — profile too short")
        accepted: list[tuple[int, int, float, float, float]] = []
        for j in range(len(extrema) - 1):
            i_idx, i_kind = extrema[j]
            k_idx, k_kind = extrema[j + 1]
            if i_kind != "trough" or k_kind != "peak":
                print(f"    skip pair {i_kind}@{i_idx} → {k_kind}@{k_idx} (not trough→peak)")
                continue
            length = float(p_cum[k_idx] - p_cum[i_idx])
            gain = float(p_elev[k_idx] - p_elev[i_idx])
            grade = gain / length if length > 0 else 0
            reasons = []
            if grade <= 0:
                reasons.append(f"grade {grade*100:.2f}% <= 0")
            if length < args.min_length:
                reasons.append(f"length {length:.0f} < {args.min_length}")
            if grade < args.min_grade:
                reasons.append(f"grade {grade*100:.2f}% < {args.min_grade*100:.1f}%")
            if gain < args.min_gain:
                reasons.append(f"gain {gain:.1f} < {args.min_gain}")
            verdict = "ACCEPT" if not reasons else "REJECT (" + "; ".join(reasons) + ")"
            print(f"    trough@{i_idx} → peak@{k_idx}: length={length:.0f} m  grade={grade*100:.2f}%  gain={gain:.1f} m  → {verdict}")
            if not reasons:
                accepted.append((i_idx, k_idx, length, grade, gain))
        accepted_per_pass.append((label, accepted))

        if accepted:
            print(f"  score breakdown for {len(accepted)} accepted climb(s):")
        for ti, pi, length, grade, gain in accepted:
            nodes, _, _ = chain_node_slice(
                chain, chain_cum, chain_total, p_cum, ti, pi, reversed_pass,
            )
            elevation_profile = [float(x) for x in p_elev[ti : pi + 1]]
            sb = score_breakdown(nodes, elevation_profile, length, args.sample_step, node_degree)
            print(
                f"    climb trough@{ti}→peak@{pi}  ({length:.0f} m, {grade*100:.2f}%, +{gain:.1f} m, "
                f"{len(nodes)} chain nodes)"
            )
            print_score_breakdown_lines(sb, indent="      ")

    if args.debug_plot:
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(cum, elev_raw, color="#bbbbbb", linewidth=1, label="raw DEM")
        ax.plot(cum, elev, color="#1f77b4", linewidth=2, label=f"smoothed ({args.smooth_window} m)")
        all_peaks, _ = find_peaks(elev, prominence=args.prominence)
        all_troughs, _ = find_peaks(-elev, prominence=args.prominence)
        if len(all_peaks):
            ax.scatter(cum[all_peaks], elev[all_peaks], color="#d62728", zorder=5, label="peaks", s=40)
        if len(all_troughs):
            ax.scatter(cum[all_troughs], elev[all_troughs], color="#2ca02c", zorder=5, label="troughs", s=40)
        for label, accepted in accepted_per_pass:
            color = "#00b050" if label == "forward" else "#ff7f0e"
            for ti, pi, length, grade, gain in accepted:
                if label == "forward":
                    x0, x1 = cum[ti], cum[pi]
                else:
                    n = len(cum) - 1
                    x0 = cum[n - pi]
                    x1 = cum[n - ti]
                ax.axvspan(x0, x1, color=color, alpha=0.2, label=f"{label} climb")
        handles, labels = ax.get_legend_handles_labels()
        seen = set()
        uniq = [(h, l) for h, l in zip(handles, labels) if not (l in seen or seen.add(l))]
        ax.legend([h for h, _ in uniq], [l for _, l in uniq], loc="best")
        ax.set_xlabel("distance along way (m)")
        ax.set_ylabel("elevation (m)")
        title = f"chain seed=way/{chain.primary_id} ({len(chain.way_ids)} way(s))"
        if chain.name:
            title += f" — {chain.name}"
        title += f" [{chain.highway}]"
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        suffix = f"chain_{chain.primary_id}"
        out_path = f"{args.debug_plot}/{suffix}.png" if args.debug_plot.endswith("/") else f"{args.debug_plot}_{suffix}.png"
        fig.tight_layout()
        fig.savefig(out_path, dpi=120)
        plt.close(fig)
        print(f"  plot saved: {out_path}")


def debug_combinations(
    chains: list[Chain],
    only_ids: set[int],
    dem,
    args,
    node_degree: dict[tuple[float, float], int],
) -> None:
    """Run the full per-chain + combination pipeline (silently) and print every climb
    whose osm_way_ids intersect only_ids."""
    print("\n=== combination climbs touching requested way(s) ===")
    detected: list[DetectedClimb] = []
    for chain in tqdm(chains, unit="chain", desc="detect"):
        chain = rotate_loop_chain(chain, dem)
        detected.extend(detect_in_chain(chain, dem, args, node_degree))

    print(f"  detected {len(detected)} per-chain climbs; building combinations...")
    combinations = build_combinations(detected, dem, args, node_degree)
    print(f"  built {len(combinations)} combination climb(s)")

    tagged = [("chain", dc) for dc in detected] + [("combo", dc) for dc in combinations]
    matches = [(kind, dc) for kind, dc in tagged if only_ids & set(dc.osm_way_ids)]
    matches.sort(key=lambda kd: kd[1].score)

    print(f"  {len(matches)} climb(s) include way(s) {sorted(only_ids)}:")
    if not matches:
        return

    for kind, dc in matches:
        print(
            f"\n  {kind}: {dc.name}  "
            f"({dc.climb.length_m:.0f} m, {dc.climb.grade*100:.2f}%, +{dc.climb.gain_m:.1f} m)"
        )
        print(f"    way_ids: {dc.osm_way_ids}")
        print(f"    surfaces: {dc.surfaces}  highway: {dc.highway}")
        print(f"    start: {dc.climb.coords[0][0]:.5f},{dc.climb.coords[0][1]:.5f}")
        if dc.score_components:
            print_score_breakdown_lines(dc.score_components, indent="    ")


def run_debug(
    chains: list[Chain],
    ways: list[Way],
    only_ids: set[int],
    dem,
    args,
    node_degree: dict[tuple[float, float], int],
) -> None:
    """Entry point invoked from import_osm_climbs.py when --debug-way is set."""
    ways_by_id = {w.id: w for w in ways}
    seen_chains: set[int] = set()
    for chain_idx, chain in enumerate(chains):
        if not (only_ids & set(chain.way_ids)):
            continue
        if chain_idx in seen_chains:
            continue
        seen_chains.add(chain_idx)
        debug_chain(rotate_loop_chain(chain, dem), ways_by_id, dem, args, node_degree)
    all_way_ids = {w.id for w in ways}
    missing = only_ids - all_way_ids
    if missing:
        print(f"\nNOT FOUND in PBF: {sorted(missing)}")
    debug_combinations(chains, only_ids, dem, args, node_degree)
