from __future__ import annotations


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    q = max(0.0, min(1.0, q))
    sorted_vals = sorted(values)
    idx = int(round((len(sorted_vals) - 1) * q))
    return float(sorted_vals[idx])
