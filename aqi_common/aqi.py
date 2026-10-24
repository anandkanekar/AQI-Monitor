"""US EPA Air Quality Index calculation.

Single source of truth for AQI breakpoints. Used by:
  * the Flink real-time detection job (per-reading AQI + threshold alerts)
  * the Airflow curation DAG (generates equivalent Trino SQL CASE expressions)

Units expected from sensors:
  pm25 -> ug/m3 (24h breakpoints, 2024 EPA revision)
  pm10 -> ug/m3 (24h)
  no2  -> ppb   (1h)
  o3   -> ppb   (8h breakpoints; values above 200 ppb mapped with the 1h table)
  co2  -> ppm   (not part of the EPA AQI; tracked as an indoor-air-quality level)

Must remain compatible with Python 3.10 (PyFlink runtime).
"""
from __future__ import annotations

import math
from typing import Dict, List, Optional, Tuple

# (C_low, C_high, I_low, I_high)
Breakpoint = Tuple[float, float, int, int]

BREAKPOINTS: Dict[str, List[Breakpoint]] = {
    "pm25": [
        (0.0, 9.0, 0, 50),
        (9.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 125.4, 151, 200),
        (125.5, 225.4, 201, 300),
        (225.5, 325.4, 301, 500),
    ],
    "pm10": [
        (0, 54, 0, 50),
        (55, 154, 51, 100),
        (155, 254, 101, 150),
        (255, 354, 151, 200),
        (355, 424, 201, 300),
        (425, 604, 301, 500),
    ],
    "no2": [
        (0, 53, 0, 50),
        (54, 100, 51, 100),
        (101, 360, 101, 150),
        (361, 649, 151, 200),
        (650, 1249, 201, 300),
        (1250, 2049, 301, 500),
    ],
    "o3": [
        (0, 54, 0, 50),
        (55, 70, 51, 100),
        (71, 85, 101, 150),
        (86, 105, 151, 200),
        (106, 200, 201, 300),
        (201, 604, 301, 500),
    ],
}

# EPA truncation rules: number of decimals kept before the lookup.
TRUNCATION_DECIMALS: Dict[str, int] = {"pm25": 1, "pm10": 0, "no2": 0, "o3": 0}

AQI_POLLUTANTS = tuple(BREAKPOINTS.keys())
ALL_POLLUTANTS = AQI_POLLUTANTS + ("co2",)

# Upper bound of physically plausible sensor values; above this the reading is treated as a fault.
PLAUSIBLE_RANGES: Dict[str, Tuple[float, float]] = {
    "pm25": (0.0, 1000.0),
    "pm10": (0.0, 2000.0),
    "no2": (0.0, 3000.0),
    "o3": (0.0, 1000.0),
    "co2": (250.0, 10000.0),
}

# (upper bound inclusive, category name)
CATEGORIES: List[Tuple[int, str]] = [
    (50, "Good"),
    (100, "Moderate"),
    (150, "Unhealthy for Sensitive Groups"),
    (200, "Unhealthy"),
    (300, "Very Unhealthy"),
]
HAZARDOUS = "Hazardous"

# CO2 (ppm) indoor air quality levels: (upper bound exclusive, level)
CO2_LEVELS: List[Tuple[float, str]] = [
    (800.0, "Good"),
    (1000.0, "Moderate"),
    (1500.0, "Poor"),
    (2000.0, "Unhealthy"),
]
CO2_HAZARDOUS = "Hazardous"


def truncate(value: float, decimals: int) -> float:
    factor = 10 ** decimals
    return math.floor(value * factor) / factor


def sub_index(pollutant: str, concentration: Optional[float]) -> Optional[int]:
    """Return the AQI sub-index for one pollutant, or None if not computable."""
    if concentration is None or pollutant not in BREAKPOINTS:
        return None
    if isinstance(concentration, float) and math.isnan(concentration):
        return None
    if concentration < 0:
        return None
    c = truncate(concentration, TRUNCATION_DECIMALS[pollutant])
    for c_lo, c_hi, i_lo, i_hi in BREAKPOINTS[pollutant]:
        if c <= c_hi:
            # Values that fall in the gap between two rows (only possible without truncation) snap up.
            c = max(c, c_lo)
            # Half-up rounding (matches Trino's round(); Python's round() is banker's rounding).
            return int(math.floor((i_hi - i_lo) / (c_hi - c_lo) * (c - c_lo) + i_lo + 0.5))
    return 500


def category(aqi: Optional[int]) -> Optional[str]:
    if aqi is None:
        return None
    for upper, name in CATEGORIES:
        if aqi <= upper:
            return name
    return HAZARDOUS


def co2_level(co2: Optional[float]) -> Optional[str]:
    if co2 is None:
        return None
    for upper, name in CO2_LEVELS:
        if co2 < upper:
            return name
    return CO2_HAZARDOUS


def compute_aqi(reading: Dict[str, Optional[float]]) -> Dict[str, object]:
    """Compute overall AQI (max of sub-indices) plus the dominant pollutant."""
    indices = {p: sub_index(p, reading.get(p)) for p in AQI_POLLUTANTS}
    valid = {p: i for p, i in indices.items() if i is not None}
    aqi = max(valid.values()) if valid else None
    dominant = None
    if valid:
        # Deterministic tie-break following AQI_POLLUTANTS order.
        dominant = next(p for p in AQI_POLLUTANTS if valid.get(p) == aqi)
    return {
        "aqi": aqi,
        "aqi_category": category(aqi),
        "dominant_pollutant": dominant,
        "sub_indices": indices,
        "co2_level": co2_level(reading.get("co2")),
    }


# ---------------------------------------------------------------------------
# Trino SQL generation (keeps batch curation identical to streaming logic)
# ---------------------------------------------------------------------------

def _num(v: float) -> str:
    return f"{float(v):.6f}"


def _dbl(v: float) -> str:
    """DOUBLE literal (scientific notation) so Trino doesn't use DECIMAL arithmetic."""
    s = repr(float(v))
    return s if "e" in s else f"{s}E0"


def plausible_sql(pollutant: str, column: str) -> str:
    """Trino expression that nulls out physically implausible (faulty sensor) values."""
    lo, hi = PLAUSIBLE_RANGES[pollutant]
    return f"CASE WHEN {column} BETWEEN {_num(lo)} AND {_num(hi)} THEN {column} END"


def truncated_sql(pollutant: str, column: str) -> str:
    """Trino expression applying EPA truncation and plausibility filtering."""
    lo, hi = PLAUSIBLE_RANGES[pollutant]
    decimals = TRUNCATION_DECIMALS[pollutant]
    factor = 10 ** decimals
    return (
        f"CASE WHEN {column} BETWEEN {_num(lo)} AND {_num(hi)} "
        f"THEN floor({column} * {factor}) / {factor} END"
    )


def sub_index_sql(pollutant: str, column: str) -> str:
    """Trino CASE expression computing the AQI sub-index of an already-truncated column."""
    parts = [f"CASE WHEN {column} IS NULL OR {column} < 0 THEN CAST(NULL AS integer)"]
    for c_lo, c_hi, i_lo, i_hi in BREAKPOINTS[pollutant]:
        # Same floating-point operation order as sub_index() so both layers agree bit-for-bit.
        parts.append(
            f"WHEN {column} <= {_dbl(c_hi)} THEN CAST(floor({_dbl(i_hi - i_lo)} / {_dbl(c_hi - c_lo)} * "
            f"(greatest({column}, {_dbl(c_lo)}) - {_dbl(c_lo)}) + {_dbl(i_lo)} + 0.5E0) AS integer)"
        )
    parts.append("ELSE 500 END")
    return " ".join(parts)


def category_sql(column: str) -> str:
    parts = ["CASE WHEN {c} IS NULL THEN CAST(NULL AS varchar)".format(c=column)]
    for upper, name in CATEGORIES:
        parts.append(f"WHEN {column} <= {upper} THEN '{name}'")
    parts.append(f"ELSE '{HAZARDOUS}' END")
    return " ".join(parts)


def co2_level_sql(column: str) -> str:
    parts = [f"CASE WHEN {column} IS NULL THEN CAST(NULL AS varchar)"]
    for upper, name in CO2_LEVELS:
        parts.append(f"WHEN {column} < {_num(upper)} THEN '{name}'")
    parts.append(f"ELSE '{CO2_HAZARDOUS}' END")
    return " ".join(parts)
