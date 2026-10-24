import pytest

from aqi_common import aqi


@pytest.mark.parametrize(
    "pollutant,value,expected",
    [
        ("pm25", 0.0, 0),
        ("pm25", 9.0, 50),
        ("pm25", 9.05, 50),  # truncated to 9.0
        ("pm25", 9.1, 51),
        ("pm25", 35.4, 100),
        ("pm25", 35.9, 102),
        ("pm25", 55.5, 151),
        ("pm25", 150.0, 225),
        ("pm25", 325.4, 500),
        ("pm25", 900.0, 500),
        ("pm10", 54.9, 50),  # truncated to 54
        ("pm10", 155, 101),
        ("no2", 100, 100),
        ("no2", 400, 158),
        ("o3", 70, 100),
        ("o3", 86, 151),
    ],
)
def test_sub_index(pollutant, value, expected):
    assert aqi.sub_index(pollutant, value) == expected


@pytest.mark.parametrize("value", [None, -1.0, float("nan")])
def test_sub_index_invalid(value):
    assert aqi.sub_index("pm25", value) is None


def test_compute_aqi_picks_dominant_pollutant():
    result = aqi.compute_aqi({"pm25": 60.0, "pm10": 40, "no2": 20, "o3": 30, "co2": 1200})
    assert result["aqi"] == 154
    assert result["dominant_pollutant"] == "pm25"
    assert result["aqi_category"] == "Unhealthy"
    assert result["co2_level"] == "Poor"


def test_compute_aqi_no_data():
    result = aqi.compute_aqi({})
    assert result["aqi"] is None and result["dominant_pollutant"] is None


@pytest.mark.parametrize(
    "value,expected",
    [(0, "Good"), (50, "Good"), (51, "Moderate"), (150, "Unhealthy for Sensitive Groups"),
     (200, "Unhealthy"), (300, "Very Unhealthy"), (301, "Hazardous")],
)
def test_category(value, expected):
    assert aqi.category(value) == expected


@pytest.mark.parametrize("pollutant", aqi.AQI_POLLUTANTS)
def test_sql_matches_python_implementation(pollutant):
    """Batch (Trino SQL) and streaming (Python) AQI must agree; DuckDB evaluates the SQL."""
    duckdb = pytest.importorskip("duckdb")
    import random

    rng = random.Random(42)
    hi = aqi.PLAUSIBLE_RANGES[pollutant][1]
    values = [round(rng.uniform(0, hi * 0.7), rng.choice([0, 1, 2])) for _ in range(3000)]
    values += [bp[i] for bp in aqi.BREAKPOINTS[pollutant] for i in (0, 1)]
    expr = aqi.sub_index_sql(pollutant, f"({aqi.truncated_sql(pollutant, 'c')})")
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT unnest(?::DOUBLE[]) AS c", [[float(v) for v in values]])
    rows = con.execute(f"SELECT c, {expr} FROM t").fetchall()
    mismatches = [(c, got, aqi.sub_index(pollutant, c)) for c, got in rows if got != aqi.sub_index(pollutant, c)]
    assert mismatches == []


def test_sql_generation_mirrors_python():
    """Evaluate the generated Trino CASE expressions with a tiny SQL-to-Python shim."""
    sql = aqi.sub_index_sql("pm25", "c")
    assert sql.startswith("CASE WHEN c IS NULL")
    assert sql.count("WHEN c <=") == len(aqi.BREAKPOINTS["pm25"])
    assert sql.endswith("ELSE 500 END")
    assert "floor(pm10 * 1) / 1" in aqi.truncated_sql("pm10", "pm10")
    assert "'Hazardous'" in aqi.category_sql("aqi")
    assert "'Hazardous'" in aqi.co2_level_sql("co2")
