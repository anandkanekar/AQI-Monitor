"""Render the Airflow SQL templates exactly as Airflow would and validate them with a Trino SQL parser."""
from pathlib import Path

import pytest

jinja2 = pytest.importorskip("jinja2")
sqlglot = pytest.importorskip("sqlglot")

from aqi_common.aqi import category_sql, co2_level_sql, plausible_sql, sub_index_sql, truncated_sql  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "airflow" / "dags" / "sql"
MACROS = dict(
    plausible_sql=plausible_sql,
    truncated_sql=truncated_sql,
    sub_index_sql=sub_index_sql,
    category_sql=category_sql,
    co2_level_sql=co2_level_sql,
)


def render(name, target_date=None):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(SQL_DIR)), undefined=jinja2.StrictUndefined)
    return env.get_template(name).render(ds="2026-09-22", params={"target_date": target_date}, **MACROS)


@pytest.mark.parametrize("name", sorted(p.name for p in SQL_DIR.glob("*.sql")))
def test_template_renders_to_valid_trino_sql(name):
    sql = render(name)
    assert "{{" not in sql and "{%" not in sql
    assert "'2026-09-22'" in sql
    statements = sqlglot.parse(sql, read="trino")
    assert len(statements) == 1
    assert statements[0].key == "insert"


def test_target_date_param_overrides_ds():
    assert "'2026-01-15'" in render("daily_sensor_aqi.sql", target_date="2026-01-15")


def test_ddl_parses():
    sql = (ROOT / "trino" / "sql" / "ddl.sql").read_text()
    statements = [s for s in sqlglot.parse(sql, read="trino") if s is not None]
    assert len(statements) == 9
