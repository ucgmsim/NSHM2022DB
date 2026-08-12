import duckdb
import pytest


@pytest.fixture(scope="session", autouse=True)
def _install_duckdb_sqlite_extension():
    duckdb.sql("INSTALL sqlite")
