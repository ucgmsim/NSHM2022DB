from pathlib import Path

import numpy as np
import pytest
from nshmdb.nshmdb import NSHMDB, FaultInfo, Rupture


@pytest.fixture
def test_db(tmp_path: Path) -> NSHMDB:
    """Fixture to create a temporary database for testing."""
    db_path = tmp_path / "test_nshm.db"
    nshm_db = NSHMDB(db_path)
    nshm_db.create()

    return nshm_db


@pytest.fixture
def alpine_fault_nshmdb(test_db: NSHMDB) -> NSHMDB:
    with test_db.connection() as conn:
        conn.executescript("""
        INSERT INTO parent_fault (parent_id, name) VALUES (1, 'Alpine Fault');
        INSERT INTO fault (fault_id, name, parent_id, rake, tect_type) VALUES (1, 'Segment 1', 1, 90.0, NULL);
        INSERT INTO fault_plane (plane_id, top_left_lat, top_left_lon, top_right_lat, top_right_lon,
        bottom_right_lat, bottom_right_lon, bottom_left_lat, bottom_left_lon, top_depth, bottom_depth, fault_id)
        VALUES (1, -42.0, 172.0, -42.0, 173.0, -43.0, 173.0, -43.0, 172.0, 0.0, 10.0, 1);
        INSERT INTO rupture (rupture_id, area, magnitude, len, rate) VALUES (1, 100.0, 6.5, 10.0, 0.01);
        INSERT INTO rupture_faults (rupture_fault_id, rupture_id, fault_id) VALUES (1, 1, 1);
        INSERT INTO magnitude_frequency_distribution (entry_id, fault_id, magnitude, rate) VALUES (1, 1, 6.5, 0.01);
        """)
    return test_db


def test_add_rupture(test_db: NSHMDB):
    """Test adding a rupture to the database."""
    with test_db.connection() as conn:
        test_db.add_rupture(conn, 1, 6.5, 25.0, 10.0, 0.01)

        result = conn.execute("SELECT * FROM rupture WHERE rupture_id = 1").fetchone()
        assert result == (1, 25.0, 6.5, 10.0, 0.01)


def test_add_fault_to_rupture(test_db: NSHMDB):
    """Test adding a fault to a rupture."""
    with test_db.connection() as conn:
        conn.execute(
            "INSERT INTO fault (fault_id, name, parent_id, rake) VALUES (1, 'Fault A', 0, 90.0)"
        )
        test_db.add_fault_to_rupture(conn, 1, 1)

        rupture_faults = conn.execute(
            "SELECT * FROM rupture_faults WHERE rupture_id = 1 AND fault_id = 1"
        ).fetchone()
        assert rupture_faults == (1, 1, 1)


def test_get_rupture(test_db: NSHMDB):
    """Test retrieving a rupture."""
    with test_db.connection() as conn:
        conn.execute(
            "INSERT INTO rupture (rupture_id, magnitude, area, len, rate) VALUES (1, 6.5, 25.0, 10.0, 0.01)"
        )

    rupture = test_db.get_rupture(1)
    assert rupture == Rupture(
        rupture_id=1, magnitude=6.5, area=25.0, length=10.0, rate=0.01, faults={}
    )


def test_get_fault_names(test_db: NSHMDB):
    """Test retrieving all fault names."""
    with test_db.connection() as conn:
        conn.execute("INSERT INTO parent_fault (parent_id, name) VALUES (1, 'Fault A')")
        conn.execute("INSERT INTO parent_fault (parent_id, name) VALUES (2, 'Fault B')")

    fault_names = test_db.get_fault_names()
    assert fault_names == {"Fault A", "Fault B"}


def test_get_fault(test_db: NSHMDB):
    with test_db.connection() as conn:
        conn.executescript("""
        INSERT INTO fault_plane (plane_id, top_left_lat, top_left_lon, top_right_lat, top_right_lon,
        bottom_right_lat, bottom_right_lon, bottom_left_lat, bottom_left_lon, top_depth, bottom_depth, fault_id)
        VALUES (1, -42.0, 172.0, -42.0, 173.0, -43.0, 173.0, -43.0, 172.0, 0.0, 10.0, 1);
        """)
    fault = test_db.get_fault(1)
    assert np.allclose(
        fault.corners,
        np.array(
            [
                [-42.0, 172.0, 0.0],
                [-42.0, 173.0, 0.0],
                [-43.0, 173.0, 10.0],
                [-43.0, 172.0, 10.0],
            ]
        ),
    )


def test_get_rupture_faults(alpine_fault_nshmdb: NSHMDB):
    """Test retrieving faults associated with a rupture."""
    faults = alpine_fault_nshmdb.get_rupture_faults(1)
    assert set(faults) == {"Alpine Fault"}
    fault = faults["Alpine Fault"]
    assert len(fault.planes) == 1
    assert np.allclose(
        fault.corners,
        np.array(
            [
                [-42.0, 172.0, 0.0],
                [-42.0, 173.0, 0.0],
                [-43.0, 173.0, 10.0],
                [-43.0, 172.0, 10.0],
            ]
        ),
    )


def test_get_rupture_fault_info(alpine_fault_nshmdb: NSHMDB):
    """Test retrieving faults associated with a rupture."""

    faults = alpine_fault_nshmdb.get_rupture_fault_info(1)
    assert faults == {
        "Alpine Fault": FaultInfo(
            fault_id=1, name="Segment 1", parent_id=1, rake=90.0, tect_type=None
        )
    }


def test_query(alpine_fault_nshmdb: NSHMDB):
    ruptures = alpine_fault_nshmdb.query("Alpine Fault")
    assert set(ruptures) == {1}
    rupture = ruptures[1]
    assert rupture.rupture_id == 1
    assert rupture.magnitude == 6.5
    assert rupture.rate == 0.01
    assert set(rupture.faults) == {"Alpine Fault"}


def test_rates(alpine_fault_nshmdb: NSHMDB):
    assert alpine_fault_nshmdb.most_likely_fault(1, {"Alpine Fault": 6.5}) == {
        "Alpine Fault": 0.01
    }


def test_get_fault_info(test_db: NSHMDB):
    """Test retrieving fault information."""
    with test_db.connection() as conn:
        conn.execute(
            "INSERT INTO fault (fault_id, name, parent_id, rake) VALUES (1, 'Fault A', 0, 90.0)"
        )

    fault_info = test_db.get_fault_info(1)
    assert fault_info == FaultInfo(
        fault_id=1, name="Fault A", parent_id=0, rake=90.0, tect_type=None
    )
