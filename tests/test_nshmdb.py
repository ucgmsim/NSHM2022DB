from collections.abc import Generator
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from nshmdb.nshmdb import NSHMDB, FaultInfo, FaultSystem, Rupture
from qcore import coordinates
from source_modelling.sources import Fault, Plane


@pytest.fixture
def test_db(tmp_path: Path) -> Generator[NSHMDB, None, None]:
    """Fixture to create a temporary database for testing."""
    db_path = tmp_path / "test_nshm.db"
    with NSHMDB(db_path) as db:
        yield db


@pytest.fixture
def alpine_fault_nshmdb(test_db: NSHMDB) -> NSHMDB:
    test_db.connection().executescript("""
    INSERT INTO parent_fault (parent_id, name) VALUES (1, 'Alpine Fault');
    INSERT INTO fault (fault_id, fault_system, nshm_id, rake, tect_type, parent_id) VALUES (1, 3, 1, 90.0, NULL, 1);
    INSERT INTO fault_plane (plane_id, top_left_lat, top_left_lon, top_right_lat, top_right_lon,
    bottom_right_lat, bottom_right_lon, bottom_left_lat, bottom_left_lon, top_depth, bottom_depth, fault_id)
    VALUES (1, -42.0, 172.0, -42.0, 173.0, -43.0, 173.0, -43.0, 172.0, 0.0, 10.0, 1);
    INSERT INTO rupture (rupture_id, fault_system, nshm_id, area, magnitude, len, rate) VALUES (1, 3, 1, 100.0, 6.5, 10.0, 0.01);
    INSERT INTO rupture_faults (rupture_fault_id, rupture_id, fault_id) VALUES (1, 1, 1);
    INSERT INTO magnitude_frequency_distribution (entry_id, fault_id, magnitude, rate) VALUES (1, 1, 6.5, 0.01);
    """)
    return test_db


def test_get_rupture(test_db: NSHMDB):
    """Test retrieving a rupture."""
    test_db.connection().execute(
        "INSERT INTO rupture (rupture_id, fault_system, nshm_id, magnitude, area, len, rate) VALUES (1, 3, 1, 6.5, 25.0, 10.0, 0.01)"
    )
    rupture = test_db.get_rupture(FaultSystem.Crustal, 1)
    assert rupture == Rupture(
        fault_system=FaultSystem.Crustal,
        rupture_id=1,
        magnitude=6.5,
        area=25.0,
        length=10.0,
        rate=0.01,
        faults={},
    )


def test_get_fault_names(test_db: NSHMDB):
    """Test retrieving all fault names."""
    conn = test_db.connection()
    conn.execute("INSERT INTO parent_fault (parent_id, name) VALUES (1, 'Fault A')")
    conn.execute("INSERT INTO parent_fault (parent_id, name) VALUES (2, 'Fault B')")

    fault_names = test_db.get_fault_names()
    assert fault_names == {"Fault A", "Fault B"}


def test_get_fault(test_db: NSHMDB):
    with test_db.connection() as conn:
        conn.executescript("""
        INSERT INTO parent_fault (parent_id, name) VALUES (1, 'Test Fault');
        INSERT INTO fault (fault_id, fault_system, nshm_id, rake, parent_id) VALUES (1, 3, 1, 90.0, 1);
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
            fault_system=FaultSystem.Crustal,
            fault_id=1,
            name="Alpine Fault",
            rake=90.0,
            tect_type=None,
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
    conn = test_db.connection()
    conn.execute("INSERT INTO parent_fault (parent_id, name) VALUES (1, 'Fault A')")
    conn.execute(
        "INSERT INTO fault (fault_id, fault_system, nshm_id, rake, parent_id) VALUES (1, 3, 1, 90.0, 1)"
    )

    fault_info = test_db.get_fault_info(FaultSystem.Crustal, 1)
    assert fault_info == FaultInfo(
        fault_system=FaultSystem.Crustal,
        fault_id=1,
        name="Fault A",
        rake=90.0,
        tect_type=None,
    )


def test_connection_not_connected(tmp_path: Path):
    """Test that connection() raises ConnectionError before entering context."""
    db = NSHMDB(tmp_path / "test.db")
    with pytest.raises(ConnectionError):
        db.connection()


def test_create_standalone(tmp_path: Path):
    """Test that create() works standalone (not inside a context manager)."""
    db_path = tmp_path / "standalone.db"
    db = NSHMDB(db_path)
    db.create()
    assert db_path.exists()


def test_get_fault_ids(alpine_fault_nshmdb: NSHMDB):
    """Test retrieving all NSHM fault IDs."""
    fault_ids = alpine_fault_nshmdb.get_fault_ids()
    assert fault_ids == {1}


def test_get_fault_info_not_found(test_db: NSHMDB):
    """Test that get_fault_info raises ValueError for a nonexistent fault."""
    with pytest.raises(ValueError, match="Could not find fault"):
        test_db.get_fault_info(FaultSystem.Crustal, 999)


def test_insert_many_faults_without_planes(test_db: NSHMDB):
    """Test bulk-inserting faults that have no plane geometry."""
    faults = [
        FaultInfo(
            fault_system=FaultSystem.Crustal,
            fault_id=1,
            name="Fault A",
            rake=90.0,
            tect_type=None,
            fault=None,
        ),
        FaultInfo(
            fault_system=FaultSystem.Crustal,
            fault_id=2,
            name="Fault B",
            rake=45.0,
            tect_type=1,
            fault=None,
        ),
    ]
    test_db.insert_many_faults(faults)
    assert test_db.get_fault_names() == {"Fault A", "Fault B"}


def test_insert_many_faults_with_planes(test_db: NSHMDB):
    """Test bulk-inserting faults that include plane geometry."""
    corners = np.array(
        [
            [-42.0, 172.0, 0.0],
            [-42.0, 173.0, 0.0],
            [-43.0, 173.0, 10.0],
            [-43.0, 172.0, 10.0],
        ]
    )
    plane = Plane(coordinates.wgs_depth_to_nztm(corners))
    fault_geom = Fault([plane])

    faults = [
        FaultInfo(
            fault_system=FaultSystem.Crustal,
            fault_id=1,
            name="Alpine Fault",
            rake=90.0,
            tect_type=None,
            fault=fault_geom,
        ),
    ]
    test_db.insert_many_faults(faults)
    assert test_db.get_fault_names() == {"Alpine Fault"}
    assert test_db.get_fault_ids() == {1}


def test_insert_many_ruptures(test_db: NSHMDB):
    """Test bulk-inserting ruptures and their fault associations."""
    faults = [
        FaultInfo(
            fault_system=FaultSystem.Crustal,
            fault_id=1,
            name="Fault A",
            rake=90.0,
            tect_type=None,
            fault=None,
        ),
    ]
    test_db.insert_many_faults(faults)

    ruptures = pd.DataFrame(
        {
            "magnitude": [6.5],
            "area": [100.0],
            "len": [10.0],
            "rate": [0.01],
            "fault_system": [int(FaultSystem.Crustal)],
        },
        index=pd.Index([1], name="nshm_id"),
    )
    rupture_faults = pd.DataFrame(
        {
            "rupture_id": [1],
            "fault_id": [1],
            "fault_system": [int(FaultSystem.Crustal)],
        }
    )
    test_db.insert_many_ruptures(ruptures, rupture_faults)

    rupture = test_db.get_rupture(FaultSystem.Crustal, 1)
    assert rupture.magnitude == 6.5
    assert rupture.rate == 0.01


def test_insert_magnitude_frequency_distribution(test_db: NSHMDB):
    """Test bulk-inserting magnitude frequency distribution entries."""
    faults = [
        FaultInfo(
            fault_system=FaultSystem.Crustal,
            fault_id=1,
            name="Fault A",
            rake=90.0,
            tect_type=None,
            fault=None,
        ),
    ]
    test_db.insert_many_faults(faults)

    mfds = pd.DataFrame(
        {
            "nshm_id": [1],
            "fault_system": [int(FaultSystem.Crustal)],
            "magnitude": [6.5],
            "rate": [0.01],
        }
    )
    test_db.insert_magnitude_frequency_distribution(mfds)

    conn = test_db.connection()
    result = conn.execute(
        "SELECT magnitude, rate FROM magnitude_frequency_distribution"
    ).fetchall()
    assert len(result) == 1
    assert result[0] == (6.5, 0.01)
