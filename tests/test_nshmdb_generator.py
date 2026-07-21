from pathlib import Path

import geojson

from nshmdb.nshmdb import NSHMDB, FaultSystem
from nshmdb.scripts import nshm_db_generator
from nshmdb.scripts.nshm_db_generator import (
    HIKURANGI_NAME,
    PUYSEGUR_NAME,
    infer_fault_system,
)

CRU_FAULT_SOLUTIONS = Path("tests") / "CRU_fault_system_solution_small.zip"


def test_nshmdb_generator(tmp_path: Path):
    nshmdb_path = tmp_path / "nhsmdb.db"
    nshm_db_generator.main(CRU_FAULT_SOLUTIONS, nshmdb_path)
    with NSHMDB(nshmdb_path) as db:
        rupture = db.get_rupture(FaultSystem.Crustal, 3)
        assert rupture.rupture_nshm_id == 3
        assert rupture.magnitude == 7.2375555
        assert rupture.rate == 1.012588e-05
        assert rupture.area == 1090332700
        assert rupture.length == 34817.69
        assert set(rupture.faults) == {"Acton"}


CRU_FAULT_SOLUTIONS_FULL = Path("tests") / "CRU_fault_system_solution.zip"


def test_nshmdb_generator_runs(tmp_path: Path):
    nshmdb_path = tmp_path / "nhsmdb.db"
    nshm_db_generator.main(CRU_FAULT_SOLUTIONS_FULL, nshmdb_path)


def _make_feature_collection(parent_name: str) -> geojson.FeatureCollection:
    feature = geojson.Feature(
        geometry=None, properties={"ParentName": parent_name}
    )
    return geojson.FeatureCollection(features=[feature])


def test_infer_fault_system_hikurangi():
    fc = _make_feature_collection(HIKURANGI_NAME)
    assert infer_fault_system(fc) == FaultSystem.Hikurangi


def test_infer_fault_system_puysegur():
    fc = _make_feature_collection(PUYSEGUR_NAME)
    assert infer_fault_system(fc) == FaultSystem.Puysegur


def test_infer_fault_system_crustal():
    fc = _make_feature_collection("Alpine Fault")
    assert infer_fault_system(fc) == FaultSystem.Crustal
