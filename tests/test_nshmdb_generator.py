from pathlib import Path

import numpy as np
from nshmdb.nshmdb import NSHMDB
from nshmdb.scripts import nshm_db_generator

CRU_FAULT_SOLUTIONS = Path("tests") / "CRU_fault_system_solution.zip"


def test_nshmdb_generator(tmp_path: Path):
    nshmdb_path = tmp_path / "nhsmdb.db"
    nshm_db_generator.main(CRU_FAULT_SOLUTIONS, nshmdb_path)
    db = NSHMDB(nshmdb_path)
    rupture = db.get_rupture(3)
    assert rupture.rupture_id == 3
    assert rupture.magnitude == 7.2375555
    assert rupture.rate == 1.012588e-05
    assert rupture.area == 1090332700
    assert rupture.length == 34817.69
    assert set(rupture.faults) == {"Acton"}
