#!/usr/bin/env python3
import functools
import zipfile
from pathlib import Path
from typing import Annotated

import geojson
import nshmdb.fault
import numpy as np
import pandas as pd
import qcore.geo
import typer
from geojson import FeatureCollection
from nshmdb.fault import Fault
from nshmdb.nshmdb import NSHMDB

app = typer.Typer()


POLYGONS_PATH = Path("ruptures") / "sect_polygons.geojson"
FAULT_INFORMATION_PATH = Path("ruptures") / "fault_sections.geojson"
RUPTURE_FAULT_JOIN_PATH = Path("ruptures") / "fast_indices.csv"


def extract_faults_from_info(
    fault_info_list: FeatureCollection,
) -> list[Fault]:
    faults = []
    for fault_feature in fault_info_list:
        fault_trace = geojson.utils.coords(fault_feature)
        name = fault_feature.properties["FaultName"]
        dip_dir = fault_feature.properties["DipDir"]
        dip = fault_feature.properties["DipDeg"]
        width = fault_feature.properties["Width"]
        projected_width = width * np.cos(np.radians(dip))
        bottom = fault_feature.properties["LowDepth"]
        rake = fault_feature.properties["Rake"]

        planes = []
        for i in range(len(fault_trace) - 1):
            top_left = fault_trace[i][::-1]
            top_right = fault_trace[i + 1][::-1]
            bottom_left = qcore.geo.ll_shift(*top_left, dip_dir, projected_width)
            bottom_right = qcore.geo.ll_shift(*top_right, dip_dir, projected_width)
            corners = np.array([top_left, top_right, bottom_right, bottom_left])
            corners = np.append(corners, np.array([0, 0, bottom, bottom]), axis=1)
            planes.append(nshmdb.fault.FaultPlane(corners, rake))
        faults.append(Fault(name, None, planes))
    return faults


def insert_rupture(db: NSHMDB, rupture_df: pd.DataFrame):
    rupture_id = rupture_df["rupture"].iloc[0]
    rupture_faults = rupture_df["section"].to_list()
    db.insert_rupture(rupture_id, rupture_faults)


@app.command()
def main(
    cru_solutions_zip_path: Annotated[
        Path,
        typer.Argument(
            help="CRU solutions zip file", readable=True, dir_ok=False, exists=True
        ),
    ],
    sqlite_db_path: Annotated[
        Path, typer.Argument(help="Output SQLite DB path", writable=True, exists=True)
    ],
):
    """Generate the NSHM2022 rupture data from a CRU system solution package."""

    db = NSHMDB(sqlite_db_path)
    db.create()

    with zipfile.ZipFile(cru_solutions_zip_path, "r") as cru_solutions_zip_file:

        with cru_solutions_zip_file.open(
            str(FAULT_INFORMATION_PATH)
        ) as fault_info_handle:
            faults_info = geojson.load(fault_info_handle)

        faults = extract_faults_from_info(faults_info)

        with cru_solutions_zip_file.open(
            str(RUPTURE_FAULT_JOIN_PATH)
        ) as rupture_fault_join_handle:
            rupture_fault_join_df = pd.read_csv(rupture_fault_join_handle)
            rupture_fault_join_df["section"] = rupture_fault_join_df["section"].astype(
                "Int64"
            )

    for i, fault in enumerate(faults):
        fault_info = faults_info[i]
        parent_id = fault_info.proprties["ParentID"]
        db.insert_parent(parent_id, fault_info.properties["ParentName"])
        db.insert_fault(fault_info.properties["FaultID"], parent_id, fault)

    rupture_fault_join_df.groupby("rupture").apply(
        functools.partial(insert_rupture, db=db)
    )


if __name__ == "__main__":
    app()
