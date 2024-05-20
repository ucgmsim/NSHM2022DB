#!/usr/bin/env python3
"""
NSHM2022 Rupture Data Generation Script

This script generates NSHM2022 rupture data from a CRU system solution package.

Usage:
    python script_name.py [OPTIONS] CRU_SOLUTIONS_ZIP_PATH SQLITE_DB_PATH

Arguments:
    CRU_SOLUTIONS_ZIP_PATH : str
        Path to the CRU solutions zip file.
    SQLITE_DB_PATH : str
        Output SQLite DB path.

Options:
    --skip-faults-creation : bool, optional
        If flag is set, skip fault creation.
    --skip-rupture-creation : bool, optional
        If flag is set, skip rupture creation.

Example:
    python generate_nshm2022_data.py data/cru_solutions.zip output/nshm2022.sqlite
"""
import zipfile
from pathlib import Path
from typing import Annotated

from nshmdb.fault import Fault
from nshmdb.nshmdb import NSHMDB

import geojson
import nshmdb.fault
import numpy as np
import pandas as pd
import qcore.geo
import tqdm
import typer
from geojson import FeatureCollection

app = typer.Typer()


POLYGONS_PATH = Path("ruptures") / "sect_polygons.geojson"
FAULT_INFORMATION_PATH = Path("ruptures") / "fault_sections.geojson"
RUPTURE_FAULT_JOIN_PATH = Path("ruptures") / "fast_indices.csv"


def extract_faults_from_info(
    fault_info_list: FeatureCollection,
) -> list[Fault]:
    """Extract the fault geometry from the fault information description.

    Parameters
    ----------
    fault_info_list : FeatureCollection
        The GeoJson object containing the fault definitions.

    Returns
    -------
    list[Fault]
        The list of extracted faults.
    """
    faults = []
    for i in range(len(fault_info_list.features)):
        fault_feature = fault_info_list[i]
        fault_trace = list(geojson.utils.coords(fault_feature))
        name = fault_feature.properties["FaultName"]
        dip_dir = fault_feature.properties["DipDir"]
        dip = fault_feature.properties["DipDeg"]
        bottom = fault_feature.properties["LowDepth"]
        if dip == 90:
            projected_width = 0
        else:
            projected_width = bottom / np.tan(np.radians(dip))
        rake = fault_feature.properties["Rake"]
        planes = []
        for i in range(len(fault_trace) - 1):
            top_left = fault_trace[i][::-1]
            top_right = fault_trace[i + 1][::-1]
            bottom_left = qcore.geo.ll_shift(*top_left, projected_width, dip_dir)
            bottom_right = qcore.geo.ll_shift(*top_right, projected_width, dip_dir)
            corners = np.array([top_left, top_right, bottom_right, bottom_left])
            corners = np.append(
                corners,
                np.array([0, 0, bottom * 1000, bottom * 1000]).reshape((-1, 1)),
                axis=1,
            )
            planes.append(nshmdb.fault.FaultPlane(corners, rake))
        faults.append(Fault(name, None, planes))
    return faults


@app.command()
def main(
    cru_solutions_zip_path: Annotated[
        Path,
        typer.Argument(
            help="CRU solutions zip file", readable=True, dir_okay=False, exists=True
        ),
    ],
    sqlite_db_path: Annotated[
        Path,
        typer.Argument(help="Output SQLite DB path", writable=True, dir_okay=False),
    ],
    skip_faults_creation: Annotated[
        bool, typer.Option(help="If flag is set, skip fault creation.")
    ] = False,
    skip_rupture_creation: Annotated[
        bool, typer.Option(help="If flag is set, skip rupture creation.")
    ] = False,
):
    """Generate the NSHM2022 rupture data from a CRU system solution package."""

    db = NSHMDB(sqlite_db_path)
    db.create()

    with zipfile.ZipFile(
        cru_solutions_zip_path, "r"
    ) as cru_solutions_zip_file, db.connection() as conn:

        with cru_solutions_zip_file.open(
            str(FAULT_INFORMATION_PATH)
        ) as fault_info_handle:
            faults_info = geojson.load(fault_info_handle)

        faults = extract_faults_from_info(faults_info)

        if not skip_faults_creation:
            for i, fault in enumerate(faults):
                fault_info = faults_info[i]
                parent_id = fault_info.properties["ParentID"]
                db.insert_parent(conn, parent_id, fault_info.properties["ParentName"])
                db.insert_fault(
                    conn, fault_info.properties["FaultID"], parent_id, fault
                )
        if not skip_rupture_creation:
            with cru_solutions_zip_file.open(
                str(RUPTURE_FAULT_JOIN_PATH)
            ) as rupture_fault_join_handle:
                rupture_fault_join_df = pd.read_csv(rupture_fault_join_handle)
                rupture_fault_join_df["section"] = rupture_fault_join_df[
                    "section"
                ].astype("Int64")
                for _, row in tqdm.tqdm(
                    rupture_fault_join_df.iterrows(),
                    desc="Binding ruptures to faults",
                    total=len(rupture_fault_join_df),
                ):
                    db.add_fault_to_rupture(conn, row["rupture"], row["section"])


if __name__ == "__main__":
    app()
