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

import copy
import difflib
import zipfile
from pathlib import Path
from typing import Annotated

import geojson
import numpy as np
import pandas as pd
import shapely
import typer
from geojson import FeatureCollection

from nshmdb.nshmdb import NSHMDB
from qcore import coordinates
from source_modelling.sources import Fault, Plane

app = typer.Typer()


FAULT_INFORMATION_PATH = Path("ruptures") / "fault_sections.geojson"
RUPTURE_FAULT_JOIN_PATH = Path("ruptures") / "fast_indices.csv"
RUPTURE_RATES_PATH = "aggregate_rates.csv"
RUPTURE_PROPERTIES_PATH = Path("ruptures") / "properties.csv"
MFDS_PATH = Path("ruptures") / "sub_seismo_on_fault_mfds.csv"


def print_array_diff(arr1: list, arr2: list) -> None:
    """Print the differences between two arrays in a readable format.

    Parameters
    ----------
    arr1 : list
        The first array to compare.
    arr2 : list
        The second array to compare.

    Returns
    -------
    None

    Example
    -------
    >>> arr1 = ["a", "b", "c"]
    >>> arr2 = ["a", "c"]
    >>> print_array_diff(arr1, arr2)
        Old: a  b  c
        New: a     c
    """
    seq_match = difflib.SequenceMatcher(a=arr1, b=arr2)
    new_line = []
    old_line = []

    for tag, i1, i2, j1, j2 in seq_match.get_opcodes():
        if tag == "equal":
            for old, new in zip(arr1[i1:i2], arr2[j1:j2]):
                new_line.append(f"{new}")
                old_line.append(f"{old}")
        elif tag == "replace":
            max_len = max(i2 - i1, j2 - j1)
            for k in range(max_len):
                old = arr1[i1 + k] if k < i2 - i1 else " " * len(str(arr2[j1 + k]))
                new = arr2[j1 + k] if k < j2 - j1 else " " * len(str(arr1[i1 + k]))
                new_line.append(f"{new}")
                old_line.append(f"{old}")
        elif tag == "delete":
            for old in arr1[i1:i2]:
                new_line.append(" " * len(f"{old}"))
                old_line.append(f"{old}")
        elif tag == "insert":
            for new in arr2[j1:j2]:
                new_line.append(f"{new}")
                old_line.append(" " * len(f"{new}"))

    print("Old: " + "  ".join(old_line))
    print("New: " + "  ".join(new_line))


def extract_faults_from_info(
    fault_info_list: FeatureCollection,
) -> dict[str, Fault]:
    """Extract the fault geometry from the fault information description.

    Parameters
    ----------
    fault_info_list : FeatureCollection
        The GeoJson object containing the fault definitions.

    Returns
    -------
    dict[str, Fault]
        A dictionary of extracted faults. The key is the name of the
        fault.
    """
    faults = {}
    for i in range(len(fault_info_list.features)):
        fault_feature = fault_info_list[i]
        fault_trace = shapely.LineString(
            coordinates.wgs_depth_to_nztm(
                np.array(list(geojson.utils.coords(fault_feature)))[:, ::-1]
            )
        )
        fault_trace_old = copy.deepcopy(fault_trace)
        fault_trace = shapely.remove_repeated_points(fault_trace, 0)
        trace_coords = np.array(fault_trace.coords)
        name = fault_feature.properties["FaultName"]
        bottom = fault_feature.properties["LowDepth"]
        dip_dir = fault_feature.properties["DipDir"]
        dip = fault_feature.properties["DipDeg"]

        if not shapely.equals_exact(fault_trace, fault_trace_old):
            old_trace = list(fault_trace_old.coords)
            new_trace = list(fault_trace.coords)
            print(f"Warning: Fault trace for {name} was altered.")
            print_array_diff(old_trace, new_trace)

        planes = []
        for j in range(len(trace_coords) - 1):
            top_left = trace_coords[j]
            top_right = trace_coords[j + 1]
            planes.append(
                Plane.from_nztm_trace(
                    np.array([top_left, top_right]),
                    0,
                    bottom,
                    dip,
                    coordinates.great_circle_bearing_to_nztm_bearing(
                        coordinates.nztm_to_wgs_depth(top_left), 1, dip_dir
                    )
                    if dip != 90
                    else 0,
                ),
            )
        faults[name] = Fault(planes)
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
    skip_mfds_creation: Annotated[
        bool, typer.Option(help="If flag is set, skip MFDS creation.")
    ] = False,
):
    """Generate the NSHM2022 rupture data from a CRU system solution package."""

    db = NSHMDB(sqlite_db_path)
    db.create()

    with (
        zipfile.ZipFile(cru_solutions_zip_path, "r") as cru_solutions_zip_file,
        db.connection() as conn,
    ):
        with cru_solutions_zip_file.open(
            str(FAULT_INFORMATION_PATH)
        ) as fault_info_handle:
            faults_info = geojson.load(fault_info_handle)

        faults = extract_faults_from_info(faults_info)
        if not skip_faults_creation:
            for i, fault in enumerate(faults.values()):
                fault_info = faults_info[i]
                fault_id = fault_info.properties["FaultID"]
                parent_id = fault_info.properties["ParentID"]
                fault_name = fault_info.properties["FaultName"]
                fault_rake = fault_info.properties["Rake"]
                conn.execute(
                    """INSERT OR REPLACE INTO parent_fault (parent_id, name) VALUES (?, ?)""",
                    (parent_id, fault_info.properties["ParentName"]),
                )
                conn.execute(
                    """INSERT OR REPLACE INTO fault (fault_id, name, rake, parent_id) VALUES (?, ?, ?, ?)""",
                    (fault_id, fault_name, fault_rake, parent_id),
                )
                conn.executemany(
                    """INSERT INTO fault_plane (
                            top_left_lat,
                            top_left_lon,
                            top_right_lat,
                            top_right_lon,
                            bottom_right_lat,
                            bottom_right_lon,
                            bottom_left_lat,
                            bottom_left_lon,
                            top_depth,
                            bottom_depth,
                            fault_id
                        ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )""",
                    [
                        (
                            *plane.corners[:, :2].ravel(),
                            plane.corners[0, 2],
                            plane.corners[-1, 2],
                            fault_id,
                        )
                        for plane in fault.planes
                    ],
                )

        if not skip_mfds_creation:
            with cru_solutions_zip_file.open(str(MFDS_PATH)) as mfds_file_handle:
                mfds = pd.read_csv(mfds_file_handle)
                mfds = mfds.rename(columns={"Section Index": "fault_id"})
                mfds = mfds.melt(
                    id_vars=["fault_id"], var_name="magnitude", value_name="rate"
                )
                mfds = mfds[mfds["rate"] > 0]
                mfds.to_sql(
                    "magnitude_frequency_distribution",
                    conn,
                    index=False,
                    if_exists="append",
                )

        if not skip_rupture_creation:
            with (
                cru_solutions_zip_file.open(
                    str(RUPTURE_FAULT_JOIN_PATH)
                ) as rupture_fault_join_handle,
                cru_solutions_zip_file.open(
                    str(RUPTURE_RATES_PATH)
                ) as rupture_rates_handle,
                cru_solutions_zip_file.open(
                    str(RUPTURE_PROPERTIES_PATH)
                ) as rupture_properties_path,
            ):
                rupture_rates = pd.read_csv(rupture_rates_handle).set_index(
                    "Rupture Index"
                )
                rupture_properties = pd.read_csv(rupture_properties_path).set_index(
                    "Rupture Index"
                )
                rupture_properties = rupture_properties.join(rupture_rates)
                rupture_properties = rupture_properties.rename(
                    columns={
                        "Magnitude": "magnitude",
                        "Area (m^2)": "area",
                        "Length (m)": "len",
                        "rate_weighted_mean": "rate",
                    }
                )
                rupture_properties = rupture_properties[
                    ["magnitude", "area", "len", "rate"]
                ]
                rupture_properties.to_sql(
                    "rupture",
                    conn,
                    index=True,
                    index_label="rupture_id",
                    if_exists="append",
                )

                rupture_fault_join_df = pd.read_csv(rupture_fault_join_handle)
                rupture_fault_join_df["section"] = rupture_fault_join_df[
                    "section"
                ].astype("Int64")
                rupture_fault_join_df = rupture_fault_join_df.rename(
                    columns={"section": "fault_id", "rupture": "rupture_id"}
                )
                rupture_fault_join_df.to_sql(
                    "rupture_faults", conn, index=False, if_exists="append"
                )
