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
"""

import copy
import difflib
import zipfile
from pathlib import Path
from typing import Annotated
from zipfile import ZipFile

import geojson
import numpy as np
import pandas as pd
import shapely
import typer
from geojson import FeatureCollection
from rich.console import Console
from rich.table import Table

from nshmdb.nshmdb import NSHMDB, FaultInfo, FaultSystem
from qcore import coordinates
from source_modelling.sources import Fault, Plane

app = typer.Typer()


FAULT_INFORMATION_PATH = Path("ruptures") / "fault_sections.geojson"
RUPTURE_FAULT_JOIN_PATH = Path("ruptures") / "fast_indices.csv"
RUPTURE_RATES_PATH = "aggregate_rates.csv"
RUPTURE_PROPERTIES_PATH = Path("ruptures") / "properties.csv"
MFDS_PATH = Path("ruptures") / "sub_seismo_on_fault_mfds.csv"


def print_array_diff(arr1: list, arr2: list) -> None:
    console = Console()
    seq_match = difflib.SequenceMatcher(a=arr1, b=arr2)

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Segment")

    old_row = ["Old"]
    new_row = ["New"]

    for tag, i1, i2, j1, j2 in seq_match.get_opcodes():
        if tag == "equal":
            for val in arr1[i1:i2]:
                old_row.append(str(val))
                new_row.append(str(val))
        elif tag == "replace":
            # Highlight replacements
            for i in range(max(i2 - i1, j2 - j1)):
                old_val = str(arr1[i1 + i]) if i < (i2 - i1) else ""
                new_val = str(arr2[j1 + i]) if i < (j2 - j1) else ""
                old_row.append(f"[red]{old_val}[/red]")
                new_row.append(f"[green]{new_val}[/green]")
        elif tag == "delete":
            for val in arr1[i1:i2]:
                old_row.append(f"[red]{val}[/red]")
                new_row.append("")
        elif tag == "insert":
            for val in arr2[j1:j2]:
                old_row.append("")
                new_row.append(f"[green]{val}[/green]")

    table.add_row(*old_row)
    table.add_row(*new_row)

    console.print(table)


def extract_faults_from_info(
    fault_system: FaultSystem,
    fault_info_list: FeatureCollection,
) -> list[FaultInfo]:
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
    faults = []
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
        fault_id = fault_feature.properties["FaultID"]
        name = fault_feature.properties["ParentName"]
        top = fault_feature.properties["UpDepth"]
        bottom = fault_feature.properties["LowDepth"]
        dip_dir = fault_feature.properties["DipDir"]
        dip = fault_feature.properties["DipDeg"]
        rake = fault_feature.properties["Rake"]

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
                    top,
                    bottom,
                    dip,
                    coordinates.great_circle_bearing_to_nztm_bearing(
                        coordinates.nztm_to_wgs_depth(top_left), 1, dip_dir
                    )
                    if dip != 90
                    else 0,
                ),
            )
        faults.append(
            FaultInfo(
                fault_id=fault_id,
                fault_system=fault_system,
                name=name,
                rake=rake,
                tect_type=None,
                fault=Fault(planes),
            )
        )
    return faults


HIKURANGI_NAME = "Hikurangi, Kermadec to Louisville ridge, 30km - with slip deficit smoothed near East Cape and locked near trench."
PUYSEGUR_NAME = "Puysegur, 15km, 50% coupling, corrected dip direction"


def infer_fault_system(geojson: FeatureCollection) -> FaultSystem:
    """Infer the fault system from an NSHM 2022 feature collection."""
    features = geojson.features
    test_feature = features[0]
    name = test_feature.properties["ParentName"]
    if name == HIKURANGI_NAME:
        return FaultSystem.Hikurangi
    elif name == PUYSEGUR_NAME:
        return FaultSystem.Puysegur
    return FaultSystem.Crustal


def populate_mfds_table(
    db: NSHMDB, solutions_zip_file: ZipFile, fault_system: FaultSystem
) -> None:
    with solutions_zip_file.open(str(MFDS_PATH)) as mfds_file_handle:
        mfds = pd.read_csv(mfds_file_handle)
        mfds = mfds.rename(columns={"Section Index": "nshm_id"})
        mfds = mfds.melt(id_vars=["nshm_id"], var_name="magnitude", value_name="rate")
        mfds = mfds[mfds["rate"] > 0]
        mfds["fault_system"] = fault_system
        db.insert_magnitude_frequency_distribution(mfds)


def populate_rupture_table(
    db: NSHMDB, solutions_zip_file: ZipFile, fault_system: FaultSystem
) -> None:
    with (
        solutions_zip_file.open(
            str(RUPTURE_FAULT_JOIN_PATH)
        ) as rupture_fault_join_handle,
        solutions_zip_file.open(str(RUPTURE_RATES_PATH)) as rupture_rates_handle,
        solutions_zip_file.open(
            str(RUPTURE_PROPERTIES_PATH)
        ) as rupture_properties_path,
    ):
        rupture_rates = pd.read_csv(rupture_rates_handle).set_index("Rupture Index")
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
        rupture_properties = rupture_properties[["magnitude", "area", "len", "rate"]]
        rupture_properties["fault_system"] = fault_system

        rupture_fault_join_df = pd.read_csv(rupture_fault_join_handle)
        rupture_fault_join_df["section"] = rupture_fault_join_df["section"].astype(
            "Int64"
        )
        rupture_fault_join_df = rupture_fault_join_df.rename(
            columns={"section": "fault_id", "rupture": "rupture_id"}
        )
        rupture_fault_join_df["fault_system"] = fault_system
        db.insert_many_ruptures(rupture_properties, rupture_fault_join_df)


@app.command()
def main(
    cru_solutions_zip_path: Annotated[
        Path,
        typer.Argument(
            help="NSHM solutions zip file", readable=True, dir_okay=False, exists=True
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

    with (
        zipfile.ZipFile(cru_solutions_zip_path, "r") as solutions_zip_file,
        NSHMDB(sqlite_db_path) as db,
    ):
        with solutions_zip_file.open(str(FAULT_INFORMATION_PATH)) as fault_info_handle:
            faults_info = geojson.load(fault_info_handle)

        fault_system = infer_fault_system(faults_info)

        faults = extract_faults_from_info(fault_system, faults_info)

        if not skip_faults_creation:
            db.insert_many_faults(faults)

        if not skip_mfds_creation:
            populate_mfds_table(db, solutions_zip_file, fault_system)

        if not skip_rupture_creation:
            populate_rupture_table(db, solutions_zip_file, fault_system)
