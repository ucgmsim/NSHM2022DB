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
        solutions_zip_file.open(str(RUPTURE_RATES_PATH)) as rupture_rates_handle,
        solutions_zip_file.open(
            str(RUPTURE_PROPERTIES_PATH)
        ) as rupture_properties_path,
    ):


@app.command()
def main(
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
        zipfile.ZipFile(solutions_zip_path, "r") as solutions_zip_file,
        NSHMDB(sqlite_db_path) as db,
    ):
        fault_system = infer_fault_system(faults_info)

        faults = extract_faults_from_info(fault_system, faults_info)

        if not skip_faults_creation:
            db.insert_many_faults(faults)

        if not skip_mfds_creation:
            populate_mfds_table(db, solutions_zip_file, fault_system)

        if not skip_rupture_creation:
            populate_rupture_table(db, solutions_zip_file, fault_system)
