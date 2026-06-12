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

from pathlib import Path
from typing import Annotated

import typer

from nshmdb import api
from nshmdb.nshmdb import NSHMDB

app = typer.Typer()


def _parse_version(version: str) -> tuple[int, int, int]:
    match version.split("."):
        case [major, minor, patch]:
            return int(major), int(minor), int(patch)
        case _:
            raise ValueError(f"Cannot parse version {version!r}")


@app.command()
def main(
    version: Annotated[str, typer.Argument(help="NSHM version to download")],
    sqlite_db_path: Annotated[
        Path,
        typer.Argument(help="Output SQLite DB path", writable=True, dir_okay=False),
    ],
    api_key: Annotated[str, typer.Option(help="Weka API key", envvar="NSHMDB_API_KEY")],
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
    nshm_version = _parse_version(version)
    solution = api.download_composite_solution(api_key, nshm_version)
    with NSHMDB(sqlite_db_path) as db:
        if not skip_faults_creation:
            db.insert_many_faults(solution.faults)
        if not skip_rupture_creation:
            db.insert_many_ruptures(
                solution.rupture_properties, solution.rupture_join_table
            )
        if (
            not skip_mfds_creation
            and solution.magnitude_frequency_distribution is not None
        ):
            db.insert_magnitude_frequency_distribution(
                solution.magnitude_frequency_distribution
            )
