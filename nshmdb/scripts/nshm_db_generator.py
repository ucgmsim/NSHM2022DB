#!/usr/bin/env python3
"""
NSHM2022 rupture data generation script.

Fetches NSHM logic tree solutions via the Weka API and composites them
into a unified SQLite database payload.
"""

from pathlib import Path
from typing import Annotated

import typer

from nshmdb import api
from nshmdb.nshmdb import NSHMDB
from qcore import cli

app = typer.Typer()


def _parse_version(version: str) -> tuple[int, int, int]:
    """
    Parses a semantic version string into a tuple of integers.

    Parameters
    ----------
    version : str
        The version string in "major.minor.patch" format.

    Returns
    -------
    tuple[int, int, int]
        The parsed (major, minor, patch) version integers.

    Raises
    ------
    ValueError
        If the version string does not strictly match the expected format.
    """
    match version.split("."):
        case [major, minor, patch]:
            return int(major), int(minor), int(patch)
        case _:
            raise ValueError(f"Cannot parse version {version!r}")


@cli.from_docstring(app)
def main(
    version: Annotated[str, typer.Argument()],
    sqlite_db_path: Annotated[
        Path,
        typer.Argument(writable=True, dir_okay=False),
    ],
    api_key: Annotated[str, typer.Option(envvar="NSHMDB_API_KEY")],
    skip_faults_creation: Annotated[bool, typer.Option()] = False,
    skip_rupture_creation: Annotated[bool, typer.Option()] = False,
    skip_mfds_creation: Annotated[bool, typer.Option()] = False,
):
    """
    Generate the NSHM2022 rupture data from a CRU system solution package.

    Parameters
    ----------
    version : str
        NSHM version to download.
    sqlite_db_path : Path
        Output SQLite DB path.
    api_key : str
        Weka API key.
    skip_faults_creation : bool, optional
        If flag is set, skip fault creation.
    skip_rupture_creation : bool, optional
        If flag is set, skip rupture creation.
    skip_mfds_creation : bool, optional
        If flag is set, skip MFDS creation.
    """
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
