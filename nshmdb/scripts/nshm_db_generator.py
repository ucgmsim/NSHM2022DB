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

import geojson
import numpy as np
import pandas as pd
import qcore.coordinates
import tqdm
import typer
from geojson import FeatureCollection
from nshmdb.nshmdb import NSHMDB
from source_modelling.sources import Fault, Plane

app = typer.Typer()


FAULT_INFORMATION_PATH = Path("ruptures") / "fault_sections.geojson"
RUPTURE_FAULT_JOIN_PATH = Path("ruptures") / "fast_indices.csv"
RUPTURE_RATES_PATH = "aggregate_rates.csv"
RUPTURE_PROPERTIES_PATH = Path("ruptures") / "properties.csv"
MFDS_PATH = Path('ruptures') / 'sub_seismo_on_fault_mfds.csv'


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
        fault_trace = list(geojson.utils.coords(fault_feature))
        name = fault_feature.properties["FaultName"]
        dip_dir = fault_feature.properties["DipDir"]
        dip = fault_feature.properties["DipDeg"]
        bottom = fault_feature.properties["LowDepth"]
        if dip == 90:
            projected_width = 0
        else:
            projected_width = bottom / np.tan(np.radians(dip))
        planes = []
        for i in range(len(fault_trace) - 1):
            top_left = qcore.coordinates.wgs_depth_to_nztm(
                np.append(fault_trace[i][::-1], 0)
            )
            top_right = qcore.coordinates.wgs_depth_to_nztm(
                np.append(fault_trace[i + 1][::-1], 0)
            )
            dip_dir_direction = (
                np.array(
                    [
                        projected_width * np.cos(np.radians(dip_dir)),
                        projected_width * np.sin(np.radians(dip_dir)),
                        bottom,
                    ]
                )
                * 1000
            )
            bottom_left = top_left + dip_dir_direction
            bottom_right = top_right + dip_dir_direction
            corners = np.array([top_left, top_right, bottom_right, bottom_left])
            planes.append(Plane(corners))
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

    with zipfile.ZipFile(
        cru_solutions_zip_path, "r"
    ) as cru_solutions_zip_file, db.connection() as conn:
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
                db.insert_parent(conn, parent_id, fault_info.properties["ParentName"])
                db.insert_fault(
                    conn, fault_id, parent_id, fault_name, fault_rake, fault
                )
        if not skip_mfds_creation:
            with cru_solutions_zip_file.open(str(MFDS_PATH)) as mfds_file_handle, db.connection() as conn:
                mfds = pd.read_csv(mfds_file_handle)
                mfds = mfds.rename(columns={'Section Index': 'fault_id'})
                mfds = mfds.melt(id_vars=['fault_id'], var_name='magnitude', value_name='rate')
                mfds = mfds[mfds['rate'] > 0]
                mfds.to_sql('magnitude_frequency_distribution', conn, index=False, if_exists='append')
                
        if not skip_rupture_creation:
            with cru_solutions_zip_file.open(
                str(RUPTURE_FAULT_JOIN_PATH)
            ) as rupture_fault_join_handle, cru_solutions_zip_file.open(
                str(RUPTURE_RATES_PATH)
            ) as rupture_rates_handle, cru_solutions_zip_file.open(
                str(RUPTURE_PROPERTIES_PATH)
            ) as rupture_properties_path:
            
                rupture_rates = pd.read_csv(rupture_rates_handle).set_index(
                    "Rupture Index"
                )
                rupture_properties = pd.read_csv(rupture_properties_path).set_index(
                    "Rupture Index"
                )
                rupture_properties = rupture_properties.join(rupture_rates)
                rupture_properties.apply(
                    lambda rupture: db.add_rupture(
                        conn,
                        rupture.name,
                        rupture["Magnitude"],
                        rupture["Area (m^2)"],
                        rupture["Length (m)"],
                        rupture["rate_weighted_mean"],
                    ),
                    axis=1,
                )
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
