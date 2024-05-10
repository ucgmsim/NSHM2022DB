#!/usr/bin/env python3
import csv
import json
import sqlite3
from pathlib import Path
from sqlite3 import Connection
from typing import Annotated, Any

import numpy as np
import qcore.geo
import typer

app = typer.Typer()


def strike_between_coordinates(a: (float, float), b: (float, float)) -> float:
    a_lat, a_lon = a
    b_lat, b_lon = b
    return qcore.geo.ll_bearing(a_lon, a_lat, b_lon, b_lat)


def distance_between(a: (float, float), b: (float, float)) -> float:
    a_lat, a_lon = a
    b_lat, b_lon = b
    return qcore.geo.ll_dist(a_lon, a_lat, b_lon, b_lat)


def centre_point(
    a: (float, float), b: (float, float), dip: float, dip_dir: float, width: float
) -> (float, float):
    a_lat, a_lon = a
    b_lat, b_lon = b
    c_lon, c_lat = qcore.geo.ll_mid(a_lon, a_lat, b_lon, b_lat)
    projected_width = width * np.cos(np.radians(dip)) / 2
    return qcore.geo.ll_shift(c_lat, c_lon, projected_width, dip_dir)


def insert_magnitude_frequency_distribution(
    conn: Connection, magnitude_frequency_distribution: list[dict[str, float | str]]
):
    for section_distribution in magnitude_frequency_distribution:
        segment_id = int(section_distribution["Section Index"])
        for magnitude_key, probability_raw in section_distribution.items():
            if magnitude_key == "Section Index":
                continue
            magnitude = float(magnitude_key)
            probability = float(probability_raw)
            conn.execute(
                "INSERT INTO magnitude_frequency_distribution (fault_id, magnitude, probability) VALUES (?, ?, ?)",
                (segment_id, magnitude, probability),
            )


def insert_faults(conn: Connection, fault_map: dict[str, Any]):
    for feature in fault_map:
        properties = feature["properties"]
        dip = properties["DipDeg"]
        rake = properties["Rake"]
        dbottom = properties["LowDepth"]
        dtop = properties["UpDepth"]
        dip_dir = properties["DipDir"]
        leading_edge = feature["geometry"]["coordinates"]
        width = float(dbottom / np.sin(np.radians(dip)))
        fault_name = properties["FaultName"]
        fault_id = properties["FaultID"]
        parent_id = properties["ParentID"]
        parent_name = properties["ParentName"]
        conn.execute(
            "INSERT OR REPLACE INTO parent_fault (parent_id, name) VALUES (?, ?)",
            (parent_id, parent_name),
        )
        conn.execute(
            "INSERT INTO fault (fault_id, name, parent_id) VALUES (?, ?, ?)",
            (fault_id, fault_name, parent_id),
        )
        for i in range(len(leading_edge) - 1):
            left = tuple(reversed(leading_edge[i]))
            right = tuple(reversed(leading_edge[i + 1]))
            c_lat, c_lon = centre_point(left, right, dip, dip_dir, dbottom)
            strike = strike_between_coordinates(left, right)
            length = distance_between(left, right)
            conn.execute(
                "INSERT INTO fault_segment (strike, rake, dip, dtop, dbottom, length, width, dip_dir, clon, clat, fault_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    strike,
                    rake,
                    dip,
                    dtop,
                    dbottom,
                    length,
                    width,
                    dip_dir,
                    c_lon,
                    c_lat,
                    fault_id,
                ),
            )


def insert_ruptures(conn: Connection, indices: dict[int, int]):
    """Inserts rupture data into the database.

    Parameters
    ----------
    conn : sqlite3.Connection
        A connection object to the SQLite database.
    indices : dict[int, int]
        A dictionary containing rupture indices mapped to fault indices. If
        indices[0] = 1, then the fault 1 is involved in rupture 0.
    """    for row in indices:
        rupture_idx, fault_idx = [int(value) for value in row.values()]
        conn.execute(
            "INSERT OR REPLACE INTO rupture (rupture_id) VALUES (?)", (rupture_idx,)
        )

        conn.execute(
            "INSERT INTO rupture_faults (rupture_id, fault_id) VALUES (?, ?)",
            (rupture_idx, fault_idx),
        )


@app.command()
def main(
    fault_sections_geojson_filepath: Annotated[
        Path,
        typer.Option(help="Fault sections geojson file", readable=True, exists=True),
    ] = "fault_sections.geojson",
    fast_indices_filepath: Annotated[
        Path, typer.Option(help="Fast indices csv file", readable=True, exists=True)
    ] = "fast_indices.csv",
    mfds_filepath: Annotated[
        Path, typer.Option(help="MFDS filepath", readable=True, exists=True)
    ] = "sub_seismo_on_fault_mfds.csv",
    sqlite_db_path: Annotated[
        Path, typer.Option(help="Output SQLite DB path", writable=True, exists=True)
    ] = "nshm2022.db",
):
    with open(fault_sections_geojson_filepath, "r", encoding="utf-8") as fault_file:
        geojson_object = json.load(fault_file)
    with open(fast_indices_filepath, "r", encoding="utf-8") as csv_file_handle:
        csv_reader = csv.DictReader(csv_file_handle)
        indices = list(csv_reader)
    with open(mfds_filepath, "r", encoding="utf-8") as mfds_file_handle:
        csv_reader = csv.DictReader(mfds_file_handle)
        magnitude_frequency_distribution = list(csv_reader)
    with sqlite3.connect(sqlite_db_path) as conn:
        conn.execute("PRAGMA foreign_keys = 1")
        insert_faults(conn, geojson_object["features"])
        insert_ruptures(conn, indices)
        insert_magnitude_frequency_distribution(conn, magnitude_frequency_distribution)


if __name__ == "__main__":
    app()
