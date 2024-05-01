#!/usr/bin/env python3
import json
import csv
import qcore.geo
import numpy as np
import sqlite3
from sqlite3 import Connection
from typing import Any


def strike_between_coordinates(a, b) -> float:
    a_lat, a_lon = a
    b_lat, b_lon = b
    return qcore.geo.ll_bearing(a_lon, a_lat, b_lon, b_lat)


def distance_between(a, b) -> float:
    a_lat, a_lon = a
    b_lat, b_lon = b
    return qcore.geo.ll_dist(a_lon, a_lat, b_lon, b_lat)


def centre_point(a, b, dip, dip_dir, width):
    a_lat, a_lon = a
    b_lat, b_lon = b
    c_lon, c_lat = qcore.geo.ll_mid(a_lon, a_lat, b_lon, b_lat)
    projected_width = width * np.cos(np.radians(dip)) / 2
    return qcore.geo.ll_shift(c_lat, c_lon, projected_width, dip_dir)


def geojson_feature_to_fault(feature):
    properties = feature["properties"]
    dip = properties["DipDeg"]
    rake = properties["Rake"]
    dbottom = properties["LowDepth"]
    dtop = properties["UpDepth"]
    dip_dir = properties["DipDir"]
    name = properties["ParentName"]
    leading_edge = feature["geometry"]
    width = float(dbottom / np.sin(np.radians(dip)))
    segments = []
    for i in range(len(leading_edge) - 1):
        left = tuple(reversed(leading_edge[i]))
        right = tuple(reversed(leading_edge[i + 1]))
        c_lat, c_lon = centre_point(left, right, dip, dip_dir, dbottom)
        strike = strike_between_coordinates(left, right)
        length = distance_between(left, right)
        segments.append(
            fault.FaultSegment(
                strike=strike,
                rake=rake,
                dip=dip,
                dtop=dtop,
                dbottom=dbottom,
                length=length,
                width=width,
                dip_dir=dip_dir,
                clon=c_lon,
                clat=c_lat,
            )
        )
    return fault.Fault(name=name, tect_type=None, segments=segments)


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
        segments = []
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


def subsection_parent_map(fault_features):
    merged = {}
    for feature in fault_features:
        parent_name = feature["properties"]["ParentName"]
        if parent_name not in merged:
            merged[parent_name] = {
                "properties": feature["properties"],
                "geometry": feature["geometry"]["coordinates"],
            }
        else:
            merged[parent_name]["geometry"].extend(
                feature["geometry"]["coordinates"][1:]
            )
    return merged


def subsection_parent_lookup(fault_features):
    subsection_parent_lookup = {}
    for feature in fault_features:
        subsection_parent_lookup[feature["properties"]["FaultID"]] = feature[
            "properties"
        ]["ParentID"]
    return subsection_parent_lookup


def insert_ruptures(conn, indices):
    for row in indices:
        rupture_idx, segment_idx = [int(value) for value in row.values()]
        conn.execute(
            "INSERT OR REPLACE INTO rupture (rupture_id) VALUES (?)", (rupture_idx,)
        )

        conn.execute(
            "INSERT INTO rupture_faults (rupture_id, segment_id) VALUES (?, ?)",
            (rupture_idx, segment_idx),
        )


if __name__ == "__main__":
    with open("fault_sections.geojson", "r") as fault_file:
        geojson_object = json.load(fault_file)
        # merged_fault_map = subsection_parent_map(geojson_object["features"])
        # parent_lookup = subsection_parent_lookup(geojson_object["features"])
    with open("fast_indices.csv", "r") as csv_file_handle:
        csv_reader = csv.DictReader(csv_file_handle)
        indices = list(csv_reader)
    with sqlite3.connect("nshm2022.db") as conn:
        insert_faults(conn, geojson_object["features"])
        insert_ruptures(conn, indices)
