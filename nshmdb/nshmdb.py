"""
Module to interact with the NSHMDB (National Seismic Hazard Model Database).

This module provides classes and functions to interact with an SQLite database
containing national seismic hazard model data. It includes functionalities to
insert fault and rupture data into the database, as well as retrieve fault
information associated with ruptures.

Classes
-------
NSHMDB
    Class for interacting with the NSHMDB database.

Usage
-----
Initialize an instance of NSHMDB with the path to the SQLite database file.
Use the methods of the NSHMDB class to interact with fault and rupture data
in the database.

>>> db = NSHMDB('path/to/nshm.db')
>>> db.get_rupture_faults(0) # Should return two faults in this rupture.
"""

import collections
import dataclasses
import importlib.resources
import sqlite3
from pathlib import Path
from sqlite3 import Connection
from typing import Optional

import duckdb
import numpy as np
from qcore import coordinates
from source_modelling.sources import Fault, Plane

from nshmdb import query


@dataclasses.dataclass
class Rupture:
    rupture_id: int
    magnitude: float
    area: float
    length: float
    rate: Optional[float]
    faults: dict[str, Fault]

    def __repr__(self):
        return f"{self.__class__.__name__}(rupture_id={self.rupture_id}, magnitude={self.magnitude}, area={self.area}, rate={self.rate}, faults={list(self.faults)})"


@dataclasses.dataclass
class FaultInfo:
    """Fault metadata stored in the database.

    fault_id : int
        The id of the fault.
    name : str
        The name of the fault.
    parent_id : int
        The id of the parent fault for this fault.
    rake : float
        The rake of the fault.
    tect_type : int
        The tectonic type of the fault.
    """

    fault_id: int
    name: str
    parent_id: int
    rake: float
    tect_type: Optional[int]


class NSHMDB:
    """Class for interacting with the NSHMDB database.

    Parameters
    ----------
        db_filepath : Path
            Path to the SQLite database file.
    """

    db_filepath: Path

    def __init__(self, db_filepath: Path):
        self.db_filepath = db_filepath

    def create(self):
        """Create the tables for the NSHMDB database."""
        schema_traversable = importlib.resources.files("nshmdb.schema") / "schema.sql"
        with importlib.resources.as_file(schema_traversable) as schema_path:
            with open(schema_path, "r", encoding="utf-8") as schema_file_handle:
                schema = schema_file_handle.read()
        with self.connection() as conn:
            conn.executescript(schema)

    def connection(self) -> Connection:
        """Establish a connection to the SQLite database.

        Returns
        -------
        Connection
        """
        return sqlite3.connect(self.db_filepath)

    # The functions `insert_parent`, `insert_fault`, and `add_fault_to_rupture`
    # reuse a connection for efficiency (rather than use db.connection()). There
    # are thousands of faults and tens of millions of rupture, fault binding
    # pairs. Without reusing a connection it takes hours to setup the database.

    def insert_parent(self, conn: Connection, parent_id: int, parent_name: str):
        """Insert parent fault data into the database.

        Parameters
        ----------
        conn : Connection
            The db connection object.
        parent_id : int
            ID of the parent fault.
        name : str
            Name of the parent fault.
        """
        conn.execute(
            """INSERT OR REPLACE INTO parent_fault (parent_id, name) VALUES (?, ?)""",
            (parent_id, parent_name),
        )

    def insert_fault(
        self,
        conn: Connection,
        fault_id: int,
        parent_id: int,
        fault_name: str,
        fault_rake: float,
        fault: Fault,
    ):
        """Insert fault data into the database.

        Parameters
        ----------
        conn : Connection
            The db connection object.
        fault_id : int
            ID of the fault.
        parent_id : int
            ID of the parent fault.
        fault_name : str
            The name of the fault.
        fault_rake : float
            The rake of the fault.
        fault : Fault
            Fault object containing fault geometry.
        """
        conn.execute(
            """INSERT OR REPLACE INTO fault (fault_id, name, rake, parent_id) VALUES (?, ?, ?, ?)""",
            (fault_id, fault_name, fault_rake, parent_id),
        )
        for plane in fault.planes:
            conn.execute(
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
                (
                    *plane.corners[:, :2].ravel(),
                    plane.corners[0, 2],
                    plane.corners[-1, 2],
                    fault_id,
                ),
            )

    def add_rupture(
        self,
        conn: Connection,
        rupture_id: int,
        magnitude: float,
        area: float,
        length: float,
        rate: float,
    ) -> None:
        conn.execute(
            "INSERT INTO rupture (rupture_id, magnitude, area, len, rate) VALUES (?, ?, ?, ?, ?)",
            (rupture_id, magnitude, area, length, rate),
        )

    def most_likely_fault(
        self, rupture_id: int, parent_fault_magnitudes: dict[str, float]
    ) -> dict[str, list[float]]:
        """Return the segment in the rupture with the highest annual rate."""
        with self.connection() as conn:
            magnitudes = np.array(
                conn.execute(
                    """SELECT DISTINCT mfd.magnitude
            FROM magnitude_frequency_distribution mfd
            JOIN rupture_faults rf ON rf.fault_id = mfd.fault_id
            WHERE rf.rupture_id = ?
            ORDER BY mfd.magnitude""",
                    (rupture_id,),
                ).fetchall()
            ).ravel()
            idx = np.minimum(
                np.searchsorted(magnitudes, list(parent_fault_magnitudes.values())),
                len(magnitudes) - 1,
            )
            parent_fault_magnitudes_rounded = np.minimum(
                magnitudes[idx], magnitudes[np.minimum(idx + 1, len(magnitudes) - 1)]
            )
            rates = conn.execute(
                """SELECT pf.name, mfd.rate
            FROM parent_fault pf
            JOIN fault f ON f.parent_id = pf.parent_id
            JOIN rupture_faults rf ON rf.fault_id = f.fault_id
            JOIN magnitude_frequency_distribution mfd ON mfd.fault_id = f.fault_id
            WHERE rf.rupture_id = ? AND
            ("""
                + " OR ".join(
                    ["pf.name = ? AND mfd.magnitude = ?"] * len(parent_fault_magnitudes)
                )
                + ")",
                (rupture_id,)
                + tuple(
                    [
                        item
                        for tup in zip(
                            parent_fault_magnitudes, parent_fault_magnitudes_rounded
                        )
                        for item in tup
                    ]
                ),
            )
            segment_rates = collections.defaultdict(list)
            for parent_fault_name, rate in rates:
                segment_rates[parent_fault_name].append(rate)
            return segment_rates

    def add_fault_to_rupture(self, conn: Connection, rupture_id: int, fault_id: int):
        """Insert rupture data into the database.

        Parameters
        ----------
        conn : Connection
            The db connection object.
        rupture_id : int
            ID of the rupture.
        fault_ids : list[int]
            List of faults involved in the rupture.
        """
        conn.execute(
            "INSERT OR IGNORE INTO rupture (rupture_id) VALUES (?)", (rupture_id,)
        )
        conn.execute(
            "INSERT INTO rupture_faults (rupture_id, fault_id) VALUES (?, ?)",
            (rupture_id, fault_id),
        )

    def get_fault(self, fault_id: int) -> Fault:
        """Get a specific fault definition from a database.

        Parameters
        ----------
        fault_id : int
            The id of the fault to retreive.

        Returns
        -------
        Fault
            The fault geometry.
        """

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * from fault_plane where fault_id = ?", (fault_id,))
            planes = []
            for (
                _,
                top_left_lat,
                top_left_lon,
                top_right_lat,
                top_right_lon,
                bottom_right_lat,
                bottom_right_lon,
                bottom_left_lat,
                bottom_left_lon,
                top,
                bottom,
                _,
            ) in cursor.fetchall():
                corners = np.array(
                    [
                        [top_left_lat, top_left_lon, top],
                        [top_right_lat, top_right_lon, top],
                        [bottom_right_lat, bottom_right_lon, bottom],
                        [bottom_left_lat, bottom_left_lon, bottom],
                    ]
                )
                planes.append(Plane(coordinates.wgs_depth_to_nztm(corners)))
            cursor.execute("SELECT * from fault where fault_id = ?", (fault_id,))
            return Fault(planes)

    def get_fault_info(self, fault_id: int) -> FaultInfo:
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * from fault where fault_id = ?", (fault_id,))
            return FaultInfo(*cursor.fetchone())

    def get_rupture(self, rupture_id: int) -> Rupture:
        with self.connection() as conn:
            cursor = conn.cursor()
            (rupture_id, magnitude, area, length, rate) = cursor.execute(
                "SELECT rupture_id, magnitude, area, len, rate FROM rupture WHERE rupture_id = ?",
                (rupture_id,),
            ).fetchone()

        return Rupture(
            rupture_id=rupture_id,
            magnitude=magnitude,
            area=area,
            length=length,
            rate=rate,
            faults=self.get_rupture_faults(rupture_id),
        )

    def get_rupture_faults(self, rupture_id: int) -> dict[str, Fault]:
        """Retrieve faults involved in a rupture from the database.

        Parameters
        ----------
        rupture_id : int

        Returns
        -------
        dict[str, Fault]
            A dictionary with fault names as keys, and fault geometry
            as values.
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT fs.*, p.parent_id, p.name
                FROM fault_plane fs
                JOIN rupture_faults rf ON fs.fault_id = rf.fault_id
                JOIN fault f ON fs.fault_id = f.fault_id
                JOIN parent_fault p ON f.parent_id = p.parent_id
                WHERE rf.rupture_id = ?
                ORDER BY f.parent_id""",
                (rupture_id,),
            )
            fault_planes = cursor.fetchall()
            faults = collections.defaultdict(lambda: Fault([]))
            for (
                _,
                top_left_lat,
                top_left_lon,
                top_right_lat,
                top_right_lon,
                bottom_right_lat,
                bottom_right_lon,
                bottom_left_lat,
                bottom_left_lon,
                top,
                bottom,
                _,
                parent_id,
                parent_name,
            ) in fault_planes:
                corners = np.array(
                    [
                        [top_left_lat, top_left_lon, top],
                        [top_right_lat, top_right_lon, top],
                        [bottom_right_lat, bottom_right_lon, bottom],
                        [bottom_left_lat, bottom_left_lon, bottom],
                    ]
                )
                faults[parent_name].planes.append(
                    Plane(coordinates.wgs_depth_to_nztm(corners))
                )
            return faults

    def get_rupture_fault_info(self, rupture_id: int) -> dict[str, FaultInfo]:
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT p.name, f.*
                FROM fault f
                JOIN rupture_faults rf on f.fault_id = rf.fault_id
                JOIN parent_fault p ON f.parent_id = p.parent_id
                WHERE rf.rupture_id = ?
                """,
                (rupture_id,),
            )
            fault_rows = cursor.fetchall()
            return {row[0]: FaultInfo(*row[1:]) for row in fault_rows}

    def get_fault_names(self) -> list[str]:
        with self.connection() as conn:
            return [
                name
                for (name,) in conn.execute("SELECT name FROM parent_fault").fetchall()
            ]

    def query(
        self,
        query_str: str,
        magnitude_bounds: tuple[Optional[float], Optional[float]] = (None, None),
        rate_bounds: tuple[Optional[float], Optional[float]] = (None, None),
        limit: int = 100,
        fault_count_limit: Optional[int] = None,
    ) -> dict[int, Rupture]:
        conn = duckdb.connect(self.db_filepath)
        sql_query, parameters = query.to_sql(
            query_str,
            rate_bounds=rate_bounds,
            magnitude_bounds=magnitude_bounds,
            limit=limit,
            fault_count_limit=fault_count_limit,
        )
        ruptures = conn.sql(sql_query, params=parameters).fetchall()
        return {
            id: Rupture(
                rupture_id=id,
                magnitude=magnitude,
                area=area,
                length=length,
                rate=rate,
                faults=self.get_rupture_faults(id),
            )
            for (id, magnitude, area, length, rate) in ruptures
        }
