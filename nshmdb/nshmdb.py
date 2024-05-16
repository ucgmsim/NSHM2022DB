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

import dataclasses
import sqlite3
from pathlib import Path
from sqlite3 import Connection

import numpy as np

import nshmdb.fault
from nshmdb.fault import Fault


@dataclasses.dataclass
class NSHMDB:
    """Class for interacting with the NSHMDB database.

    Parameters
    ----------
        db_filepath : Path
            Path to the SQLite database file.
    """

    db_filepath: Path

    def connection(self) -> Connection:
        """Establish a connection to the SQLite database.

        Returns
        -------
        Connection
        """
        return sqlite3.connect(self.db_filepath)

    def insert_fault(self, fault_id: int, parent_id: int, fault: Fault):
        """Insert fault data into the database.

        Parameters
        ----------
        fault_id : int
            ID of the fault.
        parent_id : int
            ID of the parent fault.
        fault : Fault
            Fault object containing fault geometry.
        """
        with self.connection() as conn:
            conn.execute(
                """INSERT INTO fault (fault_id, name, parent_id) VALUES (?, ?, ?)""",
                (fault_id, fault.name, parent_id),
            )
            for segment in fault.segments:
                conn.execute(
                    """INSERT INTO fault_segment (
                        top_left_lat,
                        top_left_lon,
                        top_right_lat,
                        top_right_lon,
                        bottom_right_lat,
                        bottom_right_lon,
                        top_depth,
                        bottom_depth,
                        rake,
                        fault_id
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )""",
                    *segment.corners[:, :2].ravel(),
                    segment.corners[0, 2],
                    segment.corners[-1, 2]
                )

    def insert_rupture(self, rupture_id: int, fault_ids: list[int]):
        """Insert rupture data into the database.

        Parameters
        ----------
        rupture_id : int
            ID of the rupture.
        fault_ids : list[int]
            List of faults involved in the rupture.
        """
        with self.connection() as conn:
            conn.execute("INSERT INTO rupture (rupture_id) VALUES (?)", (rupture_id,))
            for fault_id in fault_ids:
                conn.execute(
                    "INSERT INTO rupture_faults VALUES (?, ?)", (rupture_id, fault_id)
                )

    def get_rupture_faults(self, rupture_id: int) -> list[Fault]:
        """Retrieve faults involved in a rupture from the database.

        Parameters
        ----------
        rupture_id : int

        Returns
        -------
        list[Fault]
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT fs.*, p.parent_id, p.name
                FROM fault_segment fs
                JOIN rupture_faults rf ON fs.fault_id = rf.fault_id
                JOIN fault f ON fs.fault_id = f.fault_id
                JOIN parent_fault p ON f.parent_id = p.parent_id
                WHERE rf.rupture_id = ?
                ORDER BY f.parent_id""",
                (rupture_id,),
            )
            fault_segments = cursor.fetchall()
            cur_parent_id = None
            faults = []
            for (
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
                rake,
                parent_id,
                parent_name,
            ) in fault_segments:
                if parent_id != cur_parent_id:
                    faults.append(
                        Fault(
                            name=parent_name,
                            tect_type=None,
                            segments=[],
                        )
                    )
                    cur_parent_id = parent_id
                corners = np.array(
                    [
                        [top_left_lat, top_left_lon, top],
                        [top_right_lat, top_right_lon, top],
                        [bottom_right_lat, bottom_right_lon, bottom],
                        [bottom_left_lat, bottom_right_lon, bottom],
                    ]
                )
                faults[-1].segments.append(nshmdb.fault.FaultSegment(corners, rake))
            return faults
