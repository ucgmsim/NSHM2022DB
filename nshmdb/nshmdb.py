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

from nshmdb import query
from qcore import coordinates
from source_modelling.sources import Fault, Plane


@dataclasses.dataclass
class Rupture:
    """A rupture from the database."""

    rupture_id: int
    """The rupture id."""
    magnitude: float
    """The rupture magnitude (note: this is not the moment magnitude)"""
    area: float
    """The rupture area (in km^2)."""
    length: float
    """The rupture length (in km)."""
    rate: Optional[float]
    """An optional yearly rate of rupture."""
    faults: dict[str, Fault]
    """The faults in the rupture."""
    """"""

    def __repr__(self) -> str:
        """Return a human readable debug representation of the Rupture object.

        Returns
        -------
        str
            The rupture representation.
        """
        return f"{self.__class__.__name__}(rupture_id={self.rupture_id}, magnitude={self.magnitude}, area={self.area}, rate={self.rate}, faults={list(self.faults)})"


@dataclasses.dataclass
class FaultInfo:
    """Fault metadata stored in the database."""

    fault_id: int
    """The id of the fault."""
    name: str
    """The name of the fault."""
    parent_id: int
    """The id of the parent fault for this fault."""
    rake: float
    """The rake of the fault."""
    tect_type: Optional[int]
    """The tectonic type of the fault."""


class NSHMDB:
    """Class for interacting with the NSHMDB database.

    Parameters
    ----------
        db_filepath : Path
            Path to the SQLite database file.
    """

    db_filepath: Path

    def __init__(self, db_filepath: Path):
        """Initialise the NSHMDB instance.

        Parameters
        ----------
        db_filepath : Path
            Path to the SQLite database file.
        """
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

    def add_rupture(
        self,
        conn: Connection,
        rupture_id: int,
        magnitude: float,
        area: float,
        length: float,
        rate: float,
    ) -> None:
        """Add a rupture into the database.

        Parameters
        ----------
        conn : Connection
            The SQLite db connection.
        rupture_id : int
            The rupture id.
        magnitude : float
            The magnitude of the rupture.
        area : float
            The area of the rupture.
        length : float
            The length of the rupture.
        rate : float
            The rupture rate.
        """
        conn.execute(
            "INSERT INTO rupture (rupture_id, magnitude, area, len, rate) VALUES (?, ?, ?, ?, ?)",
            (rupture_id, magnitude, area, length, rate),
        )

    def most_likely_fault(
        self, rupture_id: int, parent_fault_magnitudes: dict[str, float]
    ) -> dict[str, float]:
        """
        Calculate the cumulative activity rate for each fault involved in a specified rupture.

        This function queries the database for the activity rates associated with the expected
        magnitudes of each fault within a given rupture. The `parent_fault_magnitudes`
        parameter provides a mapping of each fault in the rupture to its expected magnitude,
        which is determined based on magnitude scaling relations without requiring knowledge
        of the rupture path. Using the magnitude frequency distribution (MFD) in the database,
        the function retrieves the closest available magnitude to the one provided, and sums
        the associated activity rates to create a "pseudo-activity rate" for each fault.

        This cumulative activity rate is returned as a dictionary mapping each fault segment
        to its calculated rate, supporting downstream processes in determining a likely starting
        fault for rupture propagation (see `workflow.scripts.nshm2022_to_realisation`).

        Parameters
        ----------
        rupture_id : int
            The unique identifier of the rupture to query.
        parent_fault_magnitudes : dict[str, float]
            A mapping of parent fault names to their expected magnitudes. These magnitudes
            define the target values for querying activity rates in the MFD table.

        Returns
        -------
        dict[str, float]
            A dictionary mapping each parent fault name to its cumulative activity rate
            at the given magnitude.
        """
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
                """SELECT pf.name, SUM(mfd.rate)
            FROM parent_fault pf
            JOIN fault f ON f.parent_id = pf.parent_id
            JOIN rupture_faults rf ON rf.fault_id = f.fault_id
            JOIN magnitude_frequency_distribution mfd ON mfd.fault_id = f.fault_id
            WHERE rf.rupture_id = ? AND
            ("""
                + " OR ".join(
                    ["pf.name = ? AND mfd.magnitude = ?"] * len(parent_fault_magnitudes)
                )
                + """) GROUP BY pf.name""",
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
            return {
                segment_name: cumulative_rate for segment_name, cumulative_rate in rates
            }

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
        """Get the fault information for a given fault id.

        Parameters
        ----------
        fault_id : int
            The fault id to retreive info for.


        Returns
        -------
        FaultInfo
            The fault information.
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * from fault where fault_id = ?", (fault_id,))
            return FaultInfo(*cursor.fetchone())

    def get_rupture(self, rupture_id: int) -> Rupture:
        """Retrieve a rupture from the database.

        Parameters
        ----------
        rupture_id : int
            The rupture to retrieve.


        Returns
        -------
        Rupture
            The rupture from the database.
        """
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
            faults = collections.defaultdict(lambda: [])
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
                faults[parent_name].append(
                    Plane(coordinates.wgs_depth_to_nztm(corners))
                )
            return {name: Fault(planes) for name, planes in faults.items()}

    def get_rupture_fault_info(self, rupture_id: int) -> dict[str, FaultInfo]:
        """Get the rupture fault information for a given rupture.

        Parameters
        ----------
        rupture_id : int
            The rupture id.


        Returns
        -------
        dict[str, FaultInfo]
            A dictionary mapping fault name to fault information for each fault in the rupture.
        """
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

    def get_fault_names(self) -> set[str]:
        """Get the list of fault names in the database.

        Returns
        -------
        set[str]
            The list of fault names.
        """
        with self.connection() as conn:
            return {
                name
                for (name,) in conn.execute("SELECT name FROM parent_fault").fetchall()
            }

    def query(
        self,
        query_str: str,
        magnitude_bounds: tuple[Optional[float], Optional[float]] = (None, None),
        rate_bounds: tuple[Optional[float], Optional[float]] = (None, None),
        limit: int = 100,
        fault_count_limit: Optional[int] = None,
    ) -> dict[int, Rupture]:
        """Make an advanced query for ruptures in the database using the query engine in `nshmdb.query`.

        See `nshmdb.query.to_sql` for details on what the parameters
        of this function should look like.

        Parameters
        ----------
        query_str : str
            The query string to execute.
        magnitude_bounds : tuple[Optional[float], Optional[float]]
            The magnitude bounds.
        rate_bounds : tuple[Optional[float], Optional[float]]
            The rate bounds.
        limit : int
            The limit on the number of returned ruptures.
        fault_count_limit : Optional[int]
            The fault count limit on the returned ruptures.


        Returns
        -------
        dict[int, Rupture]
            A mapping from rupture id to Rupture object for each rupture satisfying the query parameters.
        """
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
