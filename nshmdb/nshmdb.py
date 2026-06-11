"""
Module to interact with the NSHMDB (National Seismic Hazard Model Database).

Use the `NSHMDB` context manager to open a database connection and query
fault and rupture data.

>>> with NSHMDB('path/to/nshm.db') as db:
...     faults = db.get_rupture_faults(1)
"""

import collections
import contextlib
import dataclasses
import importlib.resources
import sqlite3
from dataclasses import field
from enum import IntEnum, auto
from pathlib import Path
from sqlite3 import Connection
from types import TracebackType
from typing import Optional, Self

import duckdb
import numpy as np
import pandas as pd

from nshmdb import query
from qcore import coordinates
from source_modelling.sources import Fault, Plane


class FaultSystem(IntEnum):
    """NSHM Fault systems"""

    Hikurangi = auto()
    Puysegur = auto()
    Crustal = auto()


@dataclasses.dataclass
class Rupture:
    """A rupture from the database."""

    fault_system: FaultSystem
    rupture_id: int

    magnitude: float
    """The rupture magnitude (note: this is not the moment magnitude)"""
    area: float
    """The rupture area (in km^2)."""
    length: float
    """The rupture length (in km)."""
    rate: Optional[float]
    """An optional yearly rate of rupture."""
    faults: dict[str, Fault] = field(repr=False)
    """The faults in the rupture."""


@dataclasses.dataclass
class FaultInfo:
    """Fault metadata stored in the database."""

    fault_system: FaultSystem
    fault_id: int

    name: str
    """The name of the fault."""

    rake: float
    """The rake of the fault."""

    tect_type: int | None
    """The tectonic type of the fault."""

    fault: Fault | None = None


class NSHMDB(contextlib.AbstractContextManager):
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
        self._conn = None

    def create(self) -> None:
        """Create the tables for the NSHMDB database.

        Safe to call on an existing database; all statements use
        ``CREATE TABLE IF NOT EXISTS``.
        """
        schema_traversable = importlib.resources.files("nshmdb.schema") / "schema.sql"
        with importlib.resources.as_file(schema_traversable) as schema_path:
            with open(schema_path, "r", encoding="utf-8") as schema_file_handle:
                schema = schema_file_handle.read()
        conn = self._conn or sqlite3.connect(self.db_filepath)
        conn.executescript(schema)
        if self._conn is None:
            conn.close()

    def connect(self) -> None:
        """Open the database connection and create the schema if needed."""
        if not self._conn:
            self._conn = sqlite3.connect(self.db_filepath)
            self.create()

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
        self._conn = None

    def __enter__(self) -> Self:
        """Open the database connection."""
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the database connection."""
        self.close()

    def connection(self) -> Connection:
        """Establish a connection to the SQLite database.

        Returns
        -------
        Connection
        """
        if self._conn is None:
            raise ConnectionError(
                "Must enter database context before executing any sqlite commands."
            )

        return self._conn

    def add_rupture(
        self,
        fault_system: FaultSystem,
        nshm_id: int,
        magnitude: float,
        area: float,
        length: float,
        rate: float,
    ) -> None:
        """Add a rupture into the database.

        Parameters
        ----------
        fault_system : FaultSystem
            The fault system of the rupture.
        nshm_id : int
            The NSHM rupture id.
        magnitude : float
            The magnitude of the rupture.
        area : float
            The area of the rupture.
        length : float
            The length of the rupture.
        rate : float
            The rupture rate.
        """
        self.connection().execute(
            "INSERT INTO rupture (fault_system, nshm_id, magnitude, area, len, rate) VALUES (?, ?, ?, ?, ?, ?)",
            (fault_system, nshm_id, magnitude, area, length, rate),
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
        conn = self.connection()

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

    def add_fault_to_rupture(self, rupture_id: int, fault_id: int) -> None:
        """Link a fault to an existing rupture in the database.

        Parameters
        ----------
        rupture_id : int
            Internal ID of the rupture (must already exist).
        fault_id : int
            Internal ID of the fault to link.
        """
        conn = self.connection()
        conn.execute(
            "INSERT INTO rupture_faults (rupture_id, fault_id) VALUES (?, ?)",
            (rupture_id, fault_id),
        )

    def insert_many_faults(self, faults: list[FaultInfo]) -> None:
        """Bulk-insert fault definitions into the database.

        Parent fault records are upserted from the fault names; fault plane
        geometry stored on each ``FaultInfo.fault`` is also inserted.

        Parameters
        ----------
        faults : list[FaultInfo]
            The fault definitions to insert.
        """
        cursor = self.connection().cursor()

        cursor.executemany(
            "INSERT OR IGNORE INTO parent_fault (name) VALUES (?)",
            ((fault.name,) for fault in faults),
        )
        cursor.execute("SELECT name, parent_id FROM parent_fault")
        parent_id_map = dict(cursor.fetchall())

        cursor.execute("SELECT MAX(fault_id) FROM fault")
        max_id = cursor.fetchone()[0]
        next_fault_idx = max_id + 1 if max_id is not None else 0

        cursor.executemany(
            "INSERT INTO fault (fault_id, fault_system, nshm_id, rake, tect_type, parent_id) VALUES (?, ?, ?, ?, ?, ?)",
            (
                (
                    next_fault_idx + i,
                    f.fault_system,
                    f.fault_id,
                    f.rake,
                    f.tect_type,
                    parent_id_map[f.name],
                )
                for i, f in enumerate(faults)
            ),
        )

        plane_tuples = []
        for i, f in enumerate(faults):
            if not f.fault:
                continue
            fault_idx = next_fault_idx + i
            for plane in f.fault.planes:
                coords = plane.corners[:, :2].ravel()
                plane_tuples.append((*coords, plane.top_m, plane.bottom_m, fault_idx))

        if plane_tuples:
            cursor.executemany(
                """
                INSERT INTO fault_plane (
                    top_left_lat, top_left_lon, top_right_lat, top_right_lon,
                    bottom_right_lat, bottom_right_lon, bottom_left_lat, bottom_left_lon,
                    top_depth, bottom_depth, fault_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                plane_tuples,
            )

        cursor.close()
        self.connection().commit()

    def _nshm_id_to_fault_id(self, nshm_ids: pd.DataFrame) -> pd.DataFrame:
        conn = self.connection()
        fault_id_map = pd.read_sql_query(
            "SELECT fault_system, nshm_id, fault_id FROM fault",
            conn,
        )
        fault_id_map = fault_id_map.rename(columns=dict(nshm_id="fault_nshm_id"))
        return nshm_ids.merge(
            fault_id_map, on=["fault_system", "fault_nshm_id"], how="left"
        )

    def _nshm_id_to_rupture_id(self, nshm_ids: pd.DataFrame) -> pd.DataFrame:
        conn = self.connection()
        rupture_id_map = pd.read_sql_query(
            "SELECT fault_system, nshm_id, rupture_id FROM rupture",
            conn,
        )
        rupture_id_map = rupture_id_map.rename(columns=dict(nshm_id="rupture_nshm_id"))

        return nshm_ids.merge(
            rupture_id_map, on=["fault_system", "rupture_nshm_id"], how="left"
        )

    def insert_many_ruptures(
        self, ruptures: pd.DataFrame, rupture_faults: pd.DataFrame
    ) -> None:
        """Bulk-insert rupture data and their fault associations.

        Parameters
        ----------
        ruptures : pd.DataFrame
            DataFrame indexed by NSHM rupture id with columns
            ``magnitude``, ``area``, ``len``, ``rate``, and ``fault_system``.
        rupture_faults : pd.DataFrame
            DataFrame with columns ``rupture_id`` (NSHM rupture id),
            ``fault_id`` (NSHM fault id), and ``fault_system``.
        """
        conn = self.connection()
        ruptures.to_sql(
            "rupture", conn, index=True, index_label="nshm_id", if_exists="append"
        )

        rupture_faults = rupture_faults.rename(
            columns=dict(rupture_id="rupture_nshm_id", fault_id="fault_nshm_id")
        )

        df_joined = self._nshm_id_to_rupture_id(rupture_faults)
        df_joined = self._nshm_id_to_fault_id(df_joined)

        rupture_join_table = df_joined[["rupture_id", "fault_id"]]

        rupture_join_table.to_sql(
            "rupture_faults", conn, index=False, if_exists="append"
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
        conn = self.connection()
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
        return Fault(planes)

    def get_fault_info(self, fault_id: int) -> FaultInfo:
        """Get the fault information for a given fault id.

        Parameters
        ----------
        fault_id : int
            The internal fault id (primary key).

        Returns
        -------
        FaultInfo
            The fault information.
        """
        conn = self.connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT f.fault_system, f.fault_id, p.name, f.rake, f.tect_type
            FROM fault f
            JOIN parent_fault p ON f.parent_id = p.parent_id
            WHERE f.fault_id = ?
            """,
            (fault_id,),
        )
        return FaultInfo(*cursor.fetchone(), fault=None)

    def insert_magnitude_frequency_distribution(self, mfds: pd.DataFrame) -> None:
        """Bulk-insert magnitude frequency distribution entries.

        Parameters
        ----------
        mfds : pd.DataFrame
            DataFrame with columns ``nshm_id``, ``fault_system``,
            ``magnitude``, and ``rate``.
        """
        mfds = mfds.rename(columns=dict(nshm_id="fault_nshm_id"))
        mfds = self._nshm_id_to_fault_id(mfds)
        mfds[["fault_id", "magnitude", "rate"]].to_sql(
            "magnitude_frequency_distribution",
            self.connection(),
            index=False,
            if_exists="append",
        )

    def get_rupture(self, fault_system: FaultSystem, nshm_id: int) -> Rupture:
        """Retrieve a rupture from the database.

        Parameters
        ----------
        fault_system : FaultSystem
            The fault system of the rupture.
        nshm_id : int
            The rupture to retrieve.

        Returns
        -------
        Rupture
            The rupture from the database.
        """
        conn = self.connection()
        cursor = conn.cursor()
        (rupture_id, magnitude, area, length, rate) = cursor.execute(
            "SELECT rupture_id, magnitude, area, len, rate FROM rupture WHERE nshm_id = ? AND fault_system = ?",
            (nshm_id, fault_system),
        ).fetchone()

        return Rupture(
            rupture_id=nshm_id,
            fault_system=FaultSystem(fault_system),
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
        conn = self.connection()
        cursor = conn.cursor()
        cursor.execute(
            """SELECT fs.*, f.fault_id, f.fault_system, p.parent_id, p.name
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
            fault_id,
            fault_system,
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
            # HACK: Geometries are only connected in the crustal setting. This
            # will be addressed by fault planarisation for subduction sources at
            # a later date.
            fault_name = (
                parent_name
                if fault_system == FaultSystem.Crustal
                else f"{parent_name}: Section {fault_id}"
            )
            faults[fault_name].append(Plane(coordinates.wgs_depth_to_nztm(corners)))
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
        conn = self.connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT p.name, f.fault_system, f.fault_id, p.name, f.rake, f.tect_type
            FROM fault f
            JOIN rupture_faults rf on f.fault_id = rf.fault_id
            JOIN parent_fault p ON f.parent_id = p.parent_id
            WHERE rf.rupture_id = ?
            """,
            (rupture_id,),
        )
        fault_rows = cursor.fetchall()
        return {row[0]: FaultInfo(*row[1:], fault=None) for row in fault_rows}

    def get_fault_names(self) -> set[str]:
        """Get the list of fault names in the database.

        Returns
        -------
        set[str]
            The list of fault names.
        """
        conn = self.connection()
        return {
            name for (name,) in conn.execute("SELECT name FROM parent_fault").fetchall()
        }

    def get_fault_ids(self) -> set[int]:
        """Get the list of fault ids in the database.

        Returns
        -------
        set[int]
            The list of fault ids.
        """
        conn = self.connection()
        return {
            fault_id
            for (fault_id,) in conn.execute("SELECT fault_id FROM fault").fetchall()
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
        with duckdb.connect(self.db_filepath) as conn:
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
                    fault_system=fault_system,
                    magnitude=magnitude,
                    area=area,
                    length=length,
                    rate=rate,
                    faults=self.get_rupture_faults(id),
                )
                for (id, fault_system, magnitude, area, length, rate) in ruptures
            }
