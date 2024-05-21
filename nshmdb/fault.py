"""Module for representing fault planes and faults.

This module provides classes and functions for representing fault planes and
faults, along with methods for calculating various properties such as
dimensions, orientation, and coordinate transformations.

Classes
-------
TectType:
    An enumeration of all the different kinds of fault types.

FaultPlane:
    A representation of a single plane of a Fault.

Fault:
    A representation of a fault, consisting of one or more FaultPlanes.
"""

import dataclasses
from enum import Enum

import numpy as np
import qcore.coordinates
import qcore.geo


class TectType(Enum):
    """An enumeration of all the different kinds of fault types."""

    ACTIVE_SHALLOW = 1
    VOLCANIC = 2
    SUBDUCTION_INTERFACE = 3
    SUBDUCTION_SLAB = 4


_KM_TO_M = 1000


@dataclasses.dataclass
class FaultPlane:
    """A representation of a single plane of a Fault.

    This class represents a single plane of a fault, providing various
    properties and methods for calculating its dimensions, orientation, and
    converting coordinates between different reference frames.

    Attributes
    ----------
    rake : float
        The rake angle of the fault plane.
    """

    _corners: np.ndarray
    rake: float

    def __init__(self, corners: np.ndarray, rake: float):
        self._corners = qcore.coordinates.wgs_depth_to_nztm(corners)
        self.rake = rake

    @property
    def corners(self) -> np.ndarray:
        """

        Returns
        -------
        np.ndarray
            The corners of the fault plane in (lat, lon, depth) format.
        """
        return qcore.coordinates.nztm_to_wgs_depth(self._corners)

    @property
    def length_m(self) -> float:
        """
        Returns
        -------
        float
            The length of the fault plane (in metres).
        """
        return np.linalg.norm(self._corners[1] - self._corners[0])

    @property
    def width_m(self) -> float:
        """
        Returns
        -------
        float
            The width of the fault plane (in metres).
        """
        return np.linalg.norm(self._corners[-1] - self._corners[0])

    @property
    def bottom_m(self) -> float:
        """
        Returns
        -------
        float
            The bottom depth (in metres).
        """
        return self._corners[-1, -1]

    @property
    def width(self) -> float:
        """
        Returns
        -------
        float
            The width of the fault plane (in kilometres).
        """
        return self.width_m / _KM_TO_M

    @property
    def length(self) -> float:
        """
        Returns
        -------
        float
            The length of the fault plane (in kilometres).
        """
        return self.length_m / _KM_TO_M

    @property
    def projected_width_m(self) -> float:
        """
        Returns
        -------
        float
            The projected width of the fault plane (in metres).
        """
        return self.length_m * np.cos(np.radians(self.dip))

    @property
    def projected_width(self) -> float:
        """
        Returns
        -------
        float
            The projected width of the fault plane (in kilometres).
        """
        return self.projected_width / _KM_TO_M

    @property
    def strike(self) -> float:
        """
        Returns
        -------
        float
            The bearing of the strike direction of the fault
            (from north; in degrees)
        """

        north_direction = np.array([1, 0, 0])
        up_direction = np.array([0, 0, 1])
        strike_direction = self._corners[1] - self._corners[0]
        return qcore.geo.oriented_bearing_wrt_normal(
            north_direction, strike_direction, up_direction
        )

    @property
    def dip_dir(self) -> float:
        """
        Returns
        -------
        float
            The bearing of the dip direction (from north; in degrees).
        """
        north_direction = np.array([1, 0, 0])
        up_direction = np.array([0, 0, 1])
        dip_direction = self._corners[-1] - self._corners[0]
        dip_direction[-1] = 0
        return qcore.geo.oriented_bearing_wrt_normal(
            north_direction, dip_direction, up_direction
        )

    @property
    def dip(self) -> float:
        """
        Returns
        -------
        float
            The dip angle of the fault.
        """
        return np.degrees(np.arcsin(np.abs(self.bottom_m) / self.width_m))

    def plane_coordinates_to_global_coordinates(
        self, plane_coordinates: np.ndarray
    ) -> np.ndarray:
        """Convert plane coordinates to nztm global coordinates.

        Parameters
        ----------
        plane_coordinates : np.ndarray
            Plane coordinates to convert. Plane coordinates are
            2D coordinates (x, y) given for a fault plane (a plane), where x
            represents displacement along the length of the fault, and y
            displacement along the width of the fault (see diagram below). The
            origin for plane coordinates is the centre of the fault.

                          +x
          -1/2,-1/2 ─────────────────>
                ┌─────────────────────┐ │
                │      < width >      │ │
                │                 ^   │ │
                │               length│ │ +y
                │                 v   │ │
                │                     │ │
                └─────────────────────┘ ∨
                                     1/2,1/2

        Returns
        -------
        np.ndarray
            An 3d-vector of (lat, lon, depth) transformed coordinates.
        """
        origin = self._corners[0]
        top_right = self._corners[1]
        bottom_left = self._corners[-1]
        frame = np.vstack((top_right - origin, bottom_left - origin))
        offset = np.array([1 / 2, 1 / 2])

        return qcore.coordinates.nztm_to_wgs_depth(
            origin + (plane_coordinates + offset) @ frame
        )

    def global_coordinates_to_plane_coordinates(
        self,
        global_coordinates: np.ndarray,
    ) -> np.ndarray:
        """Convert coordinates (lat, lon, depth) to plane coordinates (x, y).

        See plane_coordinates_to_global_coordinates for a description of
        plane coordinates.

        Parameters
        ----------
        global_coordinates : np.ndarray
            Global coordinates to convert.

        Returns
        -------
        np.ndarray
            The plane coordinates (x, y) representing the position of
            global_coordinates on the fault plane.

        Raises
        ------
        ValueError
            If the given coordinates do not lie in the fault plane.
        """
        origin = self._corners[0]
        top_right = self._corners[1]
        bottom_left = self._corners[-1]
        frame = np.vstack((top_right - origin, bottom_left - origin))
        offset = qcore.coordinates.wgs_depth_to_nztm(global_coordinates) - origin
        plane_coordinates, residual, _, _ = np.linalg.lstsq(frame.T, offset, rcond=None)
        if not np.isclose(residual[0], 0, atol=1e-02):
            raise ValueError("Coordinates do not lie in fault plane.")
        return np.clip(plane_coordinates - np.array([1 / 2, 1 / 2]), -1 / 2, 1 / 2)

    def global_coordinates_in_plane(self, global_coordinates: np.ndarray) -> bool:
        """Test if some global coordinates lie in the bounds of a plane.

        Parameters
        ----------
        global_coordinates : np.ndarray
            The global coordinates to check

        Returns
        -------
        bool
            True if the given global coordinates (lat, lon, depth) lie on the
            fault plane.
        """

        try:
            plane_coordinates = self.global_coordinates_to_plane_coordinates(
                global_coordinates
            )
            return np.all(
                np.logical_or(
                    np.abs(plane_coordinates) < 1 / 2,
                    np.isclose(np.abs(plane_coordinates), 1 / 2, atol=1e-3),
                )
            )
        except ValueError:
            return False

    def centroid(self) -> np.ndarray:
        """Returns the centre of the fault plane.

        Returns
        -------
        np.ndarray
            A 1 x 3 dimensional vector representing the centroid of the fault
            plane in (lat, lon, depth) format.

        """

        return qcore.coordinates.nztm_to_wgs_depth(
            np.mean(self._corners, axis=0).reshape((1, -1))
        ).ravel()


@dataclasses.dataclass
class Fault:
    """A representation of a fault, consisting of one or more FaultPlanes.

    This class represents a fault, which is composed of one or more FaultPlanes.
    It provides methods for computing the area of the fault, getting the widths and
    lengths of all fault planes, retrieving all corners of the fault, converting
    global coordinates to fault coordinates, converting fault coordinates to global
    coordinates, generating a random hypocentre location within the fault, and
    computing the expected fault coordinates.

    Attributes
    ----------
    name : str
        The name of the fault.
    tect_type : TectType | None
        The type of fault this is (e.g. crustal, volcanic, subduction).
    planes : list[FaultPlane]
        A list containing all the FaultPlanes that constitute the fault.

    Methods
    -------
    area:
        Compute the area of a fault.
    widths:
        Get the widths of all fault planes.
    lengths:
        Get the lengths of all fault planes.
    corners:
        Get all corners of a fault.
    global_coordinates_to_fault_coordinates:
        Convert global coordinates to fault coordinates.
    fault_coordinates_to_wgsdepth_coordinates:
        Convert fault coordinates to global coordinates.
    """

    name: str
    tect_type: TectType | None
    planes: list[FaultPlane]

    def area(self) -> float:
        """Compute the area of a fault.

        Returns
        -------
        float
            The area of the fault.
        """
        return sum(plane.width * plane.length for plane in self.planes)

    def widths(self) -> np.ndarray:
        """Get the widths of all fault planes.

        Returns
        -------
        np.ndarray of shape (1 x n)
            The widths of all fault planes contained in this fault.
        """
        return np.array([seg.width for seg in self.planes])

    def lengths(self) -> np.ndarray:
        """Get the lengths of all fault planes.

        Returns
        -------
        np.ndarray of shape (1 x n)
            The lengths of all fault planes contained in this fault.
        """
        return np.array([seg.length for seg in self.planes])

    def corners(self) -> np.ndarray:
        """Get all corners of a fault.

        Returns
        -------
        np.ndarray of shape (4n x 3)
            The corners of each fault plane in the fault, stacked vertically.
        """
        return np.vstack([plane.corners for plane in self.planes])

    def global_coordinates_to_fault_coordinates(
        self, global_coordinates: np.ndarray
    ) -> np.ndarray:
        """Convert global coordinates in (lat, lon, depth) format to fault coordinates.

        Fault coordinates are a tuple (s, d) where s is the distance (in
        kilometres) from the top centre, and d the distance from the top of the
        fault (refer to the diagram).

        ┌─────────┬──────────────┬────┐
        │         │      ╎       │    │
        │         │      ╎       │    │
        │         │    d ╎       │    │
        │         │      ╎       │    │
        │         │      └╶╶╶╶╶╶╶╶╶╶+ │
        │         │           s  │  ∧ │
        │         │              │  │ │
        │         │              │  │ │
        └─────────┴──────────────┴──┼─┘
                                    │
                            point: (s, d)

        Parameters
        ----------
        global_coordinates : np.ndarray of shape (1 x 3)
            The global coordinates to convert.

        Returns
        -------
        np.ndarray
            The fault coordinates.

        Raises
        ------
        ValueError
            If the given point does not lie on the fault.
        """

        running_length = 0.0
        midpoint = np.sum(self.lengths()) / 2
        for plane in self.planes:
            if plane.global_coordinates_in_plane(global_coordinates):
                plane_coordinates = plane.global_coordinates_to_plane_coordinates(
                    global_coordinates
                )
                strike_length = plane_coordinates[0] + 1 / 2
                dip_length = plane_coordinates[1] + 1 / 2
                return np.array(
                    [
                        running_length + strike_length * plane.length - midpoint,
                        max(dip_length * plane.width, 0),
                    ]
                )
            running_length += plane.length
        raise ValueError("Specified coordinates not contained on fault.")

    def fault_coordinates_to_wgsdepth_coordinates(
        self, fault_coordinates: np.ndarray
    ) -> np.ndarray:
        """Convert fault coordinates to global coordinates.

        See global_coordinates_to_fault_coordinates for a description of fault
        coordinates.

        Parameters
        ----------
        fault_coordinates : np.ndarray
            The fault coordinates of the point.

        Returns
        -------
        np.ndarray
            The global coordinates (lat, lon, depth) for this point.

        Raises
        ------
        ValueError
            If the fault coordinates are out of bounds.
        """
        midpoint = np.sum(self.lengths()) / 2
        remaining_length = fault_coordinates[0] + midpoint
        for plane in self.planes:
            plane_length = plane.length
            if remaining_length < plane_length:
                return plane.plane_coordinates_to_global_coordinates(
                    np.array(
                        [
                            remaining_length / plane_length - 1 / 2,
                            fault_coordinates[1] / plane.width - 1 / 2,
                        ]
                    ),
                )
            remaining_length -= plane_length
        raise ValueError("Specified fault coordinates out of bounds.")
