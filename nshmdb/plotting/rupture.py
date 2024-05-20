"""
Module to plot ruptures  using PyGMT.

This module provides functionality to visualize fault ruptures on a map using PyGMT.

Functions:
    plot_rupture: Plot ruptures on faults.
"""

import numpy as np
from nshmdb.fault import Fault
from pygmt_helper import plotting


def plot_rupture(title: str, faults: list[Fault]):
    """Plot faults involved in a rupture scenario using pygmt.

    Parameters
    ----------
    title : str
        The title of the figure.
    faults : list[Fault]
        The list of faults involved in the rupture.
    """
    corners = np.vstack([fault.corners() for fault in faults])
    region = (
        corners[:, 1].min() - 0.5,
        corners[:, 1].max() + 0.5,
        corners[:, 0].min() - 0.25,
        corners[:, 0].max() + 0.25,
    )
    fig = plotting.gen_region_fig(title, region=region)

    for rupture_fault in faults:
        for plane in rupture_fault.planes:
            corners = plane.corners
            fig.plot(
                x=corners[:, 1].tolist() + [corners[0, 1]],
                y=corners[:, 0].tolist() + [corners[0, 0]],
                pen="1p",
                fill="red",
            )

    fig.show()
