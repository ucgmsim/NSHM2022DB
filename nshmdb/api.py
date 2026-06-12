"""Weka API module for fetching and compositing NSHM solution files."""

import copy
import io
import zipfile
from collections.abc import Generator, Iterator
from dataclasses import dataclass
from io import BytesIO
from pathlib import PurePath
from typing import TextIO
from zipfile import ZipFile

import geojson
import numpy as np
import pandas as pd
import pyproj
import requests
import shapely
from geojson.feature import FeatureCollection

from nshmdb.nshmdb import FaultInfo, FaultSystem
from qcore import coordinates
from source_modelling.sources import Fault, Plane

API_URL = "https://nshm-api.gns.cri.nz/weka-app-api/graphql"

SolutionVersion = tuple[int, int, int]

FAULT_INFORMATION_PATH = PurePath("ruptures") / "fault_sections.geojson"
RUPTURE_FAULT_JOIN_PATH = PurePath("ruptures") / "indices.csv"
RUPTURE_RATES_PATH = PurePath("solution") / "rates.csv"
RUPTURE_PROPERTIES_PATH = PurePath("ruptures") / "properties.csv"
MFDS_PATH = PurePath("ruptures") / "sub_seismo_on_fault_mfds.csv"

HIKURANGI_NAME = "Hikurangi, Kermadec to Louisville ridge, 30km - with slip deficit smoothed near East Cape and locked near trench."
PUYSEGUR_NAME = "Puysegur, 15km, 50% coupling, corrected dip direction"


def _get_grouped_source_ids(
    api_key: str, version: SolutionVersion
) -> dict[str, list[tuple[float, str]]]:
    """
    Fetches logic tree data and groups inversion IDs by source type.

    Parameters
    ----------
    api_key : str
        The authentication key for the Weka API.
    version : SolutionVersion
        The (major, minor, patch) version of the NSHM solution.

    Returns
    -------
    dict[str, list[tuple[float, str]]]
        A mapping of source logic tree short names to lists of their associated
        (weight, inversion_id) tuples.
    """
    major, minor, patch = version
    payload = {
        "query": """query LogicTreePageQuery($version: String!) {
          get_model(version: $version) {
            source_logic_tree {
              branch_sets {
                short_name
                branches {
                  weight
                  sources {
                    __typename
                    ... on BranchInversionSource {
                      inversion_id
                    }
                  }
                }
              }
            }
          }
        }""",
        "variables": {"version": f"NSHM_v{major}.{minor}.{patch}"},
    }

    response = requests.post(API_URL, json=payload, headers={"X-API-KEY": api_key})
    response.raise_for_status()
    data = response.json()

    source_ids = {}
    branch_sets = (
        data.get("data", {})
        .get("get_model", {})
        .get("source_logic_tree", {})
        .get("branch_sets", [])
    )

    for branch_set in branch_sets:
        short_name = branch_set.get("short_name")
        if not short_name:
            continue

        if short_name not in source_ids:
            source_ids[short_name] = []

        for branch in branch_set.get("branches", []):
            weight = branch["weight"]
            for source in branch.get("sources", []):
                inversion_id = source.get("inversion_id")
                if inversion_id and inversion_id not in source_ids[short_name]:
                    source_ids[short_name].append((weight, inversion_id))

    return source_ids


def _get_solution_download_link(api_key: str, node_id: str) -> str:
    """
    Retrieves the download URL for a specific solution node.

    Parameters
    ----------
    api_key : str
        The authentication key for the Weka API.
    node_id : str
        The GraphQL node ID for the inversion solution file.

    Returns
    -------
    str
        The file download URL.

    Raises
    ------
    ValueError
        If the API fails to return a valid file URL for the given node ID.
    """
    payload = {
        "query": """query InversionSolutionQuery($id: ID!) {
          node(id: $id) {
            ... on FileInterface {
              file_name
              file_url
            }
          }
        }""",
        "variables": {"id": node_id},
    }

    response = requests.post(API_URL, json=payload, headers={"X-API-KEY": api_key})
    response.raise_for_status()
    data = response.json()

    node = data.get("data", {}).get("node", {})
    url = node.get("file_url")
    if not url:
        raise ValueError(f"Invalid solution id: {node_id}")
    return url


def _download_nshm_solution(url: str) -> ZipFile:
    """
    Downloads and opens a zipped NSHM solution file into memory.

    Parameters
    ----------
    url : str
        The URL pointing to the solution zip file.

    Returns
    -------
    ZipFile
        An opened zip file object containing the solution files.
    """
    with requests.get(url) as f:
        return ZipFile(BytesIO(f.content), "r")


def infer_fault_system(feature_collection: FeatureCollection) -> FaultSystem:
    """
    Infers the fault system from an NSHM 2022 feature collection.

    Parameters
    ----------
    feature_collection : FeatureCollection
        The GeoJSON object containing the fault definitions.

    Returns
    -------
    FaultSystem
        The enumerated fault system associated with the collection.
    """
    name = feature_collection.features[0].properties["ParentName"]
    if name == HIKURANGI_NAME:
        return FaultSystem.Hikurangi
    elif name == PUYSEGUR_NAME:
        return FaultSystem.Puysegur
    return FaultSystem.Crustal


def _infer_dip_direction(start: np.ndarray, end: np.ndarray) -> float:
    """
    Calculates the standard dip direction based on a line segment.

    Parameters
    ----------
    start : np.ndarray
        The starting (lon, lat) coordinates.
    end : np.ndarray
        The ending (lon, lat) coordinates.

    Returns
    -------
    float
        The inferred dip direction in degrees (strike direction + 90 degrees).
    """
    geod = pyproj.Geod(ellps="WGS84")
    strike_direction, _, _ = geod.inv(start[0], start[1], end[0], end[1])
    return strike_direction + 90


def _extract_faults_from_info(
    fault_info_list: FeatureCollection,
    fault_system: FaultSystem,
) -> list[FaultInfo]:
    """
    Extracts fault geometry definitions from a GeoJSON feature collection.

    Parameters
    ----------
    fault_info_list : FeatureCollection
        The GeoJSON object containing the fault trace features and properties.
    fault_system : FaultSystem
        The overarching fault system to associate these faults with.

    Returns
    -------
    list[FaultInfo]
        A list of initialized fault information objects.
    """
    faults = []

    for fault_feature in fault_info_list.features:
        wgs_coords = list(geojson.utils.coords(fault_feature))
        fault_trace = shapely.LineString(
            coordinates.wgs_depth_to_nztm(np.array(wgs_coords)[:, ::-1])[:, :2]
        )

        fault_trace_old = copy.deepcopy(fault_trace)
        fault_trace = shapely.remove_repeated_points(fault_trace, 0)
        trace_coords = np.array(fault_trace.coords)

        fault_id = fault_feature.properties["FaultID"]
        name = fault_feature.properties["ParentName"]
        top = fault_feature.properties["UpDepth"]
        bottom = fault_feature.properties["LowDepth"]
        dip_dir = fault_feature.properties.get("DipDir")
        dip = fault_feature.properties["DipDeg"]
        rake = fault_feature.properties["Rake"]

        if not shapely.equals_exact(fault_trace, fault_trace_old):
            print(f"Warning: Fault trace for {name} was altered.")

        if dip_dir is None:
            dip_dir = _infer_dip_direction(wgs_coords[0], wgs_coords[1])

        planes = [
            Plane.from_nztm_trace(
                np.array([trace_coords[j], trace_coords[j + 1]]),
                top,
                bottom,
                dip,
                dip_dir=dip_dir if dip != 90 else 0,
            )
            for j in range(len(trace_coords) - 1)
        ]

        faults.append(
            FaultInfo(
                fault_id=fault_id,
                fault_system=fault_system,
                name=name,
                rake=rake,
                tect_type=None,
                fault=Fault(planes),
            )
        )
    return faults


def _extract_mfds(solution: ZipFile, fault_system: FaultSystem) -> pd.DataFrame | None:
    """
    Extracts the magnitude-frequency distributions (MFDs) from a solution file.

    Parameters
    ----------
    solution : ZipFile
        The opened zip file containing the solution context.
    fault_system : FaultSystem
        The fault system to associate with the parsed MFD records.

    Returns
    -------
    pd.DataFrame | None
        A melted dataframe of MFDs with positive rates, or None if the
        target file is missing.
    """
    mfds_handle_path = zipfile.Path(solution) / MFDS_PATH

    if not mfds_handle_path.exists():
        return None

    with mfds_handle_path.open() as mfds_file_handle:
        mfds = pd.read_csv(mfds_file_handle)
        mfds = mfds.rename(columns={"Section Index": "nshm_id"})
        mfds = mfds.melt(id_vars=["nshm_id"], var_name="magnitude", value_name="rate")
        mfds = mfds[mfds["rate"] > 0]
        mfds["fault_system"] = fault_system
        return mfds


def _extract_ruptures(solution: ZipFile, fault_system: FaultSystem) -> pd.DataFrame:
    """
    Extracts rupture properties and bounds them with annual rates.

    Parameters
    ----------
    solution : ZipFile
        The opened zip file containing the solution context.
    fault_system : FaultSystem
        The fault system to associate with the parsed ruptures.

    Returns
    -------
    pd.DataFrame
        A dataframe indexed by rupture index containing magnitude, area, length,
        annual rates, and the fault system identifier.
    """
    with (
        solution.open(str(RUPTURE_RATES_PATH)) as rupture_rates_handle,
        solution.open(str(RUPTURE_PROPERTIES_PATH)) as rupture_properties_handle,
    ):
        rupture_rates = pd.read_csv(rupture_rates_handle).set_index("Rupture Index")
        rupture_properties = pd.read_csv(rupture_properties_handle).set_index(
            "Rupture Index"
        )
        rupture_properties = rupture_properties.join(rupture_rates)
        rupture_properties = rupture_properties.rename(
            columns={
                "Magnitude": "magnitude",
                "Area (m^2)": "area",
                "Length (m)": "len",
                "Annual Rate": "rate",
            }
        )
        rupture_properties = rupture_properties[["magnitude", "area", "len", "rate"]]
        rupture_properties["fault_system"] = fault_system
        return rupture_properties


def _read_ruptures(handle: TextIO) -> tuple[np.ndarray, np.ndarray]:
    """
    Reads aligned arrays of rupture IDs and their composite fault indices.

    Parameters
    ----------
    handle : TextIO
        The readable text stream of the indices CSV.

    Returns
    -------
    tuple[np.ndarray, np.ndarray]
        A tuple of (rupture_ids, fault_ids), where each sequence is parallel.
    """
    _ = next(handle)  # skip header

    rupture_ids_raw = []
    fault_ids_raw = []

    for line in handle:
        parts = line.split(",")
        rupture_index = parts[0]
        num_segments = int(parts[1])
        fault_ids_raw.extend(parts[2 : 2 + num_segments])
        rupture_ids_raw.append((rupture_index, num_segments))

    counts = np.fromiter(
        (r[1] for r in rupture_ids_raw), dtype=np.int32, count=len(rupture_ids_raw)
    )
    r_ids = np.fromiter(
        (r[0] for r in rupture_ids_raw), dtype=np.int32, count=len(rupture_ids_raw)
    )

    return (
        np.repeat(r_ids, counts),
        np.array(fault_ids_raw, dtype=np.int32),
    )


def _extract_rupture_join_table(
    solution: ZipFile, fault_system: FaultSystem
) -> pd.DataFrame:
    """
    Extracts the normalized rupture-fault intersection mapping.

    Parameters
    ----------
    solution : ZipFile
        The opened zip file containing the solution context.
    fault_system : FaultSystem
        The system identifier to apply to the join rows.

    Returns
    -------
    pd.DataFrame
        A mapping dataframe resolving one-to-many relationships between
        ruptures and underlying fault segments.
    """
    with solution.open(str(RUPTURE_FAULT_JOIN_PATH)) as rupture_fault_join_handle:
        text_handle = io.TextIOWrapper(rupture_fault_join_handle, encoding="utf-8")
        rupture_ids, fault_ids = _read_ruptures(text_handle)
        return pd.DataFrame(
            {
                "rupture_id": rupture_ids,
                "fault_id": fault_ids,
                "fault_system": fault_system,
            }
        )


@dataclass
class NSHMSolution:
    """
    Data payload representing a composite or partial NSHM logic tree solution.

    Parameters
    ----------
    magnitude_frequency_distribution : pd.DataFrame | None
        The computed magnitude frequency bounds.
    rupture_join_table : pd.DataFrame
        Mapping defining relationships between rupture IDs and fault IDs.
    rupture_properties : pd.DataFrame
        Aggregated physical properties and rate values for specific ruptures.
    faults : list[FaultInfo]
        Collection of generated spatial fault definition payloads.
    """

    magnitude_frequency_distribution: pd.DataFrame | None
    rupture_join_table: pd.DataFrame
    rupture_properties: pd.DataFrame
    faults: list[FaultInfo]


def _merge_branches(solutions: Iterator[tuple[float, ZipFile]]) -> NSHMSolution:
    """
    Aggregates multiple weighted branch solutions into a single composite result.

    Rates and magnitude-frequency values are normalised across the provided sequence.

    Parameters
    ----------
    solutions : Iterator[tuple[float, ZipFile]]
        A stream of branch weights mapped to their respective downloaded
        solution file buffers.

    Returns
    -------
    NSHMSolution
        The aggregated solution containing normalised composite rates.
    """
    first_weight, first_solution = next(solutions)
    with first_solution.open(str(FAULT_INFORMATION_PATH)) as fault_info_handle:
        fault_collection = geojson.load(fault_info_handle)

    fault_system = infer_fault_system(fault_collection)

    # Optimisation here: the faults and rupture join table don't change between
    # solutions in the same fault system so we parse them only once.
    faults = _extract_faults_from_info(fault_collection, fault_system)
    rupture_join_table = _extract_rupture_join_table(first_solution, fault_system)
    rupture_properties = _extract_ruptures(first_solution, fault_system)
    mfds = _extract_mfds(first_solution, fault_system)

    # Now for every subsequent rupture we only extract MFDs and rupture
    # properties (i.e. rates). These are the only things that change between
    # branches of the fault systems.

    # Hikurangi and Puysegur don't have MFDs, so we guard against the None case
    fault_system_has_mfds = mfds is not None
    if fault_system_has_mfds:
        mfds["rate"] *= first_weight

    rupture_properties["rate"] *= first_weight

    # NOTE: You may be tempted to refactor this to for loop into two neat iterator expressions:
    #
    # rupture properties = sum(weight * rate for rate in solutions)
    # mfds = sum(weight * mfds for ...)
    #
    # But the iterator type forces us to iterate exactly once because the
    # iterator is consumed and can't be traversed twice. This is so that we can
    # stream download the content one branch at a time in memory.
    for weight, solution in solutions:
        rates = _extract_ruptures(solution, fault_system)["rate"]
        rupture_properties["rate"] += weight * rates
        if fault_system_has_mfds:
            soln_mfds = _extract_mfds(solution, fault_system)
            # Technically soln_mfds could be None but we can guard against that.
            assert soln_mfds is not None
            mfds["rate"] += weight * soln_mfds["rate"]

    return NSHMSolution(
        magnitude_frequency_distribution=mfds,
        rupture_join_table=rupture_join_table,
        rupture_properties=rupture_properties,
        faults=faults,
    )


def _stack_fault_systems(solutions: list[NSHMSolution]) -> NSHMSolution:
    """
    Concatenates sub-solutions from distinct fault systems.

    Parameters
    ----------
    solutions : list[NSHMSolution]
        The sequence of regional or disparate fault system solutions.

    Returns
    -------
    NSHMSolution
        The combined overarching structural block.
    """
    mfds = [
        s.magnitude_frequency_distribution
        for s in solutions
        if s.magnitude_frequency_distribution is not None
    ]
    if mfds:
        magnitude_frequency_distribution = pd.concat(mfds)
    else:
        magnitude_frequency_distribution = None

    return NSHMSolution(
        faults=[f for s in solutions for f in s.faults],
        rupture_properties=pd.concat(s.rupture_properties for s in solutions),
        rupture_join_table=pd.concat(s.rupture_join_table for s in solutions),
        magnitude_frequency_distribution=magnitude_frequency_distribution,
    )


def _solution_stream(
    api_key: str, solution_ids: list[tuple[float, str]]
) -> Generator[tuple[float, ZipFile], None, None]:
    """
    Lazily fetches branch zip files to minimize memory overhead.

    Parameters
    ----------
    api_key : str
        The authentication key for the Weka API.
    solution_ids : list[tuple[float, str]]
        A list of mapped tuple weights and node string IDs to process.

    Yields
    ------
    tuple[float, ZipFile]
        A tuple of the branch weight and its opened zip payload.
    """
    # Using an iterator pattern here ensures that the each zip file is freed in
    # memory after we finish processing it which reduces the total memory usage.
    for weight, node_id in solution_ids:
        url = _get_solution_download_link(api_key, node_id)
        yield weight, _download_nshm_solution(url)


def download_composite_solution(
    api_key: str, solution_version: SolutionVersion
) -> NSHMSolution:
    """
    Downloads and composites an entire weighted solution system.

    Parameters
    ----------
    api_key : str
        The authentication key for the Weka API.
    solution_version : SolutionVersion
        The targeted integer tuple formulation (major, minor, patch) pointing
        to a specific model release.

    Returns
    -------
    NSHMSolution
        The resultant singular concatenated solution package resolving all
        logic trees for the requested version context.
    """
    ids = _get_grouped_source_ids(api_key, solution_version)
    return _stack_fault_systems(
        [
            _merge_branches(_solution_stream(api_key, solution_ids))
            for solution_ids in ids.values()
            if solution_ids
        ]
    )
