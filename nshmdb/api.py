import copy
from collections.abc import Iterable
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Self
from zipfile import Path, ZipFile

import geojson
import numpy as np
import pandas as pd
import requests
import shapely
from geojson.feature import FeatureCollection

from nshmdb.nshmdb import FaultInfo, FaultSystem
from qcore import coordinates
from source_modelling.sources import Fault, Plane

API_URL = "https://nshm-api.gns.cri.nz/weka-app-api/graphql"

SolutionVersion = tuple[int, int, int]


def api_headers(api_key: str) -> dict[str, Any]:
    return {"X-API-KEY": api_key}


def format_solution(version: SolutionVersion) -> str:
    major, minor, patch = version
    return f"NSHM_v{major}.{minor}.{patch}"


def get_grouped_source_ids(
    api_key: str, version: SolutionVersion
) -> dict[str, list[str]]:
    """
    Step 1: Fetches logic tree data and groups inversion IDs by source type.
    Returns a dictionary like {'CRU': ['id1', 'id2'], 'HIK': ['id3']}
    """
    payload = {
        "query": """query LogicTreePageQuery($version: String!) {
          get_model(version: $version) {
            source_logic_tree {
              branch_sets {
                short_name
                branches {
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
        "variables": {"version": format_solution(version)},
    }

    response = requests.post(API_URL, json=payload, headers=api_headers(api_key))
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
            for source in branch.get("sources", []):
                inversion_id = source.get("inversion_id")
                if inversion_id and inversion_id not in source_ids[short_name]:
                    source_ids[short_name].append(inversion_id)

    return source_ids


def get_solution_download_link(api_key: str, node_id: str) -> str | None:
    """
    Step 2: Uses the InversionSolutionQuery to get the file_url and file_name
    for a specific node ID.
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

    response = requests.post(API_URL, json=payload, headers=api_headers(api_key))
    response.raise_for_status()
    data = response.json()

    node = data.get("data", {}).get("node", {})

    return node.get("file_url")


def _download_nshm_solution(url: str) -> ZipFile:
    with requests.get(url) as f:
        return ZipFile(BytesIO(f.content), "r")


FAULT_INFORMATION_PATH = Path("ruptures") / "fault_sections.geojson"
RUPTURE_FAULT_JOIN_PATH = Path("ruptures") / "fast_indices.csv"
RUPTURE_RATES_PATH = Path("solution") / "rates.csv"
RUPTURE_PROPERTIES_PATH = Path("ruptures") / "properties.csv"
MFDS_PATH = Path("ruptures") / "sub_seismo_on_fault_mfds.csv"

HIKURANGI_NAME = "Hikurangi, Kermadec to Louisville ridge, 30km - with slip deficit smoothed near East Cape and locked near trench."
PUYSEGUR_NAME = "Puysegur, 15km, 50% coupling, corrected dip direction"


def infer_fault_system(geojson: FeatureCollection) -> FaultSystem:
    """Infer the fault system from an NSHM 2022 feature collection."""
    features = geojson.features
    test_feature = features[0]
    name = test_feature.properties["ParentName"]
    if name == HIKURANGI_NAME:
        return FaultSystem.Hikurangi
    elif name == PUYSEGUR_NAME:
        return FaultSystem.Puysegur
    return FaultSystem.Crustal


def _extract_faults_from_info(
    fault_info_list: FeatureCollection,
) -> list[FaultInfo]:
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
    fault_system = infer_fault_system(fault_info_list)
    faults = []
    for i in range(len(fault_info_list.features)):
        fault_feature = fault_info_list[i]
        fault_trace = shapely.LineString(
            coordinates.wgs_depth_to_nztm(
                np.array(list(geojson.utils.coords(fault_feature)))[:, ::-1]
            )
        )
        fault_trace_old = copy.deepcopy(fault_trace)
        fault_trace = shapely.remove_repeated_points(fault_trace, 0)
        trace_coords = np.array(fault_trace.coords)
        fault_id = fault_feature.properties["FaultID"]
        name = fault_feature.properties["ParentName"]
        top = fault_feature.properties["UpDepth"]
        bottom = fault_feature.properties["LowDepth"]
        dip_dir = fault_feature.properties["DipDir"]
        dip = fault_feature.properties["DipDeg"]
        rake = fault_feature.properties["Rake"]

        if not shapely.equals_exact(fault_trace, fault_trace_old):
            print(f"Warning: Fault trace for {name} was altered.")

        planes = []
        for j in range(len(trace_coords) - 1):
            top_left = trace_coords[j]
            top_right = trace_coords[j + 1]
            planes.append(
                Plane.from_nztm_trace(
                    np.array([top_left, top_right]),
                    top,
                    bottom,
                    dip,
                    coordinates.great_circle_bearing_to_nztm_bearing(
                        coordinates.nztm_to_wgs_depth(top_left), 1, dip_dir
                    )
                    if dip != 90
                    else 0,
                ),
            )
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


def _extract_mfds(solution: ZipFile) -> pd.DataFrame:
    with solution.open(str(MFDS_PATH)) as mfds_file_handle:
        mfds = pd.read_csv(mfds_file_handle)
        mfds = mfds.rename(columns={"Section Index": "nshm_id"})
        mfds = mfds.melt(id_vars=["nshm_id"], var_name="magnitude", value_name="rate")
        mfds = mfds[mfds["rate"] > 0]
        return mfds


def _extract_faults(solution: ZipFile) -> list[FaultInfo]:
    with solution.open(str(FAULT_INFORMATION_PATH)) as fault_info_handle:
        return _extract_faults_from_info(geojson.load(fault_info_handle))


def _extract_ruptures(solution: ZipFile) -> pd.DataFrame:
    with (
        solution.open(str(RUPTURE_RATES_PATH)) as rupture_rates_handle,
        solution.open(str(RUPTURE_PROPERTIES_PATH)) as rupture_properties_handle,
    ):
        rupture_rates = pd.read_csv(rupture_rates_handle).set_index("Rupture Index")
        rupture_rates.rename_axis("nshm_id")
        rupture_properties = pd.read_csv(rupture_properties_handle).set_index(
            "Rupture Index"
        )
        rupture_properties.rename_axis("nshm_id")
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
        return rupture_properties


def _extract_rupture_join_table(solution: ZipFile) -> pd.DataFrame:
    with solution.open(str(RUPTURE_FAULT_JOIN_PATH)) as rupture_fault_join_handle:
        rupture_fault_join_df = pd.read_csv(rupture_fault_join_handle)
        rupture_fault_join_df["section"] = rupture_fault_join_df["section"].astype(
            "Int64"
        )
        rupture_fault_join_df = rupture_fault_join_df.rename(
            columns={"section": "fault_id", "rupture": "rupture_id"}
        )
        return rupture_fault_join_df


@dataclass
class NSHMSolution:
    magnitude_frequency_distribution: pd.DataFrame
    rupture_join_table: pd.DataFrame
    rupture_properties: pd.DataFrame
    faults: list[FaultInfo]

    @classmethod
    def from_solution_zip_file(cls, solution: ZipFile) -> Self:
        return cls(
            magnitude_frequency_distribution=_extract_mfds(solution),
            faults=_extract_faults(solution),
            rupture_properties=_extract_ruptures(solution),
            rupture_join_table=_extract_rupture_join_table(solution),
        )


def aggregate_solutions(
    solutions: Iterable[tuple[float, ZipFile]],
) -> NSHMSolution:
    composite_solution = None
    for weight, solution in solutions:
        if composite_solution is None:
            composite_solution = NSHMSolution.from_solution_zip_file(solution)
            composite_solution.rupture_properties["rate"] *= weight
        else:
            rupture_properties = _extract_ruptures(solution)
            composite_solution.rupture_properties["rate"] += (
                weight * rupture_properties["rate"]
            )
    if not composite_solution:
        raise ValueError("Empty solution stream")

    return composite_solution


def concatenate_solutions(solutions: list[NSHMSolution]) -> NSHMSolution:
    mfds = []
    rupture_join_tables = []
    rupture_properties = []
    faults = []
    for solution in solutions:
        fault_system = solution.faults[0].fault_system
        faults.extend(solution.faults)

        mfd = solution.magnitude_frequency_distribution
        mfd["fault_system"] = fault_system
        mfds.append(mfd)

        rupture_join_table = solution.rupture_join_table
        rupture_join_table["fault_system"] = fault_system
        rupture_join_tables.append(rupture_join_table)

        rupture_properties_table = solution.rupture_properties
        rupture_properties_table["fault_system"] = fault_system
        rupture_properties.append(rupture_properties_table)

    return NSHMSolution(
        faults=faults,
        magnitude_frequency_distribution=pd.concat(mfds),
        rupture_join_table=pd.concat(rupture_join_tables),
        rupture_properties=pd.concat(rupture_properties),
    )
