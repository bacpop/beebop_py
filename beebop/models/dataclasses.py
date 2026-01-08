from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Optional

from beebop.config import DatabaseFileStore, PoppunkFileStore


@dataclass
class ClusteringConfig:
    species: str
    p_hash: str
    args: SimpleNamespace
    external_clusters_prefix: Optional[str]
    fs: PoppunkFileStore
    full_db_fs: DatabaseFileStore
    ref_db_fs: DatabaseFileStore
    db_funcs: dict[str, Any]
    out_dir: str


@dataclass
class Qc:
    run_qc: bool
    type_isolate: Optional[str]
    max_a_dist: float
    max_pi_dist: float
    prop_zero: float
    prop_n: float
    max_merge: int
    betweenness: bool
    retain_failures: bool
    no_remove: bool
    length_range: list[int]
    upper_n: Optional[int]


@dataclass
class SpeciesConfig:
    refdb: str
    fulldb: str
    external_cluster_prefix: Optional[str]
    external_clusters_file: Optional[str]
    db_metadata_file: Optional[str]
    sublineages_db: Optional[str]
    qc_dict: Qc


@dataclass
class ResponseError:
    error: str
    detail: Optional[str] = None


@dataclass
class ResponseBody:
    status: str
    errors: list[ResponseError]
    data: Any


@dataclass
class LocationMetadata:
    SampleCounts: int
    Latitude: float
    Longitude: float
