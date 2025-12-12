import json
import os
import shutil
from pathlib import PurePath
from typing import Optional


class FileStore:
    """
    General filestore to be used by PoppunkFileStore
    """

    def __init__(self, path):
        """
        :param path: path to folder
        """
        self._path = path
        os.makedirs(path, exist_ok=True)

    def filename(self, file_hash) -> str:
        """
        :param file_hash: [file hash]
        :return str: [path to file incl. filename]
        """
        return os.path.join(self._path, f"{file_hash}.json")

    def get(self, file_hash) -> str:
        """
        :param file_hash: [file hash]
        :return str: [sketch]
        """
        src = self.filename(file_hash)
        if not os.path.exists(src):
            raise Exception(f"Sketch for hash '{file_hash}' not found in storage")
        else:
            with open(src, "r") as fp:
                sketch = json.load(fp)
        return sketch

    def exists(self, file_hash) -> bool:
        """
        :param file_hash: [file hash]
        :return bool: [whether file exists]
        """
        return os.path.exists(self.filename(file_hash))

    def put(self, file_hash, sketch) -> None:
        """
        :param file_hash: [file hash]
        :param sketch: [sketch to be stored]
        """
        dst = self.filename(file_hash)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst, "w") as fp:
            json.dump(sketch, fp)


class PoppunkFileStore:
    """
    Filestore that provides paths to poppunk in- and outputs
    """

    def __init__(self, storage_location):
        """
        :param storage_location: [path to storage location]
        """
        self.storage_location = storage_location
        self.input = FileStore(f"{storage_location}/json")
        self.output_base = PurePath(storage_location, "poppunk_output")
        os.makedirs(self.output_base, exist_ok=True)

    def output(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to output folder]
        """
        return str(PurePath(self.output_base, p_hash))

    def output_sub_lineages(self, p_hash: str, cluster: str, rank: str) -> str:
        """
        :param p_hash: [project hash]
        :param cluster: [cluster string full. e.g GPSC2]
        :param rank: [rank number]
        :return str: [path to sub-lineages output folder]
        """
        return str(PurePath(self.output(p_hash), f"sub_lineages_{cluster}", f"rank_{rank}"))

    def setup_output_directory(self, p_hash: str) -> None:
        """
        [Create output directory that stores all files from PopPUNK assign job.
        If the directory already exists, it is removed and recreated]

        :param p_hash: [project hash]
        """
        outdir = self.output(p_hash)
        if os.path.exists(outdir):
            shutil.rmtree(outdir)
        os.makedirs(outdir)

    def output_qc_report(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to qcreport containing failed samples]
        """
        return str(PurePath(self.output(p_hash), f"{p_hash}_qcreport.txt"))

    def output_cluster(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to cluster results file]
        """
        return str(PurePath(self.output(p_hash), "cluster_results.pickle"))

    def output_cluster_csv(self, p_hash)-> str:
        """
        :param p_hash: [project hash]
        :return str: [path to cluster results csv file]
        """
        return str(PurePath(self.output(p_hash), f"{p_hash}_clusters.csv"))

    def external_to_poppunk_clusters(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to mapping between external and poppunk clusters]
        """
        return str(PurePath(self.output(p_hash), "external_to_poppunk_clusters.pickle"))

    def output_visualisations(self, p_hash, cluster) -> str:
        """
        :param p_hash: [project hash]
        :param cluster: [cluster number]
        :return str: [path to visualisations results folder]
        """
        return str(PurePath(self.output(p_hash), f"visualise_{cluster}"))

    def partial_query_graph(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to partial query graph]
        """
        return str(PurePath(self.output(p_hash), f"{p_hash}_query.subset"))

    def query_sketches_hdf5(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to query sketches hdf5 file]
        """
        return str(PurePath(self.output(p_hash), f"{p_hash}.h5"))

    def include_file(self, p_hash: str, cluster: str) -> str:
        """
        :param p_hash: [project hash]
        :param cluster: [internal cluster number or combined
        internal cluster numbers separated by '_']
        :return str: [path to include file]

        """
        return str(PurePath(self.output(p_hash), f"include{cluster}.txt"))

    def external_previous_query_clustering_path(self, p_hash) -> str:
        """
        Generates the file path for the external
        previous query clustering results.

        :param p_hash (str): The hash value representing the query.

        :return str: [The file path to the external]
        previous query clustering CSV file.
        """
        return str(PurePath(self.output(p_hash), f"{p_hash}_external_clusters.csv"))

    def previous_query_clustering(self, p_hash) -> str:
        """
        Returns previous query clustering csv file.
        This is a generated file when poppunk assigns clusters

        :param p_hash: [project hash]
        :return str: [path to previous clustering file]
        """
        return (
            self.external_previous_query_clustering_path(p_hash)
            if self.has_external_previous_query_clustering(p_hash)
            else str(PurePath(self.output(p_hash), f"{p_hash}_clusters.csv"))
        )

    def has_external_previous_query_clustering(self, p_hash) -> bool:
        """
        Checks if an external previous
        query clustering file exists for the given hash.

        :param p_hash: The hash value representing the previous query.
        :return bool: [True if the file exists, False otherwise.]
        """
        return os.path.exists(self.external_previous_query_clustering_path(p_hash))

    def microreact_json(self, p_hash, cluster) -> str:
        """
        :param p_hash: [project hash]
        :param cluster: [cluster number]
        :return str: [path to microreact json file]
        """
        return str(
            PurePath(
                self.output_visualisations(p_hash, cluster),
                f"visualise_{cluster}.microreact",
            )
        )

    def pruned_network_output_component(self, p_hash, component: str, cluster) -> str:
        """
        [Generates the path to the pruned network component file
        for the given project hash and component number.]

        :param p_hash: [project hash]
        :param component: [component number,
            which is the same as raw cluster number]
        :param cluster: [assigned cluster number]
        :return str: [path to pruned network component file]
        """
        return str(
            PurePath(
                self.output_visualisations(p_hash, cluster),
                f"pruned_visualise_{cluster}_component_{component}.graphml",
            )
        )

    def tmp(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to tmp folder]
        """
        tmp_path = PurePath(self.output(p_hash), "tmp")
        os.makedirs(tmp_path, exist_ok=True)
        return str(tmp_path)

    def output_tmp(self, p_hash) -> str:
        """
        Generates the path to the full assign output folder when using full db.

        :param p_hash: [project hash]
        :return str: [path to full assign output folder]
        """
        path = PurePath(self.tmp(p_hash), p_hash)
        os.makedirs(path, exist_ok=True)
        return str(path)

    def partial_query_graph_tmp(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to partial query graph]
        """
        return str(PurePath(self.output_tmp(p_hash), f"{p_hash}_query.subset"))

    def external_previous_query_clustering_tmp(self, p_hash) -> str:
        """>
        :param p_hash (str): The hash value representing the query.
        :return str: [The file path to the external]
        """
        return str(
            PurePath(
                self.output_tmp(p_hash),
                f"{p_hash}_external_clusters.csv",
            )
        )

    def tmp_output_metadata(self, p_hash: str) -> str:
        """
        [Generates the path to the metadata csv file
        for the given project hash.]

        :param p_hash: [project hash]
        :return str: [path to metadata file]
        """
        return str(PurePath(self.tmp(p_hash), "metadata.csv"))


class DatabaseFileStore:
    """
    Filestore that provides paths to the database
    """

    def __init__(
        self,
        full_path: str,
        external_clusters_file: Optional[str] = None,
        db_metadata_file: Optional[str] = None,
        sub_lineages_db_path: Optional[str] = None,
    ):
        """
        :param full_path: [path to database]
        """
        self.db = full_path
        self.path = str(PurePath(full_path).parent)
        self.name = str(PurePath(full_path).stem)
        self.distances = str(PurePath(self.db, self.name).with_suffix(".dists"))
        self.previous_clustering = str(PurePath(self.db, f"{self.name}_clusters.csv"))
        self.external_clustering = (
            str(PurePath("beebop", "resources", external_clusters_file)) if external_clusters_file else None
        )
        self.metadata = str(PurePath("beebop", "resources", db_metadata_file)) if db_metadata_file else None
        self.sub_lineages_db_path = sub_lineages_db_path
