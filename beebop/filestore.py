import os
import json
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

    def filename(self, hash) -> str:
        """
        :param hash: [file hash]
        :return str: [path to file incl. filename]
        """
        return os.path.join(self._path, f"{hash}.json")

    def get(self, hash) -> str:
        """
        :param hash: [file hash]
        :return str: [sketch]
        """
        src = self.filename(hash)
        if not os.path.exists(src):
            raise Exception(f"Sketch for hash '{hash}' not found in storage")
        else:
            with open(src, "r") as fp:
                sketch = json.load(fp)
        return sketch

    def exists(self, hash) -> bool:
        """
        :param hash: [file hash]
        :return bool: [whether file exists]
        """
        return os.path.exists(self.filename(hash))

    def put(self, hash, sketch) -> None:
        """
        :param hash: [file hash]
        :param sketch: [sketch to be stored]
        """
        dst = self.filename(hash)
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

    def ensure_output_dir_exists(self, p_hash) -> None:
        """
        :param p_hash: [project hash]
        """
        outdir = self.output(p_hash)
        if not os.path.exists(outdir):
            os.mkdir(outdir)

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

    def external_to_poppunk_clusters(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to mapping between external and poppunk clusters]
        """
        return str(
            PurePath(
                self.output(p_hash), "external_to_poppunk_clusters.pickle"
            )
        )

    def output_microreact(self, p_hash, cluster) -> str:
        """
        :param p_hash: [project hash]
        :param cluster: [cluster number]
        :return str: [path to microreact results folder]
        """
        return str(PurePath(self.output(p_hash), f"microreact_{cluster}"))

    def output_network(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to network results folder]
        """
        return str(PurePath(self.output(p_hash), "network"))

    def partial_query_graph(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to partial query graph]
        """
        return str(PurePath(self.output(p_hash), f"{p_hash}_query.subset"))

    def include_files(self, p_hash, cluster) -> str:
        """
        :param p_hash: [project hash]
        :param cluster: [cluster number]
        :return str: [path to include files]
        """
        return str(PurePath(self.output(p_hash), f"include{cluster}.txt"))

    def network_file(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to network file]
        """
        return str(PurePath(self.output(p_hash), f"{p_hash}_graph.gt"))

    def external_previous_query_clustering_path(self, p_hash) -> str:
        """
        Generates the file path for the external
        previous query clustering results.

        :param p_hash (str): The hash value representing the query.

        :return str: [The file path to the external]
        previous query clustering CSV file.
        """
        return str(
            PurePath(self.output(p_hash), f"{p_hash}_external_clusters.csv")
        )

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
        return os.path.exists(
            self.external_previous_query_clustering_path(p_hash)
        )

    def distances(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to distances file]
        """
        return str(PurePath(self.output(p_hash), p_hash).with_suffix(".dists"))

    def microreact_json(self, p_hash, cluster) -> str:
        """
        :param p_hash: [project hash]
        :param cluster: [cluster number]
        :return str: [path to microreact json file]
        """
        return str(
            PurePath(
                self.output(p_hash),
                f"microreact_{cluster}",
                (f"microreact_{cluster}.microreact"),
            )
        )

    def network_output_csv(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to network csv file]
        """
        return str(
            PurePath(self.output(p_hash), "network", "network_cytoscape.csv")
        )

    def network_output_component(self, p_hash, component_number) -> str:
        """
        :param p_hash: [project hash]
        :param component_number: [component number, which is the same as cluster number]
        :return str: [path to network component file]
        """
        return str(
            PurePath(
                self.output(p_hash),
                "network",
                f"network_component_{component_number}.graphml",
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

    def assign_output_full(self, p_hash) -> str:
        """ 
        Generates the path to the full assign output folder.

        :param p_hash: [project hash]
        :return str: [path to full assign output folder]
        """
        path = PurePath(self.tmp(p_hash), p_hash)
        os.makedirs(path, exist_ok=True)
        return str(path)

    def partial_query_graph_full_assign(self, p_hash) -> str:
        """
        :param p_hash: [project hash]
        :return str: [path to partial query graph]
        """
        return str(
            PurePath(self.assign_output_full(p_hash), f"{p_hash}_query.subset")
        )
    def external_previous_query_clustering_path_full_assign(self, p_hash) -> str:
        """
        :param p_hash (str): The hash value representing the query.
        :return str: [The file path to the external]
        """
        return str(
            PurePath(self.assign_output_full(p_hash), f"{p_hash}_external_clusters.csv")
        )


class DatabaseFileStore:
    """
    Filestore that provides paths to the database
    """

    def __init__(
        self, full_path: str, external_clusters_file: Optional[str] = None
    ):
        """
        :param full_path: [path to database]
        """
        self.db = full_path
        self.path = str(PurePath(full_path).parent)
        self.name = str(PurePath(full_path).stem)
        self.distances = str(
            PurePath(self.db, self.name).with_suffix(".dists")
        )
        self.previous_clustering = str(
            PurePath(self.db, f"{self.name}_clusters.csv")
        )
        self.external_clustering = (
            str(PurePath("beebop", "resources", external_clusters_file))
            if external_clusters_file
            else None
        )
