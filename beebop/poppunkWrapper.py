from PopPUNK.assign import assign_query_hdf5
from PopPUNK.visualise import generate_visualisations
from beebop.filestore import DatabaseFileStore
import shutil
import os
import sys


class PoppunkWrapper:
    """
    [Wrapper to separate the poppunk function calls that require an enormous
    amount of arguments from the main scripts.]
    """
    def __init__(self, fs, db_paths, args, p_hash):
        """
        :param fs: [PoppunkFileStore with paths to in-/outputs]
        :param db_paths: [DatabaseFileStore with paths to db files]
        :param args: [arguments for Poppunk's assign function, stored in
            resources/args.json]
        :param p_hash: [project hash]
        """
        self.fs = fs
        self.db_paths = db_paths
        self.args = args
        self.p_hash = p_hash

    def assign_clusters(self,
                        dbFuncs: DatabaseFileStore,
                        qc_dict: dict,
                        qNames: list) -> None:
        """
        :param dbFuncs: [database functions, generated with poppunks
            setupDBFuncs()]
        :param qc_dict: [dict whether qc should run or not]
        :param qNames: [hd5 database with all sketches]
        """
        assign_query_hdf5(
            dbFuncs=dbFuncs,
            ref_db=self.db_paths.db,
            qNames=qNames,
            output=self.fs.output(self.p_hash),
            qc_dict=qc_dict,
            update_db=self.args.assign.update_db,
            write_references=self.args.assign.write_references,
            distances=self.db_paths.distances,
            serial=self.args.assign.serial,
            threads=self.args.assign.threads,
            overwrite=self.args.assign.overwrite,
            plot_fit=self.args.assign.plot_fit,
            graph_weights=self.args.assign.graph_weights,
            model_dir=self.db_paths.db,
            strand_preserved=self.args.assign.strand_preserved,
            previous_clustering=self.db_paths.db,
            external_clustering=self.fs.external_clustering,
            core=self.args.assign.core_only,
            accessory=self.args.assign.accessory_only,
            gpu_dist=self.args.assign.gpu_dist,
            gpu_graph=self.args.assign.gpu_graph,
            save_partial_query_graph=self.args.assign.save_partial_query_graph
        )

    def create_microreact(self, cluster: str, poppunk_cluster: str) -> None:
        """
        [Generates microreact visualisation output based on previous
        assign_clusters() output.]

        Args:
        :param cluster: [cluster number]
        """
        print(shutil.which('rapidnj'))
        generate_visualisations(
            query_db=self.fs.output(self.p_hash),
            ref_db=self.db_paths.db,
            distances=self.fs.distances(self.p_hash),
            rank_fit=None,
            threads=self.args.visualise.threads,
            output=self.fs.output_microreact(self.p_hash, cluster),
            gpu_dist=self.args.visualise.gpu_dist,
            deviceid=self.args.visualise.deviceid,
            external_clustering=self.fs.external_clustering,
            microreact=True,
            phandango=self.args.visualise.phandango,
            grapetree=self.args.visualise.grapetree,
            cytoscape=self.args.visualise.cytoscape,
            perplexity=self.args.visualise.perplexity,
            maxIter=self.args.visualise.maxIter,
            strand_preserved=self.args.visualise.strand_preserved,
            include_files=self.fs.include_files(self.p_hash, poppunk_cluster),
            model_dir=self.db_paths.db,
            previous_clustering=self.db_paths.previous_clustering,
            previous_query_clustering=(
                self.fs.previous_query_clustering(self.p_hash)),
            previous_mst=None,
            previous_distances=None,
            network_file=self.fs.network_file(self.p_hash),
            gpu_graph=self.args.visualise.gpu_graph,
            info_csv=self.args.visualise.info_csv,
            rapidnj=shutil.which('rapidnj'),
            api_key=None,
            tree=self.args.visualise.tree,
            mst_distances=self.args.visualise.mst_distances,
            overwrite=self.args.visualise.overwrite,
            display_cluster=self.args.visualise.display_cluster
        )

    def create_network(self) -> None:
        """
        [Generates network visualisation output in .graphml format based on
        previous assign_clusters.get_clusters() output.]
        """
        generate_visualisations(
            query_db=self.fs.output(self.p_hash),
            ref_db=self.db_paths.db,
            distances=self.fs.distances(self.p_hash),
            rank_fit=None,
            threads=self.args.visualise.threads,
            output=self.fs.output_network(self.p_hash),
            gpu_dist=self.args.visualise.gpu_dist,
            deviceid=self.args.visualise.deviceid,
            external_clustering=self.fs.external_clustering,
            microreact=self.args.visualise.microreact,
            phandango=self.args.visualise.phandango,
            grapetree=self.args.visualise.grapetree,
            cytoscape=True,
            perplexity=self.args.visualise.perplexity,
            maxIter=self.args.visualise.maxIter,
            strand_preserved=self.args.visualise.strand_preserved,
            include_files=None,
            model_dir=self.db_paths.db,
            previous_clustering=self.db_paths.previous_clustering,
            previous_query_clustering=(
                self.fs.previous_query_clustering(self.p_hash)),
            previous_mst=None,
            previous_distances=None,
            network_file=self.fs.network_file(self.p_hash),
            gpu_graph=self.args.visualise.gpu_graph,
            info_csv=self.args.visualise.info_csv,
            rapidnj=shutil.which('rapidnj'),
            api_key=None,
            tree="none",
            mst_distances=self.args.visualise.mst_distances,
            overwrite=self.args.visualise.overwrite,
            display_cluster=self.args.visualise.display_cluster
        )
