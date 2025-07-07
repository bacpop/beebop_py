import shutil

from PopPUNK.assign import assign_query_hdf5
from PopPUNK.visualise import generate_visualisations

from beebop.config import DatabaseFileStore, PoppunkFileStore


class PoppunkWrapper:
    """
    [Wrapper to separate the poppunk function calls that require an enormous
    amount of arguments from the main scripts.]
    """

    def __init__(
        self,
        fs: PoppunkFileStore,
        db_fs: DatabaseFileStore,
        args,
        p_hash: str,
        species: str,
    ):
        """
        :param fs: [PoppunkFileStore with paths to in-/outputs]
        :param db_fs:
            [DatabaseFileStore with paths to db files and species name]
        :param args: [arguments for Poppunk's assign function, stored in
            resources/args.json]
        :param p_hash: [project hash]
        """
        self.fs = fs
        self.db_fs = db_fs
        self.args = args
        self.p_hash = p_hash
        self.species = species

    def assign_clusters(
        self,
        db_funcs: dict,
        qNames: list[str],
        output: str,
    ) -> None:
        """
        :param db_funcs: [database functions, generated with poppunks
            setupDBFuncs()]
        :param qNames: [hd5 database with all sketches]
        :param output: [output folder for assign_clusters]
        """
        assign_query_hdf5(
            dbFuncs=db_funcs,
            ref_db=self.db_fs.db,
            qNames=qNames,
            output=output,
            qc_dict=vars(getattr(self.args.species, self.species).qc_dict),
            update_db=self.args.assign.update_db,
            write_references=self.args.assign.write_references,
            distances=self.db_fs.distances,
            serial=self.args.assign.serial,
            threads=self.args.assign.threads,
            overwrite=self.args.assign.overwrite,
            plot_fit=self.args.assign.plot_fit,
            graph_weights=self.args.assign.graph_weights,
            model_dir=self.db_fs.db,
            strand_preserved=self.args.assign.strand_preserved,
            previous_clustering=self.db_fs.db,
            external_clustering=self.db_fs.external_clustering,
            core=self.args.assign.core_only,
            accessory=self.args.assign.accessory_only,
            gpu_dist=self.args.assign.gpu_dist,
            gpu_graph=self.args.assign.gpu_graph,
            save_partial_query_graph=self.args.assign.save_partial_query_graph,
            stable=self.args.assign.stable,
            use_full_network=self.args.assign.use_full_network,
        )

    def create_visualisations(self, cluster: str, include_file: str) -> None:
        """
        [Generates visualisation outputs (microreact + network)
        based on previous assign_clusters output.]

        Args:
        :param cluster: [external cluster]
        :param include_file: [path to txt file with isolates
        to include in visualisation]
        """
        print(shutil.which("rapidnj"))
        generate_visualisations(
            query_db=self.fs.output(self.p_hash),
            ref_db=self.db_fs.db,
            distances=self.db_fs.distances,
            rank_fit=None,
            threads=self.args.visualise.threads,
            output=self.fs.output_visualisations(self.p_hash, cluster),
            gpu_dist=self.args.visualise.gpu_dist,
            deviceid=self.args.visualise.deviceid,
            external_clustering=self.db_fs.external_clustering,
            microreact=self.args.visualise.microreact,
            phandango=self.args.visualise.phandango,
            grapetree=self.args.visualise.grapetree,
            cytoscape=self.args.visualise.cytoscape,
            perplexity=self.args.visualise.perplexity,
            maxIter=self.args.visualise.maxIter,
            strand_preserved=self.args.visualise.strand_preserved,
            include_files=include_file,
            model_dir=self.db_fs.db,
            previous_clustering=self.db_fs.previous_clustering,
            previous_query_clustering=(self.fs.previous_query_clustering(self.p_hash)),
            previous_mst=None,
            previous_distances=None,
            network_file=None,
            gpu_graph=self.args.visualise.gpu_graph,
            info_csv=self.fs.tmp_output_metadata(self.p_hash),
            rapidnj=shutil.which("rapidnj"),
            api_key=None,
            tree=self.args.visualise.tree,
            mst_distances=self.args.visualise.mst_distances,
            overwrite=self.args.visualise.overwrite,
            display_cluster=self.args.visualise.display_cluster,
            recalculate_distances=self.args.visualise.recalculate_distances,
            use_partial_query_graph=self.fs.partial_query_graph(self.p_hash),
            tmp=self.fs.tmp(self.p_hash),
            extend_query_graph=self.args.visualise.extend_query_graph,
        )
