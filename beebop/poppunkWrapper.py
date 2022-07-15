from PopPUNK.visualise import generate_visualisations


class PoppunkWrapper:
    def __init__(self, fs, db_paths, args, p_hash):
        self.fs = fs
        self.db_paths = db_paths
        self.args = args
        self.p_hash = p_hash

    def create_microreact(self, cluster_no):
        generate_visualisations(
            query_db=self.fs.output(self.p_hash),
            ref_db=self.db_paths.db,
            distances=self.fs.distances(self.p_hash),
            rank_fit=None,
            threads=self.args.visualise.threads,
            output=self.fs.output_microreact(self.p_hash, cluster_no),
            gpu_dist=self.args.visualise.gpu_dist,
            deviceid=self.args.visualise.deviceid,
            external_clustering=self.args.visualise.external_clustering,
            microreact=True,
            phandango=self.args.visualise.phandango,
            grapetree=self.args.visualise.grapetree,
            cytoscape=self.args.visualise.cytoscape,
            perplexity=self.args.visualise.perplexity,
            strand_preserved=self.args.visualise.strand_preserved,
            include_files=self.fs.include_files(self.p_hash, cluster_no),
            model_dir=self.db_paths.db,
            previous_clustering=self.db_paths.previous_clustering,
            previous_query_clustering=(
                self.fs.previous_query_clustering(self.p_hash)),
            previous_mst=None,
            previous_distances=None,
            network_file=self.fs.network_file(self.p_hash),
            gpu_graph=self.args.visualise.gpu_graph,
            info_csv=self.args.visualise.info_csv,
            rapidnj=self.args.visualise.rapidnj,
            tree=self.args.visualise.tree,
            mst_distances=self.args.visualise.mst_distances,
            overwrite=self.args.visualise.overwrite,
            core_only=self.args.visualise.core_only,
            accessory_only=self.args.visualise.accessory_only,
            display_cluster=self.args.visualise.display_cluster,
            web=True
        )
