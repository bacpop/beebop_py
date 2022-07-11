from PopPUNK.visualise import generate_visualisations
from rq import get_current_job
from redis import Redis


def microreact(p_hash, outdir, db_paths, args):
    """
    generate files to use with microreact.
    Output files are .csv, .dot and .nwk
    (last one only for clusters with >3 isolates)

    p_hash: project hash to find input data (output from assignClusters)
    outdir: directory where input data is stored
    db_paths: location of database
    args: arguments for poppunk functions
    """

    # get results from previous job
    redis = Redis()
    current_job = get_current_job(redis)
    prev_job = current_job.dependency
    prev_result = prev_job.result

    queries_clusters = []
    for item in prev_result.values():
        queries_clusters.append(item['cluster'])

    for cluster_no in set(queries_clusters):
        generate_visualisations(
            query_db=outdir,
            ref_db=db_paths.db,
            distances=outdir + '/' + p_hash + '.dists',
            rank_fit=None,
            threads=args.visualise.threads,
            output=outdir + '/microreact_' + str(cluster_no),
            gpu_dist=args.visualise.gpu_dist,
            deviceid=args.visualise.deviceid,
            external_clustering=args.visualise.external_clustering,
            microreact=True,
            phandango=args.visualise.phandango,
            grapetree=args.visualise.grapetree,
            cytoscape=args.visualise.cytoscape,
            perplexity=args.visualise.perplexity,
            strand_preserved=args.visualise.strand_preserved,
            include_files=outdir + "/include" + str(cluster_no) + ".txt",
            model_dir=db_paths.db,
            previous_clustering=(db_paths.db + '/' +
                                 db_paths.name + '_clusters.csv'),
            previous_query_clustering=outdir + '/' + p_hash + '_clusters.csv',
            previous_mst=None,
            previous_distances=None,
            network_file=outdir + "/" + p_hash + "_graph.gt",
            gpu_graph=args.visualise.gpu_graph,
            info_csv=args.visualise.info_csv,
            rapidnj=args.visualise.rapidnj,
            tree=args.visualise.tree,
            mst_distances=args.visualise.mst_distances,
            overwrite=args.visualise.overwrite,
            core_only=args.visualise.core_only,
            accessory_only=args.visualise.accessory_only,
            display_cluster=args.visualise.display_cluster,
            web=True
        )


def network(p_hash, outdir, db_paths, args):
    """
    generate files to draw a network.
    Output files are .graphml and .csv

    p_hash: project hash to find input data (output from assignClusters)
    outdir: directory where input data is stored
    db_paths: location of database
    args: arguments for poppunk functions

    Currently poppunk does not allow to subset isolates in this mode.
    Ideally we'd want to only display clusters that have a new isolate added.
    """

    generate_visualisations(
        query_db=outdir,
        ref_db=db_paths.db,
        distances=outdir + '/' + p_hash + '.dists',
        rank_fit=None,
        threads=args.visualise.threads,
        output=outdir + '/network',
        gpu_dist=args.visualise.gpu_dist,
        deviceid=args.visualise.deviceid,
        external_clustering=args.visualise.external_clustering,
        microreact=args.visualise.microreact,
        phandango=args.visualise.phandango,
        grapetree=args.visualise.grapetree,
        cytoscape=True,
        perplexity=args.visualise.perplexity,
        strand_preserved=args.visualise.strand_preserved,
        include_files=None,
        model_dir=db_paths.db,
        previous_clustering=(db_paths.db +
                             '/' + db_paths.name + '_clusters.csv'),
        previous_query_clustering=outdir + '/' + p_hash + '_clusters.csv',
        previous_mst=None,
        previous_distances=None,
        network_file=outdir + "/" + p_hash + "_graph.gt",
        gpu_graph=args.visualise.gpu_graph,
        info_csv=args.visualise.info_csv,
        rapidnj=args.visualise.rapidnj,
        tree="nj",
        mst_distances=args.visualise.mst_distances,
        overwrite=args.visualise.overwrite,
        core_only=args.visualise.core_only,
        accessory_only=args.visualise.accessory_only,
        display_cluster=args.visualise.display_cluster,
        web=True
    )
