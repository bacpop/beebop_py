import re


def get_cluster_num(cluster: str) -> str:
    """
    [Extract the numeric part from a cluster label, regardless of the prefix.]

    :param cluster: [cluster from assign result.
    Can be prefixed with external_cluster_prefix]
    :return str: [numeric part of the cluster]
    """
    match = re.search(r"\d+", str(cluster))
    return match.group(0) if match else str(cluster)


def get_lowest_cluster(clusters_str: str) -> int:
    """
    [Get numerically lowest cluster number from semicolon-separated clusters
    string.]

    :param clusters_str: [string of all clusters for a sample, separated by
        semicolons]
    :return int: [lowest cluster number from the string]
    """
    clusters = map(int, clusters_str.split(";"))
    return min(clusters)
