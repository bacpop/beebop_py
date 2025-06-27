import argparse
import os

import graph_tool.all as gt
import pandas as pd

# This script converts a graph-tool graph to a gzipped CSV file.
# PopPUNK's GPU accelerated graphing library cuGraph cannot read .gt files,
# so we need to convert it to csv that cuGraph can read.


def save_gt_graph_as_csv_gzip(graph: gt.Graph, gzip_output_filepath: str):
    edges = [(e.source(), e.target()) for e in graph.edges()]
    # Add vertices with no edges as source and destination as themselves
    no_edge_vertices = [(v, v) for v in graph.get_vertices() if len(graph.get_all_edges(v)) == 0]
    edges.extend(no_edge_vertices)
    df_edges = pd.DataFrame(edges, columns=["source", "destination"])
    df_edges.to_csv(gzip_output_filepath, index=False, compression="gzip")


def get_input_output_paths():
    parser = argparse.ArgumentParser(description="Convert a graph-tool graph to a gzipped CSV file.")
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        required=True,
        help="Path to the input graph-tool .gt file.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input file {args.input} does not exist.")
    if not args.input.endswith(".gt"):
        raise ValueError("Input file must be a .gt file.")

    return args.input, args.input.replace(".gt", ".csv.gz")


def main():
    input_path, output_path = get_input_output_paths()
    g = gt.load_graph(input_path, fmt="gt")
    save_gt_graph_as_csv_gzip(g, output_path)
    print(f"Graph saved as gzipped CSV at {output_path}.")


if __name__ == "__main__":
    main()
