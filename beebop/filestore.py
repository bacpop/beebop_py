import os
import json
from pathlib import PurePath


class FileStore:
    def __init__(self, path):
        self._path = path
        os.makedirs(path, exist_ok=True)

    def filename(self, hash):
        return os.path.join(self._path, hash + '.json')

    def get(self, hash):
        src = self.filename(hash)
        if not os.path.exists(src):
            raise Exception(f"Sketch for hash '{hash}' not found in storage")
        else:
            with open(src, 'r') as fp:
                sketch = json.load(fp)
        return sketch

    def exists(self, hash):
        return os.path.exists(self.filename(hash))

    def put(self, hash, sketch):
        dst = self.filename(hash)
        if not os.path.exists(dst):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            with open(dst, 'w') as fp:
                json.dump(sketch, fp)


class PoppunkFileStore:
    def __init__(self, storage_location):
        self.storage_location = storage_location
        self.input = FileStore(storage_location + '/json')
        self.output_base = PurePath(storage_location, 'poppunk_output')
        os.makedirs(self.output_base, exist_ok=True)

    def output(self, p_hash):
        return str(PurePath(self.output_base, p_hash))

    def output_microreact(self, p_hash, cluster):
        return str(PurePath(self.output(p_hash), "microreact_" + str(cluster)))

    def output_network(self, p_hash):
        return str(PurePath(self.output(p_hash), "network"))

    def include_files(self, p_hash, cluster):
        return str(PurePath(self.output(p_hash),
                            "include" + str(cluster) + ".txt"))

    def network_file(self, p_hash):
        return str(PurePath(self.output(p_hash), p_hash + "_graph.gt"))

    def previous_query_clustering(self, p_hash):
        return str(PurePath(self.output(p_hash), p_hash + "_clusters.csv"))

    def distances(self, p_hash):
        return str(PurePath(self.output(p_hash), p_hash).with_suffix(".dists"))

    def microreact_csv(self, p_hash, cluster):
        return str(PurePath(self.output(p_hash),
                            "microreact_" + str(cluster),
                            "microreact_" + cluster +
                            "_microreact_clusters.csv"))

    def microreact_dot(self, p_hash, cluster):
        return str(PurePath(self.output(p_hash),
                            "microreact_" + str(cluster),
                            "microreact_" + cluster +
                            "_perplexity20.0_accessory_tsne.dot"))

    def microreact_nwk(self, p_hash, cluster):
        return str(PurePath(self.output(p_hash),
                            "microreact_" + str(cluster),
                            "microreact_" + cluster + "_core_NJ.nwk"))


class DatabaseFileStore:
    def __init__(self, full_path):
        self.db = full_path
        self.path = str(PurePath(full_path).parent)
        self.name = str(PurePath(full_path).stem)
        self.distances = str(PurePath(self.db,
                                      self.name).with_suffix('.dists.pkl'))
        self.previous_clustering = str(PurePath(self.db,
                                                self.name + '_clusters.csv'))
