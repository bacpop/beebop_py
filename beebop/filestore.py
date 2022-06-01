import os
import json


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