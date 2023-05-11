import json


def read_schema(name: str) -> dict:
    """

    :param name: [name of schema]
    :return dict: [json schema deserialized to dict]
    """
    with open('spec/'+name+'.schema.json', 'r', encoding="utf-8") as file:
        schema_data = file.read()
    schema = json.loads(schema_data)
    return schema


class Schema:
    def __init__(self):
        self.version = read_schema("version")
        self.sketch = read_schema("sketch")
        self.sketches = read_schema("sketches")
        self.cluster = read_schema("cluster")
        self.project = read_schema("project")
        # add new schemas here
