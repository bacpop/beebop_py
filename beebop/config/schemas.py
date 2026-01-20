import json


def read_schema(name: str) -> dict:
    """

    :param name: [name of schema]
    :return dict: [json schema deserialized to dict]
    """
    with open("spec/" + name + ".schema.json", "r", encoding="utf-8") as file:
        schema_data = file.read()
    schema = json.loads(schema_data)
    return schema


class Schema:
    def __init__(self):
        self.version = read_schema("version")
        self.sketch = read_schema("sketch")
        self.run_poppunk = read_schema("runPoppunk")
        self.cluster = read_schema("cluster")
        self.project = read_schema("project")
        self.species_config = read_schema("species_config")
        self.location_metadata = read_schema("location_metadata")
        self.status = read_schema("status")
        # add new schemas here
