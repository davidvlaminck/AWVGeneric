import json

from API.eminfra.EMInfraDomain import Graph, AssetDTO


class GraphService:
    def __init__(self, requester):
        self.requester = requester

        self.DEFAULT_GRAPH_RELATIE_TYPES = [
            "3ff9bf1c-d852-442e-a044-6200fe064b20",
            "e801b062-74e1-4b39-9401-163dd91d5494",
            "afbe8124-a9e2-41b9-a944-c14a41a9f4d5",
            "f0ed1efa-fe29-4861-89dc-5d3bc40f0894",
            "de86510a-d61c-46fb-805d-c04c78b27ab6",
            "6c91fe94-8e29-4906-a02c-b8507495ad21",
            "cd5104b3-5e98-4055-8af2-5724bf141e44",
            "e7d8e795-06ef-4e0f-b049-c736b54447c9",
            "34d043f5-583d-4c1e-9f99-4d89fcb84ef4",
            "3a63adb8-493a-4aa8-8e2e-164fd942b0b9",
            "0da67bde-0152-445f-8f29-6a9319f890fd",
            "812dd4f3-c34e-43d1-88f1-3bcd0b1e89c2",
            "fef0df58-8243-4869-a056-a71346bf6acd",
            "dcc18707-2ca1-4b35-bfff-9fa262da96dd",
            "41c7e2eb-17be-4f53-a49e-0f3bc31efdd0",
            "20b29934-fd5e-490f-a94b-e566513be407",
            "1aa9795c-7ed0-4d96-87b9-e51159055755",
            "321c18b8-92ca-4188-a28a-f00cdfaa0e31",
            "e2c644ec-7fbd-48ff-906a-4747b43b11a5",
            "b4e89ae7-cb69-449c-946b-fdff13f63a7a",
            "93c88f93-6e8c-4af3-a723-7e7a6d6956ac",
            "f2c5c4a1-0899-4053-b3b3-2d662c717b44",
            "a6747802-7679-473f-b2bd-db2cfd1b88d7",
        ]

    def get_graph_by_uuid(self, asset_uuid: str, depth: int = 1, relatietypes: list = None, actief: bool = True) -> Graph:
        """
        Generate the graph, starting from an asset, searching a certain depth and for some relatieTypes

        :param asset_uuid: central asset (node) to start the search from.
        :param depth: depth of the Graph. default depth of 1 step
        :param relatietypes: List of relatietypes. Default None returns all possible relatietypes
        :param actief: Returns only active assets (nodes)
        :return:
        """
        relatietypes = relatietypes or self.DEFAULT_GRAPH_RELATIE_TYPES

        request_body = {
            "limit": 1000,
            "uuidsToInclude": [asset_uuid],
            "uuidsToExpand": [asset_uuid],
            "expandDepth": depth,
            "relatieTypesToReturn": relatietypes,
            "relatieTypesToExpand": relatietypes,
            "actiefFilter": actief
        }
        uri = 'core/api/assets/graph'
        response = self.requester.post(
            url=uri
            , data=json.dumps(request_body)
        )
        if response.status_code != 201:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return Graph.from_dict(response.json())

    def get_graph(self, asset: AssetDTO, depth: int = 1, relatietypes: list = None, actief: bool = True) -> Graph:
        """
        Generate the graph, starting from an asset, searching a certain depth and for some relatieTypes

        :param asset: central asset (node) to start the search from.
        :param depth: depth of the Graph. default depth of 1 step
        :param relatietypes: List of relatietypes. Default None returns all possible relatietypes
        :param actief: Returns only active assets (nodes)
        :return:
        """
        return self.get_graph_by_uuid(asset_uuid=asset.uuid, depth=depth, relatietypes=relatietypes, actief=actief)
