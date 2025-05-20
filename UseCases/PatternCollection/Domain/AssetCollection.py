from typing import Generator

from Exceptions.AssetsMissingError import AssetsMissingError
from Exceptions.ObjectAlreadyExistsError import ObjectAlreadyExistsError
from UseCases.PatternCollection.Domain.Enums import Direction
from UseCases.PatternCollection.Domain.InfoObject import InfoObject, full_uri_to_short_type, NodeInfoObject, \
    RelationInfoObject, is_directional_relation


class AssetCollection:
    def __init__(self):
        self.object_dict: dict[str: InfoObject] = {}
        self.short_uri_dict = {}

    def _update_short_uri_dict(self, short_uri: str, uuid: str) -> None:
        if short_uri not in self.short_uri_dict:
            self.short_uri_dict[short_uri] = {uuid}
        else:
            self.short_uri_dict[short_uri].add(uuid)

    def add_node(self, d: dict) -> None:
        uuid = d['uuid']

        try:
            self.check_if_exists(uuid)
        except ObjectAlreadyExistsError:
            return

        short_uri = full_uri_to_short_type(d['typeURI'])

        info_object = NodeInfoObject(uuid=uuid, short_type=short_uri, attr_dict=d)
        actief = d.get("AIMDBStatus.isActief", True)
        info_object.active = actief

        self.object_dict[d['uuid']] = info_object

        self._update_short_uri_dict(short_uri=short_uri, uuid=uuid)

    def add_relation(self, d: dict) -> None:
        uuid = d['uuid']
        self.check_if_exists(uuid)

        asset_missing_error = AssetsMissingError(msg='')
        bron_object = self.object_dict.get(d['bron'])
        if bron_object is None:
            asset_missing_error.uuids.append(d['bron'])
            asset_missing_error.msg += f"Bron object with uuid {d['bron']} does not exist in collection.\n"

        doel_object = self.object_dict.get(d['doel'])
        if doel_object is None:
            asset_missing_error.uuids.append(d['doel'])
            asset_missing_error.msg += f"Doel object with uuid {d['doel']} does not exist in collection.\n"

        if asset_missing_error.uuids:
            raise asset_missing_error

        short_type_relation = full_uri_to_short_type(d['typeURI'])
        relation_name = short_type_relation.split('#')[-1]
        relation_info_object = RelationInfoObject(uuid=uuid, short_type=short_type_relation, attr_dict=d,
                                                  bron=bron_object, doel=doel_object)

        if is_directional_relation(short_type_relation):
            direction_1 = Direction.WITH
            direction_2 = Direction.REVERSED
        else:
            direction_1 = Direction.NONE
            direction_2 = Direction.NONE

        if relation_name not in bron_object.relations:
            bron_object.relations[relation_name] = {}
        bron_object.relations[relation_name][doel_object.uuid] = {
            'direction': direction_1,
            'relation_object': relation_info_object,
            'node_object': doel_object
        }

        if relation_name not in doel_object.relations:
            doel_object.relations[relation_name] = {}
        doel_object.relations[relation_name][bron_object.uuid] = {
            'direction': direction_2,
            'relation_object': relation_info_object,
            'node_object': bron_object
        }

        actief = d.get("AIMDBStatus.isActief")
        if actief is not None:
            relation_info_object.active = actief

        self.object_dict[d['uuid']] = relation_info_object
        self._update_short_uri_dict(short_uri=short_type_relation, uuid=uuid)

    def get_object_by_uuid(self, uuid: str) -> InfoObject:
        o = self.object_dict.get(uuid)
        if o is None:
            raise AssetsMissingError(f"Object with uuid {uuid} does not exist within the collection.")
        return o

    def get_node_object_by_uuid(self, uuid: str) -> NodeInfoObject:
        o = self.object_dict.get(uuid)
        if o is None:
            raise AssetsMissingError(f"Object with uuid {uuid} does not exist within the collection.")
        if o.is_relation:
            raise ValueError(f"Object with uuid {uuid} is a relation, not a node.")
        return o

    def get_relation_object_by_uuid(self, uuid: str) -> RelationInfoObject:
        o = self.object_dict.get(uuid)
        if o is None:
            raise AssetsMissingError(f"Object with uuid {uuid} does not exist within the collection.")
        if not o.is_relation:
            raise ValueError(f"Object with uuid {uuid} is a node, not a relation.")
        return o

    def get_attribute_dict_by_uuid(self, uuid: str) -> dict | None:
        o = self.get_object_by_uuid(uuid)
        return None if o is None else o.attr_dict

    def get_node_objects(self) -> Generator[NodeInfoObject, None, None]:
        yield from [node for node in self.object_dict.values() if not node.is_relation]

    def get_node_objects_by_types(self, list_of_short_types: [str]) -> Generator[NodeInfoObject, None, None]:
        for short_type in list_of_short_types:
            for uuid in self.short_uri_dict.get(short_type, set()):
                yield self.get_node_object_by_uuid(uuid)

    def get_relation_objects(self) -> Generator[RelationInfoObject, None, None]:
        yield from [node for node in self.object_dict.values() if node.is_relation]

    def get_relation_objects_by_types(self, list_of_short_types: [str]) -> Generator[RelationInfoObject, None, None]:
        for short_type in list_of_short_types:
            for uuid in self.short_uri_dict.get(short_type, set()):
                yield self.get_relation_object_by_uuid(uuid)

    def check_if_exists(self, uuid: str):
        existing = self.object_dict.get(uuid)
        if existing is not None:
            raise ObjectAlreadyExistsError(f"Node with uuid {uuid} already exists in collection.")

    def traverse_graph(self, start_uuid: str, relation_types: [str] = None, allowed_directions: [Direction] = None,
                       filtered_node_types: [str] = None, return_type: str = 'uuid', return_only_active: bool = True,
                       return_relation_info: bool = False
                       ) -> Generator[str | NodeInfoObject, None, None]:
        starting_object = self.get_node_object_by_uuid(start_uuid)
        if starting_object is None:
            raise ValueError(f"Node with uuid {start_uuid} does not exist in collection.")

        if relation_types is None or len(relation_types) == 0:
            relation_types = list(starting_object.relations.keys())

        if allowed_directions is None or len(allowed_directions) == 0:
            allowed_directions = [Direction.WITH, Direction.REVERSED, Direction.NONE]

        if filtered_node_types is None or len(filtered_node_types) == 0:
            filtered_node_types = list(self.short_uri_dict.keys())

        if return_type not in ['uuid', 'info_object']:
            raise ValueError(f"return_type {return_type} is not supported.")

        for relation_type in relation_types:
            if relation_type not in starting_object.relations:
                continue
            for target_uuid, relation_info in starting_object.relations[relation_type].items():
                if (relation_info['direction'] in allowed_directions and
                        relation_info['node_object'].short_type in filtered_node_types):
                    if return_only_active and (not relation_info['node_object'].active or
                                               not relation_info['relation_object'].active):
                        continue
                    if return_type == 'uuid':
                        yield target_uuid
                        if return_relation_info:
                            yield relation_info['relation_object'].uuid
                    else:
                        yield relation_info['node_object']
                        if return_relation_info:
                            yield relation_info['relation_object']
