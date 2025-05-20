import itertools
import re
from typing import Generator

from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from Exceptions.AssetsMissingError import AssetsMissingError
from Exceptions.ObjectAlreadyExistsError import ObjectAlreadyExistsError

from UseCases.PatternCollection.Domain.AssetCollection import AssetCollection
from UseCases.PatternCollection.Domain.Enums import Direction
from UseCases.PatternCollection.Domain.InfoObject import NodeInfoObject


class AssetInfoCollector:
    def __init__(self, em_infra_client: EMInfraClient, emson_client: EMSONClient):
        self.em_infra_importer = em_infra_client
        self.emson_importer = emson_client
        self.collection = AssetCollection()

    def get_assets_by_uuids(self, uuids: [str]) -> Generator[dict, None, None]:
        return self.emson_importer.get_assets_by_filter(filter={'uuid': uuids})

    def get_assetrelaties_by_uuids(self, uuids: [str]) -> Generator[dict, None, None]:
        return self.emson_importer.get_assetrelaties_by_filter(filter={'uuid': uuids})

    def get_assetrelaties_by_source_or_target_uuids(self, uuids: [str]) -> Generator[dict, None, None]:
        return self.em_infra_importer.get_objects_from_oslo_search_endpoint(
            url_part='assetrelaties', filter_dict={'asset': uuids})

    def collect_asset_info(self, uuids: [str]) -> None:
        for asset in self.get_assets_by_uuids(uuids=uuids):
            asset['uuid'] = asset.pop('@id')[39:75]
            asset['typeURI'] = asset.pop('@type')
            self.collection.add_node(asset)

    def _common_collect_relation_info(self, assetrelaties_generator: Generator[dict, None, None],
                                      ignore_duplicates: bool = False) -> None:
        asset_missing_error = AssetsMissingError(msg='')
        for relation in assetrelaties_generator:
            relation['uuid'] = relation.pop('@id')[46:82]
            relation['typeURI'] = relation.pop('@type')
            relation['bron'] = relation['RelatieObject.bron']['@id'][39:75]
            relation['doel'] = relation['RelatieObject.doel']['@id'][39:75]
            try:
                self.collection.add_relation(relation)
            except AssetsMissingError as e:
                asset_missing_error.uuids.extend(e.uuids)
                asset_missing_error.msg += e.msg
            except ObjectAlreadyExistsError as e:
                if not ignore_duplicates:
                    raise e
        if asset_missing_error.uuids:
            raise asset_missing_error

    def collect_relation_info(self, uuids: [str], ignore_duplicates: bool = False) -> None:
        self._common_collect_relation_info(self.get_assetrelaties_by_uuids(uuids=uuids),
                                           ignore_duplicates=ignore_duplicates)

    def collect_relation_info_by_sources_or_targets(self, uuids: [str], ignore_duplicates: bool = False) -> None:
        self._common_collect_relation_info(self.get_assetrelaties_by_source_or_target_uuids(uuids=uuids),
                                           ignore_duplicates=ignore_duplicates)

    def start_collecting_from_starting_uuids_using_pattern(self, starting_uuids: [str],
                                                           pattern: [tuple[str, str, object]]) -> None:
        uuid_pattern = next((t[2] for t in pattern if t[:2] == ('uuids', 'of')), None)
        type_of_patterns = [t for t in pattern if t[1] == 'type_of']
        relation_patterns = [t for t in pattern if re.match('^(<)?-\\[r(\\d)*]-(>)?$', t[1]) is not None]

        if uuid_pattern is None:
            raise ValueError('No uuids pattern found in pattern list. '
                             'Must contain one tuple with ("uuids", "of", object)')
        if not type_of_patterns:
            raise ValueError('No type_of pattern found in pattern list. '
                             'Must contain at least one tuple with (object, "type_of", object)')
        if not relation_patterns:
            raise ValueError('No relation pattern found in pattern list'
                             'Must contain at least one tuple with (object, "-[r]-", object) where r is followed by a '
                             'number and relation may or may not be directional (using < and > symbols)')

        self.collect_asset_info(uuids=starting_uuids)

        matching_objects = [uuid_pattern]
        while relation_patterns:
            new_matching_objects = []
            for obj in matching_objects:
                relation_patterns = self.order_patterns_for_object(obj, relation_patterns)

                for relation_pattern in relation_patterns:
                    if relation_pattern[0] != obj:
                        continue

                    new_matching_objects.append(relation_pattern[2])

                    type_of_obj_l = [t[2] for t in type_of_patterns if t[0] == relation_pattern[0]]
                    type_of_obj = []
                    for l in type_of_obj_l:
                        type_of_obj.extend(l)
                    if type_of_obj is None:
                        raise ValueError(f'No type_of pattern found for object {relation_pattern[0]}')

                    type_of_uuids = [asset.uuid for asset in self.collection.get_node_objects_by_types(type_of_obj)]
                    if not type_of_uuids:
                        continue
                    try:
                        self.collect_relation_info_by_sources_or_targets(uuids=type_of_uuids, ignore_duplicates=True)
                    except AssetsMissingError as e:
                        self.collect_asset_info(uuids=e.uuids)
                        self.collect_relation_info_by_sources_or_targets(uuids=type_of_uuids, ignore_duplicates=True)

                relation_patterns = [t for t in relation_patterns if t[0] != obj]
            matching_objects = new_matching_objects

    @classmethod
    def order_patterns_for_object(cls, obj: str, relation_patterns: [tuple[str, str, str]]) -> [tuple[str, str, str]]:
        ordered_patterns = []
        for relation_pattern in relation_patterns:
            if relation_pattern[2] == obj:
                ordered_patterns.append(AssetInfoCollector.reverse_relation_pattern(relation_pattern))
            else:
                ordered_patterns.append(relation_pattern)
        return ordered_patterns


    @classmethod
    def reverse_relation_pattern(cls, relation_pattern: tuple[str, str, str]) -> tuple[str, str, str]:
        rel_str = relation_pattern[1]
        parts = re.match('(<?-)\\[(r.+)](->?)', rel_str).groups()
        parts_2 = parts[0].replace('<', '>')[::-1]
        parts_0 = parts[2].replace('>', '<')[::-1]

        return relation_pattern[2], f'{parts_0}[{parts[1]}]{parts_2}', relation_pattern[0]

    def get_level_dict(self, pattern: [tuple[str, str, object]]) -> dict[str, str]:
        level_statements = [s for s in pattern if s[1] == 'level']
        level_dict = {}
        for level_statement in level_statements:
            for assettype in next(t[2] for t in pattern if t[0] == level_statement[0] and t[1] == 'type_of'):
                if assettype in level_dict:
                    raise ValueError(f'Level for {assettype} is already defined in the pattern')

                level_dict[assettype] = level_statement[2]
        return level_dict

    def filter_collection_by_pattern(self, filter_pattern: [tuple[str, str, object]], starting_uuids: [str]
                                     ) -> [NodeInfoObject]:
        uuid_pattern = next((t[2] for t in filter_pattern if t[:2] == ('uuids', 'of')), None)
        type_of_patterns = [t for t in filter_pattern if t[1] == 'type_of']
        relation_patterns = [t for t in filter_pattern if re.match('^(<)?-\\[r(\\d)*]-(>)?$', t[1]) is not None]
        node_info_objects = []

        if uuid_pattern is None:
            raise ValueError('No uuids pattern found in pattern list. '
                             'Must contain one tuple with ("uuids", "of", object)')
        if not type_of_patterns:
            raise ValueError('No type_of pattern found in pattern list. '
                             'Must contain at least one tuple with (object, "type_of", object)')
        if not relation_patterns:
            raise ValueError('No relation pattern found in pattern list'
                             'Must contain at least one tuple with (object, "-[r]-", object) where r is followed by a '
                             'number and relation may or may not be directional (using < and > symbols)')
        types = next(pattern[2] for pattern in type_of_patterns if pattern[0] == uuid_pattern)

        node_info_objects.extend([x for x in self.collection.get_node_objects_by_types(types)
                                 if x.uuid in starting_uuids])

        matching_objects = [uuid_pattern]
        while relation_patterns:
            new_matching_objects = []
            for obj in matching_objects:
                relation_patterns = self.order_patterns_for_object(obj, relation_patterns)

                for relation_pattern in relation_patterns:
                    if relation_pattern[0] != obj:
                        continue

                    new_matching_objects.append(relation_pattern[2])

                    type_of_obj = list(itertools.chain.from_iterable(
                        t[2] for t in type_of_patterns if t[0] == relation_pattern[0]
                    ))
                    if type_of_obj is None:
                        raise ValueError(f'No type_of pattern found for object {relation_pattern[0]}')

                    type_of_uuids = [asset.uuid for asset in node_info_objects if asset.short_type in type_of_obj]
                    if not type_of_uuids:
                        continue
                    objects = []

                    # relation pattern preparation
                    if relation_pattern[1].startswith('<'):
                        allowed_directions = [Direction.REVERSED]
                    elif relation_pattern[1].endswith('>'):
                        allowed_directions = [Direction.WITH]
                    else:
                        allowed_directions = [Direction.NONE]
                    rel_type_code = relation_pattern[1].split('[')[1].split(']')[0]
                    type_of_rel = [r.split('#')[1] for l in
                                   [t[2] for t in type_of_patterns if t[0] == rel_type_code]
                                   for r in l]

                    target_code = relation_pattern[2]
                    type_of_target = list(itertools.chain.from_iterable(
                        t[2] for t in type_of_patterns if t[0] == target_code
                    ))

                    for asset_uuid in type_of_uuids:
                        objects.extend(list(
                            self.collection.traverse_graph(
                                start_uuid=asset_uuid, relation_types=type_of_rel,
                                allowed_directions=allowed_directions, filtered_node_types=type_of_target,
                                return_type='info_object', return_relation_info=True)))
                    node_info_objects.extend(objects)

                relation_patterns = [t for t in relation_patterns if t[0] != obj]
            matching_objects = new_matching_objects

        return node_info_objects