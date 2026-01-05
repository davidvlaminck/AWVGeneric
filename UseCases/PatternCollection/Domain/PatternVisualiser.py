import logging

from more_itertools import batched

from API.eminfra.eminfra_client import EMInfraClient
from API.EMSONClient import EMSONClient
from API.Enums import AuthType, Environment
from UseCases.PatternCollection.Domain.AssetInfoCollector import AssetInfoCollector
from UseCases.PatternCollection.Domain.InfoObject import directional_relations


class PatternVisualiser:
    def __init__(self, auth_type: AuthType, env: Environment, **kwargs):
        self.em_infra_client = EMInfraClient(auth_type=auth_type, env=env, **kwargs)
        self.emson_importer = EMSONClient(auth_type=auth_type, env=env, **kwargs)
        self.collector = AssetInfoCollector(em_infra_client=self.em_infra_client, emson_client=self.emson_importer)


    @staticmethod
    def collect_info_given_asset_uuids(asset_info_collector: AssetInfoCollector, asset_uuids: list[str],
                                        batch_size: int = 10000, pattern: list[tuple[str, str, str | list[str]]] = None):
        # work in batches of <batch_size> asset_uuids
        for uuids in batched(asset_uuids, batch_size):
            logging.info('collecting asset info')
            asset_info_collector.start_collecting_from_starting_uuids_using_pattern(
                starting_uuids=uuids,
                pattern = pattern
            )
            
    @classmethod
    def generate_pattern_from_dicts(cls, dicts):
        # sort assets and relations
        assets = {}
        relations = {}
        for d in dicts:
            if 'bronAssetId' in d:
                relations[d['assetId']['identificator']] = d
            else:
                assets[d['assetId']['identificator']] = d

        # clean up relations
        for relation_key in list(relations.keys()):
            relation = relations[relation_key]
            source_id = relation['bronAssetId']['identificator']
            target_id = relation['doelAssetId']['identificator']
            if source_id not in assets or target_id not in assets:
                relations.pop(relation_key)

        # clean up assets
        used_assets = {}
        for relation in relations.values():
            source_id = relation['bronAssetId']['identificator']
            target_id = relation['doelAssetId']['identificator']
            used_assets[source_id] = assets[source_id]
            used_assets[target_id] = assets[target_id]

        relation_types = sorted({relation['typeURI'] for relation in relations.values()})
        relation_types_dict = {relation_type: f'r{i+1}' for i, relation_type in enumerate(relation_types)}

        asset_types = sorted({asset['typeURI'] for asset in used_assets.values()})
        asset_types_dict = {asset_type: f'{chr(i+97)}' for i, asset_type in enumerate(asset_types)}

        pattern = []

        for relation in relations.values():
            relation_type = relation['typeURI']
            relation_id = relation_types_dict[relation_type]
            directed = (relation_type in directional_relations)
            source_id = relation['bronAssetId']['identificator']
            target_id = relation['doelAssetId']['identificator']
            source_char = asset_types_dict[used_assets[source_id]['typeURI']]
            target_char = asset_types_dict[used_assets[target_id]['typeURI']]
            pattern_part = (source_char, f'-[{relation_id}]' + ('->' if directed else '-'), target_char)
            if not directed and source_char > target_char:
                pattern_part = (target_char, f'-[{relation_id}]' + ('->' if directed else '-'), source_char)
            if pattern_part not in pattern:
                pattern.append(pattern_part)

            source_used = False
            target_used = False
            for part in pattern:
                if part[1] != 'type_of':
                    continue
                if not source_used and part[0] == source_char:
                    source_used = True
                if not target_used and part[2] == target_char:
                    target_used = True
                if source_used and target_used:
                    break
            if not source_used:
                pattern_part = (source_char, 'type_of', [used_assets[source_id]['typeURI']])
                if pattern_part not in pattern:
                    pattern.append(pattern_part)
            if not target_used:
                pattern_part = (target_char, 'type_of', [used_assets[target_id]['typeURI']])
                if pattern_part not in pattern:
                    pattern.append(pattern_part)

        for relation_type, relation_char in  relation_types_dict.items():
            pattern.append((relation_char, 'type_of', [relation_type]))

        pattern.append(('uuids', 'of', 'a'))
        return pattern