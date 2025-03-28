import logging
from typing import OrderedDict

from more_itertools import batched

from API.EMInfraClient import EMInfraClient
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

        relation_types = {relation['typeURI'] for relation in relations.values()}
        relation_types_dict = {relation_type: f'r{i+1}' for i, relation_type in enumerate(relation_types)}

        asset_types = list({asset['typeURI'] for asset in used_assets.values()})
        asset_types_dict = {asset_type: f'{chr(i+97)}' for i, asset_type in enumerate(asset_types)}


        for relation in relations.values():
            relation_type = relation['typeURI']
            relation_id = relation_types_dict[relation_type]


            directed = (relation_type in directional_relations)

            source_id = relation['bronAssetId']['identificator']
            target_id = relation['doelAssetId']['identificator']



        pattern = []
        return pattern