from unittest.mock import Mock

from API.AbstractRequester import AbstractRequester
from UseCases.PatternCollection.Domain.AssetInfoCollector import AssetInfoCollector
from ClientFixtures import fake_eminfra_client, fake_emson_client


def test_asset_info_collector(fake_eminfra_client, fake_emson_client):
    collector = AssetInfoCollector(em_infra_client=fake_eminfra_client, emson_client=fake_emson_client)

    collector.collect_asset_info(uuids=['00000000-0000-0000-0000-000000000001'])
    asset_node = collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000001')
    assert asset_node.uuid == '00000000-0000-0000-0000-000000000001'


def test_asset_info_collector_inactive(fake_eminfra_client, fake_emson_client):
    collector = AssetInfoCollector(em_infra_client=fake_eminfra_client, emson_client=fake_emson_client)

    collector.collect_asset_info(uuids=['00000000-0000-0000-0000-000000000010'])
    asset_node = collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000010')
    assert asset_node.uuid == '00000000-0000-0000-0000-000000000010'
    assert asset_node.active is False


def test_small_pattern_get_level_dict_and_filter_collection(fake_eminfra_client, fake_emson_client):
    collector = AssetInfoCollector(em_infra_client=fake_eminfra_client, emson_client=fake_emson_client)
    pattern = [
        ('uuids', 'of', 'a'),
        ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
        ('a', '-[r1]-', 'b'),
        ('a', '-[r1]-', 'c'),
        ('b', 'type_of', ['onderdeel#WVLichtmast', 'onderdeel#WVConsole']),
        ('c', 'type_of', ['onderdeel#Armatuurcontroller']),
        ('r1', 'type_of', ['onderdeel#Bevestiging']),
        ('a', 'level', 0),
        ('b', 'level', -1),
        ('c', 'level', 1)]
    filter_pattern = [
        ('uuids', 'of', 'a'),
        ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
        ('a', '-[r1]-', 'b'),
        ('b', 'type_of', ['onderdeel#Armatuurcontroller']),
        ('r1', 'type_of', ['onderdeel#Bevestiging']),
        ('a', 'level', 0),
        ('b', 'level', 1)]

    collector.start_collecting_from_starting_uuids_using_pattern(
        starting_uuids=['00000000-0000-0000-0000-000000000002'], pattern=pattern)

    assert collector.collection.short_uri_dict == {
        'onderdeel#Armatuurcontroller': {'00000000-0000-0000-0000-000000000006',
                                         '00000000-0000-0000-0000-000000000026'},
        'onderdeel#Bevestiging': {'000000000002-Bevestigin-000000000004',
                                  '000000000002-Bevestigin-000000000026',
                                  '000000000006-Bevestigin-000000000002'},
        'onderdeel#VerlichtingstoestelLED': {'00000000-0000-0000-0000-000000000002'},
        'onderdeel#WVLichtmast': {'00000000-0000-0000-0000-000000000004'}}

    assert collector.get_level_dict(pattern) == {
        'onderdeel#Armatuurcontroller': 1,
        'onderdeel#VerlichtingstoestelLED': 0,
        'onderdeel#WVLichtmast': -1,
        'onderdeel#WVConsole': -1
    }

    filtered_list = collector.filter_collection_by_pattern(
        starting_uuids=['00000000-0000-0000-0000-000000000002'], filter_pattern=filter_pattern)

    assert [x['@id'] for x in filtered_list] == [
        '00000000-0000-0000-0000-000000000002',
        '00000000-0000-0000-0000-000000000006',
        '00000000-0000-0000-0000-000000000026',
        '00000000-0000-0000-0000-000000000002-Bevestigin-00000000-0000-0000-0000-000000000026',
        '00000000-0000-0000-0000-000000000006-Bevestigin-00000000-0000-0000-0000-000000000002'
    ]


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_a(fake_eminfra_client, fake_emson_client):
    collector = AssetInfoCollector(em_infra_client=fake_eminfra_client, emson_client=fake_emson_client)

    collector.start_collecting_from_starting_uuids_using_pattern(
        starting_uuids=['00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003',
                        '00000000-0000-0000-0000-000000000025'],
        pattern=[('uuids', 'of', 'a'),
                 ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
                 ('a', '-[r1]-', 'b'),
                 ('b', 'type_of', ['onderdeel#WVLichtmast', 'onderdeel#WVConsole', 'onderdeel#Armatuurcontroller']),
                 ('b', '-[r2]->', 'c'),
                 ('a', '-[r2]->', 'c'),
                 ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole',
                                   'lgc:installatie#VPBevestig']),
                 ('r1', 'type_of', ['onderdeel#Bevestiging']),
                 ('r2', 'type_of', ['onderdeel#HoortBij'])])

    assert collector.collection.short_uri_dict == {
        'lgc:installatie#VPBevestig': {'00000000-0000-0000-0000-000000000021'},
        'lgc:installatie#VPConsole': {'00000000-0000-0000-0000-000000000009'},
        'lgc:installatie#VPLMast': {'00000000-0000-0000-0000-000000000008'},
        'onderdeel#Armatuurcontroller': {'00000000-0000-0000-0000-000000000006',
                                         '00000000-0000-0000-0000-000000000007',
                                         '00000000-0000-0000-0000-000000000026'},
        'onderdeel#Bevestiging': {'000000000002-Bevestigin-000000000004',
                                  '000000000002-Bevestigin-000000000026',
                                  '000000000003-Bevestigin-000000000007',
                                  '000000000005-Bevestigin-000000000003',
                                  '000000000006-Bevestigin-000000000002',
                                  '000000000022-Bevestigin-000000000004',
                                  '000000000023-Bevestigin-000000000004',
                                  '000000000024-Bevestigin-000000000004'},
        'onderdeel#HoortBij': {'000000000004--HoortBij--000000000008',
                               '000000000005--HoortBij--000000000009',
                               '000000000025--HoortBij--000000000021'},
        'onderdeel#VerlichtingstoestelLED': {'00000000-0000-0000-0000-000000000002',
                                             '00000000-0000-0000-0000-000000000003',
                                             '00000000-0000-0000-0000-000000000022',
                                             '00000000-0000-0000-0000-000000000023',
                                             '00000000-0000-0000-0000-000000000024',
                                             '00000000-0000-0000-0000-000000000025'},
        'onderdeel#WVConsole': {'00000000-0000-0000-0000-000000000005'},
        'onderdeel#WVLichtmast': {'00000000-0000-0000-0000-000000000004'}}


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_c(fake_eminfra_client, fake_emson_client):
    collector = AssetInfoCollector(em_infra_client=fake_eminfra_client, emson_client=fake_emson_client)

    collector.start_collecting_from_starting_uuids_using_pattern(
        starting_uuids=['00000000-0000-0000-0000-000000000008', '00000000-0000-0000-0000-000000000009'],
        pattern=[('uuids', 'of', 'c'),
                 ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
                 ('a', '-[r1]-', 'b'),
                 ('b', 'type_of', ['onderdeel#WVLichtmast', 'onderdeel#WVConsole']),
                 ('a', '-[r1]-', 'd'),
                 ('d', 'type_of', ['onderdeel#Armatuurcontroller']),
                 ('b', '-[r2]->', 'c'),
                 ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole']),
                 ('r1', 'type_of', ['onderdeel#Bevestiging']),
                 ('r2', 'type_of', ['onderdeel#HoortBij'])])

    assert collector.collection.short_uri_dict == {
        'lgc:installatie#VPConsole': {'00000000-0000-0000-0000-000000000009'},
        'lgc:installatie#VPLMast': {'00000000-0000-0000-0000-000000000008'},
        'onderdeel#Armatuurcontroller': {'00000000-0000-0000-0000-000000000006',
                                         '00000000-0000-0000-0000-000000000007',
                                         '00000000-0000-0000-0000-000000000026'},
        'onderdeel#Bevestiging': {'000000000002-Bevestigin-000000000004',
                                  '000000000002-Bevestigin-000000000026',
                                  '000000000003-Bevestigin-000000000007',
                                  '000000000005-Bevestigin-000000000003',
                                  '000000000006-Bevestigin-000000000002',
                                  '000000000022-Bevestigin-000000000004',
                                  '000000000023-Bevestigin-000000000004',
                                  '000000000024-Bevestigin-000000000004'},
        'onderdeel#HoortBij': {'000000000004--HoortBij--000000000008',
                               '000000000005--HoortBij--000000000009'},
        'onderdeel#VerlichtingstoestelLED': {'00000000-0000-0000-0000-000000000002',
                                             '00000000-0000-0000-0000-000000000003',
                                             '00000000-0000-0000-0000-000000000022',
                                             '00000000-0000-0000-0000-000000000023',
                                             '00000000-0000-0000-0000-000000000024'},
        'onderdeel#WVConsole': {'00000000-0000-0000-0000-000000000005'},
        'onderdeel#WVLichtmast': {'00000000-0000-0000-0000-000000000004'}}


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_d_multiple_types(fake_eminfra_client,
                                                                                             fake_emson_client):
    collector = AssetInfoCollector(em_infra_client=fake_eminfra_client, emson_client=fake_emson_client)

    collector.start_collecting_from_starting_uuids_using_pattern(
        starting_uuids=['00000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000007'],
        pattern=[('uuids', 'of', 'd'),
                 ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
                 ('a', '-[r1]-', 'b'),
                 ('b', 'type_of', ['onderdeel#WVLichtmast']),
                 ('b', 'type_of', ['onderdeel#WVConsole']),
                 ('a', '-[r1]-', 'd'),
                 ('d', 'type_of', ['onderdeel#Armatuurcontroller']),
                 ('b', '-[r2]->', 'c'),
                 ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole']),
                 ('r1', 'type_of', ['onderdeel#Bevestiging']),
                 ('r2', 'type_of', ['onderdeel#HoortBij'])])

    assert collector.collection.short_uri_dict == {
        'lgc:installatie#VPConsole': {'00000000-0000-0000-0000-000000000009'},
        'lgc:installatie#VPLMast': {'00000000-0000-0000-0000-000000000008'},
        'onderdeel#Armatuurcontroller': {'00000000-0000-0000-0000-000000000006',
                                         '00000000-0000-0000-0000-000000000007',
                                         '00000000-0000-0000-0000-000000000026'},
        'onderdeel#Bevestiging': {'000000000002-Bevestigin-000000000004',
                                  '000000000002-Bevestigin-000000000026',
                                  '000000000003-Bevestigin-000000000007',
                                  '000000000005-Bevestigin-000000000003',
                                  '000000000006-Bevestigin-000000000002',
                                  '000000000022-Bevestigin-000000000004',
                                  '000000000023-Bevestigin-000000000004',
                                  '000000000024-Bevestigin-000000000004'},
        'onderdeel#HoortBij': {'000000000004--HoortBij--000000000008',
                               '000000000005--HoortBij--000000000009'},
        'onderdeel#VerlichtingstoestelLED': {'00000000-0000-0000-0000-000000000002',
                                             '00000000-0000-0000-0000-000000000003',
                                             '00000000-0000-0000-0000-000000000022',
                                             '00000000-0000-0000-0000-000000000023',
                                             '00000000-0000-0000-0000-000000000024'},
        'onderdeel#WVConsole': {'00000000-0000-0000-0000-000000000005'},
        'onderdeel#WVLichtmast': {'00000000-0000-0000-0000-000000000004'}}


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_d(fake_eminfra_client, fake_emson_client):
    collector = AssetInfoCollector(em_infra_client=fake_eminfra_client, emson_client=fake_emson_client)

    collector.start_collecting_from_starting_uuids_using_pattern(
        starting_uuids=['00000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000007'],
        pattern=[('uuids', 'of', 'd'),
                 ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
                 ('a', '-[r1]-', 'b'),
                 ('b', 'type_of', ['onderdeel#WVLichtmast', 'onderdeel#WVConsole']),
                 ('a', '-[r1]-', 'd'),
                 ('d', 'type_of', ['onderdeel#Armatuurcontroller']),
                 ('b', '-[r2]->', 'c'),
                 ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole']),
                 ('r1', 'type_of', ['onderdeel#Bevestiging']),
                 ('r2', 'type_of', ['onderdeel#HoortBij'])])

    assert collector.collection.short_uri_dict == {
        'lgc:installatie#VPConsole': {'00000000-0000-0000-0000-000000000009'},
        'lgc:installatie#VPLMast': {'00000000-0000-0000-0000-000000000008'},
        'onderdeel#Armatuurcontroller': {'00000000-0000-0000-0000-000000000006',
                                         '00000000-0000-0000-0000-000000000007',
                                         '00000000-0000-0000-0000-000000000026'},
        'onderdeel#Bevestiging': {'000000000002-Bevestigin-000000000004',
                                  '000000000002-Bevestigin-000000000026',
                                  '000000000003-Bevestigin-000000000007',
                                  '000000000005-Bevestigin-000000000003',
                                  '000000000006-Bevestigin-000000000002',
                                  '000000000022-Bevestigin-000000000004',
                                  '000000000023-Bevestigin-000000000004',
                                  '000000000024-Bevestigin-000000000004'},
        'onderdeel#HoortBij': {'000000000004--HoortBij--000000000008',
                               '000000000005--HoortBij--000000000009'},
        'onderdeel#VerlichtingstoestelLED': {'00000000-0000-0000-0000-000000000002',
                                             '00000000-0000-0000-0000-000000000003',
                                             '00000000-0000-0000-0000-000000000022',
                                             '00000000-0000-0000-0000-000000000023',
                                             '00000000-0000-0000-0000-000000000024'},
        'onderdeel#WVConsole': {'00000000-0000-0000-0000-000000000005'},
        'onderdeel#WVLichtmast': {'00000000-0000-0000-0000-000000000004'}}


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_b(fake_eminfra_client, fake_emson_client):
    collector = AssetInfoCollector(em_infra_client=fake_eminfra_client, emson_client=fake_emson_client)

    collector.start_collecting_from_starting_uuids_using_pattern(
        starting_uuids=['00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000005'],
        pattern=[('uuids', 'of', 'b'),
                 ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
                 ('a', '-[r1]-', 'b'),
                 ('b', 'type_of', ['onderdeel#WVLichtmast', 'onderdeel#WVConsole']),
                 ('a', '-[r1]-', 'd'),
                 ('d', 'type_of', ['onderdeel#Armatuurcontroller']),
                 ('b', '-[r2]->', 'c'),
                 ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole']),
                 ('r1', 'type_of', ['onderdeel#Bevestiging']),
                 ('r2', 'type_of', ['onderdeel#HoortBij'])])

    assert collector.collection.short_uri_dict == {
        'lgc:installatie#VPConsole': {'00000000-0000-0000-0000-000000000009'},
        'lgc:installatie#VPLMast': {'00000000-0000-0000-0000-000000000008'},
        'onderdeel#Armatuurcontroller': {'00000000-0000-0000-0000-000000000006',
                                         '00000000-0000-0000-0000-000000000007',
                                         '00000000-0000-0000-0000-000000000026'},
        'onderdeel#Bevestiging': {'000000000002-Bevestigin-000000000004',
                                  '000000000002-Bevestigin-000000000026',
                                  '000000000003-Bevestigin-000000000007',
                                  '000000000005-Bevestigin-000000000003',
                                  '000000000006-Bevestigin-000000000002',
                                  '000000000022-Bevestigin-000000000004',
                                  '000000000023-Bevestigin-000000000004',
                                  '000000000024-Bevestigin-000000000004'},
        'onderdeel#HoortBij': {'000000000004--HoortBij--000000000008',
                               '000000000005--HoortBij--000000000009'},
        'onderdeel#VerlichtingstoestelLED': {'00000000-0000-0000-0000-000000000002',
                                             '00000000-0000-0000-0000-000000000003',
                                             '00000000-0000-0000-0000-000000000022',
                                             '00000000-0000-0000-0000-000000000023',
                                             '00000000-0000-0000-0000-000000000024'},
        'onderdeel#WVConsole': {'00000000-0000-0000-0000-000000000005'},
        'onderdeel#WVLichtmast': {'00000000-0000-0000-0000-000000000004'}}


def test_reverse_relation_pattern():
    reversed1 = AssetInfoCollector.reverse_relation_pattern(('a', '-[r1]-', 'b'))
    assert reversed1 == ('b', '-[r1]-', 'a')

    reversed2 = AssetInfoCollector.reverse_relation_pattern(('a', '-[r1]->', 'b'))
    assert reversed2 == ('b', '<-[r1]-', 'a')

    reversed3 = AssetInfoCollector.reverse_relation_pattern(('a', '<-[r1]->', 'b'))
    assert reversed3 == ('b', '<-[r1]->', 'a')

    reversed4 = AssetInfoCollector.reverse_relation_pattern(('a', '<-[r1]-', 'b'))
    assert reversed4 == ('b', '-[r1]->', 'a')
