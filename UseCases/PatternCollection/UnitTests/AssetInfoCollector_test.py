from unittest.mock import Mock

from API.AbstractRequester import AbstractRequester
from UseCases.PatternCollection.Domain.AssetInfoCollector import AssetInfoCollector
from ClientFixtures import fake_eminfra_client


def test_asset_info_collector(fake_eminfra_client):
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(em_infra_client=Mock(), emson_client=Mock())
    collector.em_infra_importer = fake_eminfra_client

    collector.collect_asset_info(uuids=['00000000-0000-0000-0000-000000000001'])
    asset_node = collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000001')
    assert asset_node.uuid == '00000000-0000-0000-0000-000000000001'


def test_asset_info_collector_inactive():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(em_infra_client=Mock(), emson_client=Mock())
    collector.em_infra_importer = fake_em_infra_importer

    collector.collect_asset_info(uuids=['00000000-0000-0000-0000-000000000010'])
    asset_node = collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000010')
    assert asset_node.uuid == '00000000-0000-0000-0000-000000000010'
    assert asset_node.active is False


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_a():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(em_infra_client=Mock(), emson_client=Mock())
    collector.em_infra_importer = fake_em_infra_importer

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

    assert collector.collection.short_uri_dict == {'lgc:installatie#VPBevestig': {'00000000-0000-0000-0000-000000000021'},
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


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_c():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(em_infra_client=Mock(), emson_client=Mock())
    collector.em_infra_importer = fake_em_infra_importer

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


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_d_multiple_types():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(em_infra_client=Mock(), emson_client=Mock())
    collector.em_infra_importer = fake_em_infra_importer

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


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_d():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(em_infra_client=Mock(), emson_client=Mock())
    collector.em_infra_importer = fake_em_infra_importer

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


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_b():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(em_infra_client=Mock(), emson_client=Mock())
    collector.em_infra_importer = fake_em_infra_importer

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
