from UseCases.PatternCollection.Domain.PatternVisualiser import PatternVisualiser


def test_simple_pattern():
    dicts = [
        {'assetId': {"identificator": 'a'}, 'typeURI': 'installatie#MIVModule'},
        {'assetId': {"identificator": 'b'}, 'typeURI': 'onderdeel#Wegkantkast'},
        {'assetId': {"identificator": 'r1'}, 'typeURI': 'onderdeel#Bevestiging',
         'bronAssetId': {"identificator": 'a'},
         'doelAssetId': {"identificator": 'b'}},
    ]
    expected_pattern = [('uuids', 'of', 'a'),
                        ('a', 'type_of', ['installatie#MIVModule']),
                        ('a', '-[r1]-', 'b'),
                        ('b', 'type_of', ['onderdeel#Wegkantkast']),
                        ('r1', 'type_of', ['onderdeel#Bevestiging'])
                        ]
    generated_pattern = PatternVisualiser.generate_pattern_from_dicts(
        dicts=dicts
    )
    assert sorted(generated_pattern) == sorted(expected_pattern)


def test_simple_pattern_with_excess_assets():
    dicts = [
        {'assetId': {"identificator": 'a'}, 'typeURI': 'installatie#MIVModule'},
        {'assetId': {"identificator": 'b'}, 'typeURI': 'onderdeel#Wegkantkast'},
        {'assetId': {"identificator": 'c'}, 'typeURI': 'installatie#MIVMeetpunt'},
        {'assetId': {"identificator": 'r1'}, 'typeURI': 'onderdeel#Bevestiging',
         'bronAssetId': {"identificator": 'a'},
         'doelAssetId': {"identificator": 'b'}},
        {'assetId': {"identificator": 'r2'}, 'typeURI': 'onderdeel#Bevestiging',
         'bronAssetId': {"identificator": 'd'},
         'doelAssetId': {"identificator": 'e'}}
    ]
    expected_pattern = [('uuids', 'of', 'a'),
                        ('a', 'type_of', ['installatie#MIVModule']),
                        ('a', '-[r1]-', 'b'),
                        ('b', 'type_of', ['onderdeel#Wegkantkast']),
                        ('r1', 'type_of', ['onderdeel#Bevestiging'])
                        ]
    generated_pattern = PatternVisualiser.generate_pattern_from_dicts(
        dicts=dicts
    )
    assert sorted(generated_pattern) == sorted(expected_pattern)



def test_simple_pattern_different_ids():
    dicts = [
        {'assetId': {"identificator": 'c'}, 'typeURI': 'installatie#MIVModule'},
        {'assetId': {"identificator": 'd'}, 'typeURI': 'onderdeel#Wegkantkast'},
        {'assetId': {"identificator": 'r1'}, 'typeURI': 'onderdeel#Bevestiging',
         'bronAssetId': {"identificator": 'c'},
         'doelAssetId': {"identificator": 'd'}},
    ]
    expected_pattern = [('uuids', 'of', 'a'),
                        ('a', 'type_of', ['installatie#MIVModule']),
                        ('a', '-[r1]-', 'b'),
                        ('b', 'type_of', ['onderdeel#Wegkantkast']),
                        ('r1', 'type_of', ['onderdeel#Bevestiging'])
                        ]
    generated_pattern = PatternVisualiser.generate_pattern_from_dicts(
        dicts=dicts
    )
    assert sorted(generated_pattern) == sorted(expected_pattern)


def test_single_pattern_multiple_assets():
    dicts = [
        {'assetId': {"identificator": 'a'}, 'typeURI': 'installatie#MIVMeetpunt'},
        {'assetId': {"identificator": 'b'}, 'typeURI': 'installatie#MIVModule'},
        {'assetId': {"identificator": 'c'}, 'typeURI': 'installatie#MIVMeetpunt'},
        {'assetId': {"identificator": 'r1'}, 'typeURI': 'onderdeel#Sturing',
         'bronAssetId': {"identificator": 'a'},
         'doelAssetId': {"identificator": 'b'}},
        {'assetId': {"identificator": 'r2'}, 'typeURI': 'onderdeel#Sturing',
         'bronAssetId': {"identificator": 'c'},
         'doelAssetId': {"identificator": 'b'}},
    ]
    expected_pattern = [('uuids', 'of', 'a'),
                        ('a', 'type_of', ['installatie#MIVMeetpunt']),
                        ('a', '-[r1]-', 'b'),
                        ('b', 'type_of', ['installatie#MIVModule']),
                        ('r1', 'type_of', ['onderdeel#Sturing'])
                        ]
    generated_pattern = PatternVisualiser.generate_pattern_from_dicts(
        dicts=dicts
    )
    assert sorted(generated_pattern) == sorted(expected_pattern)


def test_bigger_pattern():
    dicts = [
        {'assetId': {"identificator": 'a'}, 'typeURI': 'installatie#MIVMeetpunt'},
        {'assetId': {"identificator": 'b'}, 'typeURI': 'installatie#MIVModule'},
        {'assetId': {"identificator": 'c'}, 'typeURI': 'onderdeel#Netwerkpoort'},
        {'assetId': {"identificator": 'd'}, 'typeURI': 'onderdeel#Netwerkelement'},
        {'assetId': {"identificator": 'e'}, 'typeURI': 'onderdeel#Wegkantkast'},
        {'assetId': {"identificator": 'r1'}, 'typeURI': 'onderdeel#Sturing',
         'bronAssetId': {"identificator": 'a'},
         'doelAssetId': {"identificator": 'b'}},
        {'assetId': {"identificator": 'r3'}, 'typeURI': 'onderdeel#Bevestiging',
         'bronAssetId': {"identificator": 'b'},
         'doelAssetId': {"identificator": 'e'}},
        {'assetId': {"identificator": 'r2'}, 'typeURI': 'onderdeel#Sturing',
         'bronAssetId': {"identificator": 'b'},
         'doelAssetId': {"identificator": 'c'}},
        {'assetId': {"identificator": 'r4'}, 'typeURI': 'onderdeel#Bevestiging',
         'bronAssetId': {"identificator": 'c'},
         'doelAssetId': {"identificator": 'd'}},
    ]
    expected_pattern = [('a', '-[r2]-', 'b'),
         ('a', 'type_of', ['installatie#MIVMeetpunt']),
         ('b', '-[r1]-', 'e'),
         ('b', '-[r2]-', 'd'),
         ('b', 'type_of', ['installatie#MIVModule']),
         ('c', '-[r1]-', 'd'),
         ('c', 'type_of', ['onderdeel#Netwerkelement']),
         ('d', 'type_of', ['onderdeel#Netwerkpoort']),
         ('e', 'type_of', ['onderdeel#Wegkantkast']),
         ('r1', 'type_of', ['onderdeel#Bevestiging']),
         ('r2', 'type_of', ['onderdeel#Sturing']),
         ('uuids', 'of', 'a')]
    generated_pattern = PatternVisualiser.generate_pattern_from_dicts(
        dicts=dicts
    )
    assert sorted(generated_pattern) == sorted(expected_pattern)