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
    assert generated_pattern == expected_pattern


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
    assert generated_pattern == expected_pattern



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
    assert generated_pattern == expected_pattern


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
                        ('a', 'type_of', ['installatie#MIVModule']),
                        ('a', '-[r1]-', 'b'),
                        ('b', 'type_of', ['installatie#MIVMeetpunt']),
                        ('r1', 'type_of', ['onderdeel#Sturing'])
                        ]
    generated_pattern = PatternVisualiser.generate_pattern_from_dicts(
        dicts=dicts
    )
    assert generated_pattern == expected_pattern