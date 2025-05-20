import logging
import sys
from pathlib import Path

from API.Enums import AuthType, Environment
from UseCases.PatternCollection.Domain.PatternVisualiser import PatternVisualiser
from UseCases.PatternCollection.Domain.PyVisWrapper import PyVisWrapper

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')


if __name__ == '__main__':
    syncer = PatternVisualiser(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)
    pattern = [('uuids', 'of', 'a'),
               ('a', 'type_of', ['installatie#MIVModule']),
               ('a', '-[r1]-', 'b'),
               ('a', '-[r2]-', 'c'),
               ('a', '-[r2]-', 'e'),
               ('c', '-[r1]-', 'd'),
               ('f', '-[r3]->', 'a'),
               ('f', '-[r1]-', 'i'),
               ('f', '-[r3]*->', 'f'),
               ('g', '-[r3]->', 'f'),
               ('h', '-[r3]->', 'g'),
               ('b', 'type_of', ['lgc:installatie#Kast']),
               ('c', 'type_of', ['onderdeel#Netwerkpoort']),
               ('d', 'type_of', ['onderdeel#Netwerkelement']),
               ('e', 'type_of', ['installatie#MIVMeetpunt']),
               ('f', 'type_of', ['lgc:installatie#LSDeel']),
               ('g', 'type_of', ['lgc:installatie#LS', 'lgc:installatie#HSDeel']),
               ('h', 'type_of', ['lgc:installatie#HS']),
               ('i', 'type_of', ['lgc:installatie#Kast', 'lgc:installatie#HSCabineLegacy']),
               ('r1', 'type_of', ['onderdeel#Bevestiging']),
               ('r2', 'type_of', ['onderdeel#Sturing']),
               ('r3', 'type_of', ['onderdeel#Voedt']),
               ('a', 'level', 0),
               ('b', 'level', -1),
               ('c', 'level', -1),
               ('d', 'level', -2),
               ('e', 'level', 1),
               ('f', 'level', -2),
               ('g', 'level', -3),
               ('h', 'level', -4),
               ('i', 'level', -1)]

    chosen_assets = syncer.em_infra_client.get_assets_by_filter(filter={
        'typeUri': 'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule', 'naam' : 'MIV233'})
    l = (list(chosen_assets))
    asset_uuids = [x['@id'][39:75] for x in l]
    syncer.collect_info_given_asset_uuids(asset_uuids=asset_uuids,
                                          asset_info_collector=syncer.collector, pattern=pattern)
    print(len(syncer.collector.collection.object_dict))

    objects_to_visualise = syncer.collector.filter_collection_by_pattern(filter_pattern=pattern,
                                                                         starting_uuids=asset_uuids)
    level_dict = syncer.collector.get_level_dict(pattern=pattern)

    PyVisWrapper().show(objects_to_visualise, launch_html=True, notebook_mode=False, level_dict=level_dict)