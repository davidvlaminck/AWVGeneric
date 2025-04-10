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
               ('b', 'type_of', ['onderdeel#Wegkantkast']),
               ('c', 'type_of', ['onderdeel#Netwerkpoort']),
               ('d', 'type_of', ['onderdeel#Netwerkelement']),
               ('e', 'type_of', ['installatie#MIVMeetpunt']),
               ('r1', 'type_of', ['onderdeel#Bevestiging']),
               ('r2', 'type_of', ['onderdeel#Sturing'])
               ]

    chosen_assets = syncer.em_infra_client.get_assets_by_filter(filter={
        'typeUri': 'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule', 'naam' : 'MIV231'})
    l = (list(chosen_assets))
    syncer.collect_info_given_asset_uuids(asset_uuids=[x['@id'][39:75] for x in l],
                                          asset_info_collector=syncer.collector, pattern=pattern)
    print(len(syncer.collector.collection.object_dict))
    PyVisWrapper().show(syncer.collector.collection.object_dict.values(), launch_html=True, notebook_mode=False)