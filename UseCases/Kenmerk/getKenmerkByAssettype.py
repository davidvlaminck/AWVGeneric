import csv
import logging
from pathlib import Path

from EMInfraRestClient import EMInfraRestClient
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


def find_eig_kenmerk(kenmerktypes: [dict]) -> dict:
    for d in kenmerktypes:
        if d['standard']:
            return d


# TODO completely refactor this file

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    settings_manager = SettingsManager(
        settings_path=Path('/home/davidlinux/Documents/AWV/resources/settings_EMInfraClient.json'))

    requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
    request_handler = RequestHandler(requester)
    rest_client = EMInfraRestClient(request_handler=request_handler)

    assettypes = ['4c68c109-85be-4183-af59-104ff6ec1825', '0bce326f-cd53-4dda-b709-04799eb7f3ed',
                  '55362c2a-be7b-4efc-9437-765b351c8c51', '4dfad588-277c-480f-8cdc-0889cfaf9c78']

    with open('eigenschappenlijst.csv', 'w', newline='') as csvfile:
        fieldnames = ['assettype', 'uuid', 'actief', 'naam', 'categorie', 'definitie', 'type', 'mogelijkeWaardes',]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for assettype_uuid in assettypes:
            kenmerktypes = rest_client.get_kenmerktype_by_uuid()
            eig_kenmerk = find_eig_kenmerk(kenmerktypes)
            eigenschappen = rest_client.get_eigenschappen_by_kenmerk_uuid(kenmerk_uuid=eig_kenmerk['kenmerkType']['uuid'])
            for e in eigenschappen:
                eig = e['eigenschap']
                if eig['type']['_type'] == 'selection':
                    for value in eig['type']['values']:
                        writer.writerow({'uuid': eig['uuid'], 'actief': eig['actief'], 'naam': eig['naam'],
                                         'categorie': eig['categorie'], 'definitie': eig['definitie'],
                                         'type': eig['type']['_type'], 'assettype': eig_kenmerk['kenmerkType']['naam'],
                                         'mogelijkeWaardes': value})
                else:
                    writer.writerow({'uuid': eig['uuid'], 'actief': eig['actief'], 'naam': eig['naam'],
                                     'categorie': eig['categorie'], 'definitie': eig['definitie'],
                                     'type': eig['type']['_type'], 'assettype': eig_kenmerk['kenmerkType']['naam']})
