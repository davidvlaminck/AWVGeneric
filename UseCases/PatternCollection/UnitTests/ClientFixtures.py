from unittest.mock import Mock

import pytest

from API.AbstractRequester import AbstractRequester
from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory


@pytest.fixture
def fake_eminfra_client() -> EMInfraClient:
    asset_1 = {
        "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000001-bGdjOmluc3RhbGxhdGllI0thc3Q",
        "AIMDBStatus.isActief": True,
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000001-bGdjOmluc3RhbGxhdGllI0thc3Q"
        },
        "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
        "tz:Schadebeheerder.schadebeheerder": {
            "tz:DtcBeheerder.naam": "District Schadebeheerder",
            "tz:DtcBeheerder.referentie": "100"
        },
        "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
        "AIMObject.notitie": "",
        "NaampadObject.naampad": "NAAMPAD/KAST",
        "AIMNaamObject.naam": "KAST",
        "loc:Locatie.omschrijving": "omschrijving",
    }
    asset_inactief = {
        "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000010-bGdjOmluc3RhbGxhdGllI0thc3Q",
        "AIMDBStatus.isActief": False
    }
    asset_2 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000002-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.A01.WV1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000002-"
        },
        "AIMObject.datumOprichtingObject": "2020-01-01",
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "VerlichtingstoestelLED.kleurTemperatuur": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedKleurTemp/3000",
        "Verlichtingstoestel.systeemvermogen": 100,
        "Verlichtingstoestel.modelnaam": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelModelnaam/ampera",
        "Verlichtingstoestel.merk": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelMerk/Schreder",
        "VerlichtingstoestelLED.lichtpuntHoogte": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedLichtpunthoogte/6",
        "VerlichtingstoestelLED.aantalTeVerlichtenRijstroken": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedAantalTeVerlichtenRijstroken/1",
        "VerlichtingstoestelLED.lumenOutput": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlLumenOutput/10000",
        "VerlichtingstoestelLED.overhang": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedOverhang/1-0",
        "Verlichtingstoestel.verlichtGebied": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelVerlichtGebied/hoofdweg",
        "VerlichtingstoestelLED.verlichtingsNiveau": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedVerlNiveau/M3",
    }
    asset_3 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000003-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.C02.WV1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000003-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
    }
    asset_4 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000004-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.A01',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000004-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
        "geo:Geometrie.log": [
            {
                "geo:DtcLog.bron": "https://geo.data.wegenenverkeer.be/id/concept/KlLogBron/meettoestel",
                "geo:DtcLog.niveau": "https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/0",
                "geo:DtcLog.geometrie": {
                    "geo:DtuGeometrie.punt": "POINT Z (200000.00 200001.00 0)"
                },
            }
        ],
        "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
        "Lichtmast.kleur": "7038",
        "WVLichtmast.aantalArmen": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLichtmastAantArmen/4",
        "WVLichtmast.armlengte": "https://wegenenverkeer.data.vlaanderen.be/doc/concept/KlWvLichtmastArmlengte/1.5",
        'Lichtmast.masthoogte': {"DtuLichtmastMasthoogte.standaardHoogte":
            "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlLichtmastMasthoogte/12.00"
        }
    }
    asset_5 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVConsole",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000005-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.FOUT1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000005-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVConsole",
        "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/uit-gebruik",
    }
    asset_6 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000006-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.A01.WV1.AC1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000006-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
        "Armatuurcontroller.serienummer": '1234561',
        "Armatuurcontroller.merk": 'merk 1',
        "Armatuurcontroller.modelnaam": 'model 1'
    }
    asset_7 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000007-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.C02.WV1.AC1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000007-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
        "Armatuurcontroller.merk": 'merk 1',
        "Armatuurcontroller.modelnaam": 'model 1'
    }
    asset_8 = {
        "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000008-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A01',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000008-"
        },
        "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
        "NaampadObject.naampad": "A0000/A0000.WV/A01",
        "loc:Locatie.puntlocatie": {
            "loc:DtcPuntlocatie.bron": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatieBron/manueel",
            "loc:3Dpunt.puntgeometrie": {
                "loc:DtcCoord.lambert72": {
                    "loc:DtcCoordLambert72.ycoordinaat": 200001.0,
                    "loc:DtcCoordLambert72.zcoordinaat": 0,
                    "loc:DtcCoordLambert72.xcoordinaat": 200001.0
                }
            },
            "loc:DtcPuntlocatie.precisie": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatiePrecisie/meter"
        },
        "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
        'lgc:EMObject.aantalTeVerlichtenRijvakkenLed': 'R1',
        'lgc:EMObject.aantalVerlichtingstoestellen': 4,
        'lgc:EMObject.contractnummerLeveringLed': '123456',
        'lgc:EMObject.datumInstallatieLed': '2020-01-01',
        'lgc:EMObject.kleurtemperatuurLed': 'K3000',
        'lgc:EMObject.ledVerlichting': True,
        'lgc:VPLMast.lichtmastBuitenGebruik': False,
        'lgc:EMObject.lichtpunthoogteTovRijweg': 6,
        'lgc:EMObject.lumenPakketLed': 10000,
        'lgc:EMObject.overhangLed': 'O+1',
        'lgc:VPLMast.ralKleurVplmast': '7038',
        'lgc:VPLMast.serienummerArmatuurcontroller1': '1234561',
        'lgc:VPLMast.serienummerArmatuurcontroller2': '1234562',
        'lgc:VPLMast.serienummerArmatuurcontroller3': '1234563',
        'lgc:VPLMast.serienummerArmatuurcontroller4': '1234564',
        'lgc:VPLMast.merkEnTypeArmatuurcontroller1': 'merk 1 model 1',
        'lgc:VPLMast.merkEnTypeArmatuurcontroller2': 'merk 2 model 2',
        'lgc:VPLMast.merkEnTypeArmatuurcontroller3': 'merk 3 model 3',
        'lgc:VPLMast.merkEnTypeArmatuurcontroller4': 'merk 4 model 4',
        'lgc:EMObject.verlichtingsniveauLed': 'M3',
        'lgc:EMObject.verlichtingstoestelMerkEnType': 'Schreder Ampera',
        'lgc:EMObject.verlichtingstoestelSysteemvermogen': 100,
        'lgc:EMObject.verlichtingstype': 'hoofdbaan',
        'lgc:VPLMast.aantalArmen': '4',
        'lgc:VPLMast.armlengte': '1,5',
        'lgc:VPLMast.paalhoogte': '12,00',
    }
    asset_9 = {
        "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPConsole",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000009-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'C02',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000009-"
        },
        "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPConsole",
        "NaampadObject.naampad": "A0000/A0000.WV/C02",
        "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
        'lgc:EMObject.aantalTeVerlichtenRijvakkenLed': 'R2',
        'lgc:EMObject.aantalVerlichtingstoestellen': 1,
        'lgc:EMObject.contractnummerLeveringLed': '123456',
        'lgc:EMObject.datumInstallatieLed': '2020-01-01',
        'lgc:EMObject.kleurtemperatuurLed': 'K3000',
        'lgc:EMObject.ledVerlichting': True,
        'lgc:VPConsole.consoleBuitenGebruik': False,
        'lgc:EMObject.lichtpunthoogteTovRijweg': 5,
        'lgc:EMObject.lumenPakketLed': 10000,
        'lgc:EMObject.overhangLed': '0',
        'lgc:VPConsole.ralKleurVpconsole': '7038',
        'lgc:EMObject.serienummerArmatuurcontroller': '1234561',
        'lgc:EMObject.verlichtingsniveauLed': 'M2',
        'lgc:EMObject.verlichtingstoestelMerkEnType': 'Schreder Ampera',
        'lgc:EMObject.verlichtingstoestelSysteemvermogen': 100,
        'lgc:EMObject.verlichtingstype': 'hoofdbaan'
    }

    relatie_10 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000002-Bevestigin-000000000004-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000002-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000004-"
        }
    }
    relatie_11 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000006-Bevestigin-000000000002-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000006-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000002-"
        }
    }
    relatie_12 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000005-Bevestigin-000000000003-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVConsole",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000005-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000003-"
        }
    }
    relatie_13 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000003-Bevestigin-000000000007-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000003-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000007-"
        }
    }
    relatie_14 = {
        "@type": "https://grp.data.wegenenverkeer.be/ns/onderdeel#HoortBij",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000004--HoortBij--000000000008-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000004-"
        },
        "RelatieObject.doel": {
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000008-"
        }
    }
    relatie_15 = {
        "@type": "https://grp.data.wegenenverkeer.be/ns/onderdeel#HoortBij",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000005--HoortBij--000000000009-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVConsole",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000005-"
        },
        "RelatieObject.doel": {
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPConsole",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000009-"
        }
    }

    asset_21 = {
        "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPBevestig",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000021-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'C03',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000021-"
        },
        "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPBevestig",
        "NaampadObject.naampad": "A0000/A0000.WV/C03",
        "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
    }

    asset_22 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000022-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.A01.WV2',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000022-"
        },
        "AIMObject.datumOprichtingObject": "2020-01-01",
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "VerlichtingstoestelLED.kleurTemperatuur": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedKleurTemp/3000",
        "Verlichtingstoestel.systeemvermogen": 100,
        "Verlichtingstoestel.modelnaam": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelModelnaam/ampera",
        "Verlichtingstoestel.merk": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelMerk/Schreder",
        "VerlichtingstoestelLED.lichtpuntHoogte": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedLichtpunthoogte/6",
        "VerlichtingstoestelLED.aantalTeVerlichtenRijstroken": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedAantalTeVerlichtenRijstroken/1",
        "VerlichtingstoestelLED.lumenOutput": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlLumenOutput/10000",
        "VerlichtingstoestelLED.overhang": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedOverhang/1-0",
        "Verlichtingstoestel.verlichtGebied": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelVerlichtGebied/hoofdweg",
        "VerlichtingstoestelLED.verlichtingsNiveau": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedVerlNiveau/M3",
    }
    asset_23 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000023-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.A01.WV3',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000023-"
        },
        "AIMObject.datumOprichtingObject": "2020-01-01",
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "VerlichtingstoestelLED.kleurTemperatuur": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedKleurTemp/3000",
        "Verlichtingstoestel.systeemvermogen": 100,
        "Verlichtingstoestel.modelnaam": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelModelnaam/ampera",
        "Verlichtingstoestel.merk": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelMerk/Schreder",
        "VerlichtingstoestelLED.lichtpuntHoogte": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedLichtpunthoogte/6",
        "VerlichtingstoestelLED.aantalTeVerlichtenRijstroken": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedAantalTeVerlichtenRijstroken/1",
        "VerlichtingstoestelLED.lumenOutput": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlLumenOutput/10000",
        "VerlichtingstoestelLED.overhang": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedOverhang/1",
        "Verlichtingstoestel.verlichtGebied": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelVerlichtGebied/hoofdweg",
        "VerlichtingstoestelLED.verlichtingsNiveau": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedVerlNiveau/M3",
    }
    asset_24 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000024-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.A01.WV4',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000024-"
        },
        "AIMObject.datumOprichtingObject": "2020-01-01",
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "VerlichtingstoestelLED.kleurTemperatuur": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedKleurTemp/3000",
        "Verlichtingstoestel.systeemvermogen": 100,
        "Verlichtingstoestel.modelnaam": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelModelnaam/ampera",
        "Verlichtingstoestel.merk": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelMerk/Schreder",
        "VerlichtingstoestelLED.lichtpuntHoogte": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedLichtpunthoogte/6",
        "VerlichtingstoestelLED.aantalTeVerlichtenRijstroken": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedAantalTeVerlichtenRijstroken/1",
        "VerlichtingstoestelLED.lumenOutput": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlLumenOutput/10000",
        "VerlichtingstoestelLED.overhang": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedOverhang/1",
        "Verlichtingstoestel.verlichtGebied": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelVerlichtGebied/hoofdweg",
        "VerlichtingstoestelLED.verlichtingsNiveau": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedVerlNiveau/M3",
    }
    asset_25 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000025-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.C03.WV1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000025-"
        },
        "AIMObject.datumOprichtingObject": "2020-01-01",
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
    }
    asset_26 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000026-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.A01.WV2.AC1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000026-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
        "Armatuurcontroller.serienummer": '1234562',
        "Armatuurcontroller.merk": 'merk 2',
        "Armatuurcontroller.modelnaam": 'model 2'
    }

    relatie_31 = {
        "@type": "https://grp.data.wegenenverkeer.be/ns/onderdeel#HoortBij",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000025--HoortBij--000000000021-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000025-"
        },
        "RelatieObject.doel": {
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPBevestig",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000021-"
        }
    }
    relatie_32 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000022-Bevestigin-000000000004-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000022-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000004-"
        }
    }
    relatie_33 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000023-Bevestigin-000000000004-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000023-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000004-"
        }
    }
    relatie_34 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000024-Bevestigin-000000000004-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000024-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000004-"
        }
    }
    relatie_35 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000002-Bevestigin-000000000026-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000002-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000026-"
        }
    }

    # from mock import patch
    #
    #
    # def test_database_thing(self):
    #     def __init__(self, dbName, user, password):
    #         # do something else
    #     with patch.object(DatabaseThing, '__init__', __init__):
    #         # assert something

    orig_create_requester = RequesterFactory.create_requester
    RequesterFactory.create_requester = Mock()
    eminfra_client = EMInfraClient(auth_type=AuthType.JWT, env=Environment.DEV, settings_path=None)
    eminfra_client.requester = Mock(spec=AbstractRequester)

    yield eminfra_client

    RequesterFactory.create_requester = orig_create_requester

    #
    # if resource == 'assets':
    #     yield from iter([a for a in [asset_1, asset_2, asset_3, asset_4, asset_5, asset_6, asset_7, asset_8, asset_9,
    #                                  asset_inactief, asset_21, asset_22, asset_23, asset_24, asset_25, asset_26]
    #                      if a['@id'][39:75] in filter_dict['uuid']])
    # elif resource == 'assetrelaties':
    #     assetrelaties = [relatie_10, relatie_11, relatie_12, relatie_13, relatie_14, relatie_15,
    #                      relatie_31, relatie_32, relatie_33, relatie_34, relatie_35]
    #     if 'uuid' in filter_dict:
    #         yield from iter([r for r in assetrelaties
    #                          if r['@id'][46:82] in filter_dict['uuid']])
    #     elif 'asset' in filter_dict:
    #         yield from iter([r for r in assetrelaties
    #                          if r['RelatieObject.bron']['@id'][39:75] in filter_dict['asset'] or
    #                          r['RelatieObject.doel']['@id'][39:75] in filter_dict['asset']])
    #
    #
