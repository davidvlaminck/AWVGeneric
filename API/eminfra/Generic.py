from API.eminfra.EMInfraDomain import RelatieEnum


def get_kenmerktype_and_relatietype_id(relatie: RelatieEnum) -> (str, str):
    """
    Returns kenmerktype_uuid and relatietype_uuid.

    :param relatie: RelatieEnum
    :return: Tuple of strings kenmerktype_uuid and relatietype_uuid
    """
    relaties_dict = {
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing": [
            "3e207d7c-26cd-468b-843c-6648c7eeebe4",
            "93c88f93-6e8c-4af3-a723-7e7a6d6956ac"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsNetwerkECC": [
            "",
            "41c7e2eb-17be-4f53-a49e-0f3bc31efdd0"
        ],
        "https://grp.data.wegenenverkeer.be/ns/onderdeel#DeelVan": [
            "",
            "afbe8124-a9e2-41b9-a944-c14a41a9f4d5"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#SluitAanOp": [
            "",
            "b4e89ae7-cb69-449c-946b-fdff13f63a7a"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt": [
            "91d6223c-c5d7-4917-9093-f9dc8c68dd3e",
            "f2c5c4a1-0899-4053-b3b3-2d662c717b44"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsSWOnderdeelVan": [
            "",
            "1aa9795c-7ed0-4d96-87b9-e51159055755"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsAdmOnderdeelVan": [
            "",
            "dcc18707-2ca1-4b35-bfff-9fa262da96dd"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij": [
            "8355857b-8892-45a5-a86b-6375b797c764",
            "812dd4f3-c34e-43d1-88f1-3bcd0b1e89c2"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijhorendeAssets": [
            "5d58905c-412c-44f8-8872-21519041e391",
            "812dd4f3-c34e-43d1-88f1-3bcd0b1e89c2"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VoedtAangestuurd": [
            "",
            "a6747802-7679-473f-b2bd-db2cfd1b88d7"
        ],
        "https://bz.data.wegenenverkeer.be/ns/onderdeel#Bezoekt": [
            "",
            "e801b062-74e1-4b39-9401-163dd91d5494"
        ],
        "https://bz.data.wegenenverkeer.be/ns/onderdeel#HeeftBeheeractie": [
            "",
            "cd5104b3-5e98-4055-8af2-5724bf141e44"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijlage": [
            "",
            "e7d8e795-06ef-4e0f-b049-c736b54447c9"
        ],
        "https://bz.data.wegenenverkeer.be/ns/onderdeel#IsAanleiding": [
            "",
            "fef0df58-8243-4869-a056-a71346bf6acd"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsSWGehostOp": [
            "",
            "20b29934-fd5e-490f-a94b-e566513be407"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Omhult": [
            "",
            "e2c644ec-7fbd-48ff-906a-4747b43b11a5"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#LigtOp": [
            "",
            "321c18b8-92ca-4188-a28a-f00cdfaa0e31"
        ],
        "https://lgc.data.wegenenverkeer.be/ns/onderdeel#GemigreerdNaar": [
            "",
            "f0ed1efa-fe29-4861-89dc-5d3bc40f0894"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBeheer": [
            "",
            "6c91fe94-8e29-4906-a02c-b8507495ad21"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging": [
            "c3494ff0-9e02-4c11-856c-da8db6238768",
            "3ff9bf1c-d852-442e-a044-6200fe064b20"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#GeeftBevestigingAan": [
            "cef6a3c0-fd1b-48c3-8ee0-f723e55dd02b",
            "3ff9bf1c-d852-442e-a044-6200fe064b20"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftNetwerktoegang": [
            "",
            "3a63adb8-493a-4aa8-8e2e-164fd942b0b9"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftToegangsprocedure": [
            "",
            "0da67bde-0152-445f-8f29-6a9319f890fd"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftNetwerkProtectie": [
            "",
            "34d043f5-583d-4c1e-9f99-4d89fcb84ef4"
        ],
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftAanvullendeGeometrie": [
            "",
            "de86510a-d61c-46fb-805d-c04c78b27ab6"
        ]
    }
    return relaties_dict[relatie.value]