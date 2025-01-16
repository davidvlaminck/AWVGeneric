from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import DocumentCategorieEnum, ProvincieEnum
from API.Enums import AuthType, Environment
from functions import download_documents


if __name__ == '__main__':
    # settings_path = Path('C:/Users/DriesVerdoodtNordend/OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json')
    # eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    awv_acm_cookie = '2cfb3172c2434454b07dac14a1ee8359'  # 16/01/2025
    eminfra_client = EMInfraClient(cookie=awv_acm_cookie, auth_type=AuthType.COOKIE, env=Environment.PRD)

    # edelta_dossiernummer
    edelta_dossiernummer = 'VWT/DVM/2023/3'

    # document_categorien
    document_categorien = [DocumentCategorieEnum.KEURINGSVERSLAG, DocumentCategorieEnum.ELEKTRISCH_SCHEMA]
    print(f'De mogelijke document categoriën zijn: {[item.value for item in DocumentCategorieEnum]}')

    # toezichter
    toezichter = 'Stefan Missotten'

    # provincie
    print(f'De mogelijke provincies zijn: {[item.value for item in ProvincieEnum]}')
    provincie = [ProvincieEnum.ANTWERPEN]
    # provincie = None

    download_dir = download_documents(eminfra_client=eminfra_client, edelta_dossiernummer=edelta_dossiernummer,
                                      document_categorie=document_categorien, provincie=provincie, toezichter=toezichter)