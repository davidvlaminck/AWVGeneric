from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import DocumentCategorieEnum, ProvincieEnum
from API.Enums import AuthType, Environment
from functions import download_documents


if __name__ == '__main__':
    awv_acm_cookie = ''
    eminfra_client = EMInfraClient(cookie=awv_acm_cookie, auth_type=AuthType.COOKIE, env=Environment.PRD)

    # edelta_dossiernummer
    edelta_dossiernummer = 'VWT/DVM/2023/3'

    # document_categorien
    document_categorie = [DocumentCategorieEnum.FOTO]
    print(f'De mogelijke document categoriën zijn: {[item.value for item in DocumentCategorieEnum]}')

    # toezichter
    toezichter = 'Stefan Missotten'

    # provincie
    print(f'De mogelijke provincies zijn: {[item.value for item in ProvincieEnum]}')
    # provincie = [ProvincieEnum.ANTWERPEN]
    provincie = None

    download_dir = download_documents(eminfra_client=eminfra_client
                                      , edelta_dossiernummer=edelta_dossiernummer
                                      , document_categorie=document_categorie
                                      , provincie=provincie
                                      , toezichter=toezichter
                                      )
