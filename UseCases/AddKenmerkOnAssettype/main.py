from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, LogicalOpEnum
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path(r'C:\Users\vlaminda\Documents\resources\settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    assettypes = eminfra_client.get_all_assettypes()

    assettype = next((x for x in assettypes if x.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Meteostation'), None)
    print(assettype)

    # POST POST /assettypes/{id}/ Kenmerk type toevoegen
    eminfra_client


#
# AssetTypeKenmerkTypeAddDTO{
# kenmerkType*	ResourceRefDTO{
# links	Links{...}
# uuid	string
# }
# }
# AssetTypeKenmerkTypeDTO{
# kenmerkType	https://apps.mow.vlaanderen.be/eminfra/core/api/swagger.json#/definitions/KenmerkTypeDTOKenmerkTypeDTO{
# links	Links{...}
# uuid	string
# createdOn	string($date-time)
# modifiedOn	string($date-time)
# naam	string
# actief	boolean
# predefined	boolean
# standard	boolean
# definitie	string
# }
# actief	boolean
# standard	boolean
# }