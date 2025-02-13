from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, LogicalOpEnum
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    gemigreerd_van_kenmerk = eminfra_client.get_kenmerktype_by_naam('Gemigreerd van')
    gemigreerd_naar_kenmerk = eminfra_client.get_kenmerktype_by_naam('Gemigreerd naar')

    assettypes = eminfra_client.get_all_assettypes()

    assettype = next((x for x in assettypes if x.uri ==
                      'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Datakabel'), None)
    print(assettype)

    bestaande_kenmerken = eminfra_client.get_kenmerken_by_assettype_uuid(assettype.uuid)
    gemigreerd_van = next((x for x in bestaande_kenmerken if x.kenmerkType.uuid == gemigreerd_van_kenmerk), None)
    print(gemigreerd_van)

    if gemigreerd_van is None:
        eminfra_client.add_kenmerk_to_assettype(assettype_uuid=assettype.uuid,
                                                kenmerktype_uuid=gemigreerd_naar_kenmerk.uuid)






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