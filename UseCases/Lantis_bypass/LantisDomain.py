from dataclasses import dataclass
from enum import Enum

from API.eminfra.EMInfraDomain import BoomstructuurAssetTypeEnum, RelatieEnum


class AssetType(Enum):
    """
    An enumeration of asset types with their corresponding values.
    """
    AB = 'Afstandsbewaking'
    CABINECONTROLLER = 'Cabinecontroller'
    CAMERA = 'Camera'
    CAMGROEP = 'CAMGroep'
    DNBHOOGSPANNING = 'DNBHOOGSPANNING'
    DYNBORDGROEP = 'DYNBORDGROEP'
    ENERGIEMETERDNB = 'ENERGIEMETERDNB'
    GALGPAAL = 'GALGPAAL'
    HS = 'HS'
    HSCABINE = 'HSCabine'
    HSDEEL = 'HSDeel'
    INSTALLATIE = 'Installatie'
    IP = 'IP'
    LSDEEL = 'LSDeel'
    MIVLVE = 'MIVLVE'
    MPT = 'Meetpunt'
    POORT = 'Poort'
    RSSBORD = 'RSSBord'
    RSSGROEP = 'DynBordGroep'
    RVMSBORD = 'RVMSBord'
    RVMSGROEP = 'DynBordGroep'
    SEGC = 'Segmentcontroller'
    SEINBRUG = 'Seinbrug'
    TT = 'Teletransmissieverbinding'
    WEGKANTKAST = 'Wegkantkast'
    WVGROEP = 'Wegverlichtingsgroep'
    WVLICHTMAST = 'Wegverlichtingsmast'


@dataclass
class RelatieInfo:
    """
    A data class representing relationship information.

    Args:
        uri (RelatieEnum): The type of relationship.
        bronAsset_uuid (str, optional): The UUID of the source asset. Defaults to None.
        doelAsset_uuid (str, optional): The UUID of the target asset. Defaults to None.
        column_typeURI_relatie (str, optional): The type URI of the relationship column. Defaults to None.
    """
    uri: RelatieEnum
    bronAsset_uuid: str | None = None
    doelAsset_uuid: str | None = None
    column_typeURI_relatie: str | None = None


@dataclass
class AssetInfo:
    """
    A data class representing asset information.

    Args:
        asset_type (AssetType): The type of the asset.
        column_uuid (str, optional): The UUID of the column. Defaults to None.
        column_typeURI (str, optional): The type URI of the column. Defaults to None.
        column_name (str, optional): The name of the column. Defaults to None.
        column_status (str, optional): The status of the column. Defaults to None.
        column_asset_aanwezig (str, optional): The presence of the asset column. Defaults to None.
    """
    asset_type: AssetType
    column_uuid: str | None = None
    column_typeURI: str | None = None
    column_name: str | None = None
    column_status: str | None = None
    column_asset_aanwezig: str | None = None


@dataclass
class ParentAssetInfo:
    """
    A data class representing parent asset information.

    Args:
        parent_asset_type (BoomstructuurAssetTypeEnum): The type of the parent asset.
        column_parent_uuid (str, optional): The UUID of the parent column. Defaults to None.
        column_parent_name (str, optional): The name of the parent column. Defaults to None.
    """
    parent_asset_type: BoomstructuurAssetTypeEnum
    column_parent_uuid: str | None = None
    column_parent_name: str | None = None


@dataclass
class EigenschapInfo:
    """
    A data class representing property information.

    Args:
        eminfra_eigenschap_name (str): The name of the EMInfra property.
        column_eigenschap_name (str): The name of the column property.
    """
    eminfra_eigenschap_name: str
    column_eigenschap_name: str