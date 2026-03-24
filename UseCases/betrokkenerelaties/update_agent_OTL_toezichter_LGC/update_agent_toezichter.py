import json
import logging
from typing import Optional, Tuple, List

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.eminfra.EMInfraDomain import AssetDTO, AgentDTO, IdentiteitKenmerk, ToezichtgroepDTO, SchadebeheerderKenmerk

from UseCases.utils import (
    load_settings_path,
    configure_logger,
    build_query_search_by_naampad,
    build_query_search_betrokkenerelaties,
)

ENVIRONMENT = Environment.PRD

def splits_assets(assets: List[AssetDTO]) -> Tuple[List[AssetDTO], List[AssetDTO]]:
    """
    Splits a list of assets into Legacy- and OTL-assets.

    Args:
        assets: List of assets.

    Returns:
        Tuple of (LGC assets, OTL assets).
    """
    lgc_assets = []
    otl_assets = []
    for asset in assets:
        if asset.type.uri.startswith("https://lgc.data.wegenenverkeer.be"):
            lgc_assets.append(asset)
        elif asset.type.uri.startswith("https://wegenenverkeer.data.vlaanderen.be/"):
            otl_assets.append(asset)
        else:
            raise NotImplementedError(asset.type.uri)
    return lgc_assets, otl_assets

def get_single_generator_element(gen) -> Optional[object]:
    """
    Return the first element from a generator, or None if empty.
    """
    my_list = list(gen)
    return my_list[0] if my_list else None

def fetch_agent(client: EMInfraClient, name: str, actief: bool = True) -> Optional[object]:
    """
    Fetch an agent by name.
    """
    return get_single_generator_element(client.agent_service.search_agent(naam=name, actief=actief))

def update_lgc_asset(
    client: EMInfraClient,
    asset: AssetDTO,
    toezichter_old: IdentiteitKenmerk,
    toezichter_new: IdentiteitKenmerk,
    toezichtgroep_new: ToezichtgroepDTO,
    schadebeheerder_new: SchadebeheerderKenmerk,
    idx: int,
) -> None:
    """
    Update LGC asset with new toezichter, toezichtsgroep, and schadebeheerder.
    """
    logging.info(f"Update asset {idx}: {asset.uuid}.")
    toezichterkenmerk_voorgaand = client.toezichter_service.get_toezichter_by_uuid(asset_uuid=asset.uuid)
    if (
        toezichterkenmerk_voorgaand.toezichter is not None
        and toezichterkenmerk_voorgaand.toezichter.uuid == toezichter_old.uuid
    ):
        if toezichter_old.uuid == toezichter_new.uuid:
            logging.info(f'Old and new toezichter are identical. No update needed.')
        else:
            logging.info(f'Update toezichter from "{toezichter_old.naam}" to "{toezichter_new.naam}".')
            client.toezichter_service.add_toezichter(
                asset_uuid=asset.uuid,
                toezichter_uuid=toezichter_new.uuid,
                toezichtgroep_uuid=toezichtgroep_new.uuid,
            )
        logging.info(f"Update schadebeheerder to {schadebeheerder_new.naam}.")
        client.schadebeheerder_service.add_schadebeheerder_by_uuid(
            asset_uuid=asset.uuid,
            schadebeheerder=schadebeheerder_new,
        )

def update_otl_asset_replace_agent(
    client: EMInfraClient,
    asset: AssetDTO,
    old_agent: AgentDTO,
    new_agent: AgentDTO,
    role: str,
    idx: int,
) -> None:
    """
    Update OTL asset's betrokkenerelatie for a given role.
    param add_agent: Add Agent. Add the new agent at all costs. Default False.
    type add_agent: bool
    """
    logging.info(f"Update asset {idx}: {asset.uuid}.")
    query = build_query_search_betrokkenerelaties(bron_asset=asset, agent=old_agent, rol=role)
    betrokkenerelatie = get_single_generator_element(client.agent_service.search_betrokkenerelaties(query_dto=query))
    if betrokkenerelatie:
        if betrokkenerelatie.doel["uuid"] == new_agent.uuid:
            logging.info(f'Old and new agent "{new_agent.naam}" are identical (rol={role}. No update needed.')
        else:
            try:
                client.agent_service.add_betrokkenerelatie(
                    asset=asset,
                    agent_uuid=new_agent.uuid,
                    rol=role,
                )
                client.agent_service.remove_betrokkenerelatie(betrokkenerelatie_uuid=betrokkenerelatie.uuid)
            except Exception as e:
                raise ValueError(
                    f"Exception occurred: {e} updating HeeftBetrokkene-relatie.\n"
                    f"Failed to update {role} from {old_agent.naam} to {new_agent.naam}."
                ) from e


if __name__ == "__main__":
    configure_logger()
    logging.info("Patrick Van Ransbeeck")
    logging.info("Update toezichter, toezichtsgroep, and schadebeheerder for all assets in a given boomstructuur.")

    with open("agent_toezichter_config.json", "r", encoding="utf-8") as file:
        data = json.load(file)
    beheerobjecten = data.keys()

    eminfra_client = EMInfraClient(env=ENVIRONMENT, auth_type=AuthType.JWT, settings_path=load_settings_path())

    for beheerobject in beheerobjecten:
        logging.info(f"Updating assets in boomstructuur: {beheerobject}.")
        lgc_info = data[beheerobject]["lgc"]
        otl_info = data[beheerobject]["otl"]

        # Fetch LGC agents
        toezichter_lgc_old = get_single_generator_element(
            eminfra_client.toezichter_service.search_identiteit(naam=lgc_info["old"]["toezichter"], actief=None)
        )
        toezichter_lgc_new = get_single_generator_element(
            eminfra_client.toezichter_service.search_identiteit(naam=lgc_info["new"]["toezichter"])
        )
        toezichtgroep_lgc_new = get_single_generator_element(
            eminfra_client.toezichter_service.search_toezichtgroep_lgc(naam=lgc_info["new"]["toezichtgroep"])
        )
        schadebeheerder_lgc_new = eminfra_client.schadebeheerder_service.get_schadebeheerder_by_name(
            name=lgc_info["new"]["schadebeheerder"])[0]

        # Fetch OTL agents
        toezichter_otl_old = fetch_agent(eminfra_client, otl_info["old"]["toezichter"])
        toezichter_otl_new = fetch_agent(eminfra_client, otl_info["new"]["toezichter"])
        toezichtsgroep_otl_old = fetch_agent(eminfra_client, otl_info["old"]["toezichtsgroep"])
        toezichtsgroep_otl_new = fetch_agent(eminfra_client, otl_info["new"]["toezichtsgroep"])
        schadebeheerder_otl_old = fetch_agent(eminfra_client, otl_info["old"]["schadebeheerder"])
        schadebeheerder_otl_new = fetch_agent(eminfra_client, otl_info["new"]["schadebeheerder"])

        # Search assets
        query_dto = build_query_search_by_naampad(naampad=beheerobject)
        assets = list(eminfra_client.asset_service.search_assets_generator(query_dto=query_dto, actief=True))
        lgc_assets, otl_assets = splits_assets(assets)

        # Update LGC assets
        for idx, asset in enumerate(lgc_assets):
            update_lgc_asset(
                client=eminfra_client,
                asset=asset,
                toezichter_old=toezichter_lgc_old,
                toezichter_new=toezichter_lgc_new,
                toezichtgroep_new=toezichtgroep_lgc_new,
                schadebeheerder_new=schadebeheerder_lgc_new,
                idx=idx,
            )

        # Update OTL assets
        for idx, asset in enumerate(otl_assets):
            query = build_query_search_betrokkenerelaties(bron_asset=asset, agent=toezichter_otl_old, rol='toezichter')
            asset_betrokkenerelatie_toezichter = get_single_generator_element(eminfra_client.agent_service.search_betrokkenerelaties(query_dto=query))

            if asset_betrokkenerelatie_toezichter:
                if toezichter_otl_old.uuid == toezichter_otl_new.uuid:
                    logging.info(f'Old and new toezichter are identical ("{toezichter_otl_old.naam}" = "{toezichter_lgc_old.naam}"). No update needed.')
                else:
                    update_otl_asset_replace_agent(client=eminfra_client, asset=asset, old_agent=toezichter_otl_old,
                                           new_agent=toezichter_otl_new, role="toezichter", idx=idx)

                # Toezichtsgroep
                query = build_query_search_betrokkenerelaties(bron_asset=asset, agent=toezichtsgroep_otl_new, rol='toezichtsgroep')
                asset_betrokkenerelatie_toezichtsgroep = get_single_generator_element(
                    eminfra_client.agent_service.search_betrokkenerelaties(query_dto=query))
                if asset_betrokkenerelatie_toezichtsgroep:
                    if asset_betrokkenerelatie_toezichtsgroep.doel["uuid"] == toezichtsgroep_otl_new.uuid:
                        logging.info(
                            f'Old and new toezichter are identical ("{asset_betrokkenerelatie_toezichtsgroep.doel["naam"]}" = "{toezichtsgroep_otl_new.naam}"). No update needed.')
                    else:
                        update_otl_asset_replace_agent(
                            client=eminfra_client, asset=asset, old_agent=toezichtsgroep_otl_old,
                            new_agent=toezichtsgroep_otl_new, role="toezichtsgroep", idx=idx)
                else:
                    eminfra_client.agent_service.add_betrokkenerelatie(asset=asset, agent_uuid=toezichtsgroep_otl_new.uuid, rol='toezichtsgroep')

                # Schadebeheerder
                query = build_query_search_betrokkenerelaties(bron_asset=asset, agent=schadebeheerder_otl_old, rol='schadebeheerder')
                asset_betrokkenerelatie_schadebeheerder = get_single_generator_element(
                    eminfra_client.agent_service.search_betrokkenerelaties(query_dto=query))
                if asset_betrokkenerelatie_schadebeheerder:
                    if asset_betrokkenerelatie_schadebeheerder.doel["uuid"] == schadebeheerder_otl_new.uuid:
                        logging.info(
                            f'Old and new toezichter are identical ("{asset_betrokkenerelatie_schadebeheerder.doel["naam"]}" = "{schadebeheerder_otl_new.naam}"). No update needed.')
                    else:
                        update_otl_asset_replace_agent(
                            client=eminfra_client, asset=asset, old_agent=schadebeheerder_otl_old,
                            new_agent=schadebeheerder_otl_new, role="schadebeheerder", idx=idx)
                else:
                    eminfra_client.agent_service.add_betrokkenerelatie(asset=asset, agent_uuid=schadebeheerder_otl_new.uuid, rol='schadebeheerder')