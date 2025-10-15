import logging
from openpyxl import load_workbook

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import Graph, AssetDTO, QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, OperatorEnum, \
    TermDTO
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, read_rsa_report


def get_nodes_from_graph(graph: Graph, uri: str = None) -> [AssetDTO]:
    """
    Returns a list of nodes, extracted from a graph.
    When no uri provided, return all nodes.
    If node is missing in the Graph, returns an empty list.

    :param graph:
    :param uri:
    :return:
    """
    return [
        node
        for node in graph.nodes
        if uri is not None and (node.type.uri == uri) or uri is None
    ]


def get_single_node_from_graph(graph: Graph, uri: str) -> AssetDTO:
    """
    Returns one single node, extracted from a graph.
    If node is missing or multiple nodes are available in the Graph, raises Error.
    See also: get_nodes_from_graph()

    :param graph:
    :param uri:
    :return:
    """
    list_nodes = get_nodes_from_graph(graph=graph, uri=uri)
    if len(list_nodes) != 1:
        raise ValueError(f'A total of "{len(list_nodes)}" node(s) in the graph that corresponds to uri: {uri}')
    return list_nodes[0]


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s',
                        filemode="w")
    logging.info('Ophalen eigenschappen van de beheeracties van Inspectie Wegverlichting, horend bij VPLMast.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    df_assets = pd.read_excel('Export masten C1383.xlsx', sheet_name='Sheet0', header=0, usecols=['id'])

    _, relatietype_beheeractie = eminfra_client.get_kenmerktype_and_relatietype_id(
        relatie_uri='https://bz.data.wegenenverkeer.be/ns/onderdeel#HeeftBeheeractie')
    _, relatietype_bezoekt = eminfra_client.get_kenmerktype_and_relatietype_id(
        relatie_uri='https://bz.data.wegenenverkeer.be/ns/onderdeel#Bezoekt')
    relatieTypes = [relatietype_bezoekt, relatietype_beheeractie]

    rows = []
    for idx, asset_row in df_assets.iterrows():
        logging.info("ophalen asset VPLMast")
        asset = eminfra_client.get_asset_by_id(assettype_id=asset_row.get("id"))
        row = {"VPLMast_uuid": asset.uuid, "VPLMast_naam": asset.naam}

        logging.info("Ophalen Graph, met een diepte van 2 stappen.")
        logging.info("Via de relatie Bezoekt -> InspectieWegverlichting")
        graph = eminfra_client.get_graph(asset_uuid=asset.uuid, depth=1, relatieTypes=[relatietype_bezoekt],
                                         actiefFilter=True)
        asset_VPLMast = get_single_node_from_graph(
            graph=graph,
            uri='https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast')
        asset_InspectieWegverlichting = get_single_node_from_graph(
            graph=graph,
            uri='https://bz.data.wegenenverkeer.be/ns/controlefiche#InspectieWegverlichting')

        logging.info("via de relatie HeeftBeheerActie -> InspectieMastControle en InspectieToestel")
        graph = eminfra_client.get_graph(asset_uuid=asset_InspectieWegverlichting.uuid, depth=1,
                                         relatieTypes=[relatietype_beheeractie], actiefFilter=True)
        asset_InspectieMastConsole = get_single_node_from_graph(
            graph=graph,
            uri='https://bz.data.wegenenverkeer.be/ns/beheeractie#InspectieMastConsole')
        asset_InspectieToestel = get_single_node_from_graph(
            graph=graph,
            uri='https://bz.data.wegenenverkeer.be/ns/beheeractie#InspectieToestel')

        logging.info("Ophalen van de eigenschappen van beide beheeracties")
        eigenschappen_VPLMast = eminfra_client.get_eigenschappen(assetId=asset_VPLMast.uuid)
        eigenschappen_InspectieWegverlichting = eminfra_client.get_eigenschappen(
            assetId=asset_InspectieWegverlichting.uuid)
        eigenschappen_InspectieMastConsole = eminfra_client.get_eigenschappen(assetId=asset_InspectieMastConsole.uuid)
        eigenschappen_InspectieToestel = eminfra_client.get_eigenschappen(assetId=asset_InspectieToestel.uuid)

        logging.info("Iedere eigenschap toevoegen aan het dataframe met prefix [asset_naam].[eigenschap_naam]")
        row["InspectieWegverlichting_uuid"] = asset_InspectieWegverlichting.uuid
        row["InspectieWegverlichting_naam"] = asset_InspectieWegverlichting.naam
        for eigenschap in eigenschappen_InspectieWegverlichting:
            row[f'InspectieWegverlichting_{eigenschap.eigenschap.definitie}'] = eigenschap.typedValue.get("value")

        row["InspectieMastConsole_uuid"] = asset_InspectieMastConsole.uuid
        row["InspectieMastConsole_naam"] = asset_InspectieMastConsole.naam
        for eigenschap in eigenschappen_InspectieMastConsole:
            row[f'InspectieMastConsole_{eigenschap.eigenschap.definitie}'] = eigenschap.typedValue.get("value")

        row["InspectieInspectieToestel_uuid"] = asset_InspectieToestel.uuid
        row["InspectieInspectieToestel_naam"] = asset_InspectieToestel.naam
        for eigenschap in eigenschappen_InspectieToestel:
            row[f'InspectieInspectieToestel_{eigenschap.eigenschap.definitie}'] = eigenschap.typedValue.get("value")

        rows.append(row)

    logging.info("Aanmaken pandas dataframe en wegschrijven naar Excel")
    excel_path = "bezoekfiches masten C1383.xlsx"

    # Export DataFrame to Excel using openpyxl engine
    df = pd.DataFrame(rows)
    df.to_excel(excel_path, index=False, sheet_name="info_bezoekfiches", engine="openpyxl")
    # Reopen with openpyxl to modify settings
    workbook = load_workbook(excel_path)
    sheet = workbook.active
    # Freeze the first row and first column
    sheet.freeze_panes = sheet["C2"]  # freezes row 1 and column B (info VPLMast)
    # Save changes
    workbook.save(excel_path)
