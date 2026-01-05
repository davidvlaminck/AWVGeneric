from datetime import datetime

import pandas as pd
from pathlib import Path

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    environment = Environment.PRD
    print(f'environment:\t\t{environment}')
    settings_path = Path.home() / 'OneDrive - Nordend' / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    generator_agents = eminfra_client.agents.search_agent(naam='John Cleese')
    agents = list(generator_agents) # convert generator to a list
    print(f"Found a total of:\t{len(agents)} agents.")

    # Convert the list to a dictionary
    agents_dict = {
        "uuid": [agent.uuid for agent in agents]
        , "naam": [agent.naam for agent in agents]
        , "ovoCode": [agent.ovoCode for agent in agents]
        , "voId": [agent.voId for agent in agents]
    }

    # Convert the dictionary to a pandas df
    df_agents = pd.DataFrame(data= agents_dict)
    df_agents_sorted = df_agents.sort_values(by="naam", ascending=True)
    # Convert pandas df to an Excel
    df_agents_sorted.to_excel(f'agents_{environment.name}_{datetime.now().date()}.xlsx', sheet_name='Agents', index=False, freeze_panes=[1,0])