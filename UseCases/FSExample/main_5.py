import asyncio
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.Enums import Environment, AuthType

async def main():
    cookie='2eceed0bd1d44e50a507bb9e8d5eedfd'
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.COOKIE, cookie=cookie)

    assettype = await eminfra_client.get_assettype_by_id('a7eadedf-b5cf-491b-8b89-ccced9a37004')

    print(assettype)


if __name__ == '__main__':
    asyncio.run(main())