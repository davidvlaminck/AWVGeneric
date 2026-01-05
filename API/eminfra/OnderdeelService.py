import logging


class OnderdeelService:
    def __init__(self, requester):
        self.requester = requester

    def create_onderdeel(self, naam: str, type_uuid: str) -> dict | None:
        json_body = {
            "naam": naam,
            "typeUuid": type_uuid
        }
        url = 'core/api/onderdelen'
        response = self.requester.post(url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()
