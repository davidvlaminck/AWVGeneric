from requests import Response

from API.AbstractRequester import AbstractRequester


class CookieRequester(AbstractRequester):
    def __init__(self, cookie: str = '', first_part_url: str = ''):
        super().__init__(first_part_url=first_part_url)
        self.cookie = cookie
        self.headers.update({'Cookie': f'acm-awv={cookie}'})

    def get(self, url: str = '', **kwargs) -> Response:
        return super().get(url=url, **kwargs)

    def post(self, url: str = '', **kwargs) -> Response:
        return super().post(url=url, **kwargs)

    def put(self, url: str = '', **kwargs) -> Response:
        return super().put(url=url, **kwargs)

    def patch(self, url: str = '', **kwargs) -> Response:
        return super().patch(url=url, **kwargs)

    def delete(self, url: str = '', **kwargs) -> Response:
        return super().delete(url=url, **kwargs)