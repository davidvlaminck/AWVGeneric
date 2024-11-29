from requests import Response

from API.AbstractRequester import AbstractRequester


class CookieRequester(AbstractRequester):
    def __init__(self, cookie: str = '', first_part_url: str = ''):
        super().__init__(first_part_url=first_part_url)
        self.cookie = cookie
        self.headers.update({'Cookie': f'acm-awv={cookie}'})

    def get(self, url: str = '', **kwargs) -> Response:
        kwargs = self.modify_kwargs_for_bearer_token(kwargs)
        return super().get(url=url, **kwargs)

    def post(self, url: str = '', **kwargs) -> Response:
        kwargs = self.modify_kwargs_for_bearer_token(kwargs)
        return super().post(url=url, **kwargs)

    def put(self, url: str = '', **kwargs) -> Response:
        kwargs = self.modify_kwargs_for_bearer_token(kwargs)
        return super().put(url=url, **kwargs)

    def patch(self, url: str = '', **kwargs) -> Response:
        kwargs = self.modify_kwargs_for_bearer_token(kwargs)
        return super().patch(url=url, **kwargs)

    def delete(self, url: str = '', **kwargs) -> Response:
        kwargs = self.modify_kwargs_for_bearer_token(kwargs)
        return super().delete(url=url, **kwargs)

    @staticmethod
    def modify_kwargs_for_bearer_token(kwargs: dict) -> dict:
        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        for arg, headers in kwargs.items():
            if arg == 'headers':
                if 'accept' not in headers:
                    headers['accept'] = ''
                if headers["accept"] is not None:
                    headers["accept"] = (
                        headers["accept"] + ", application/json"
                        if headers["accept"] != ''
                        else "application/json"
                    )
                headers['Content-Type'] = 'application/vnd.awv.eminfra.v1+json'
                kwargs['headers'] = headers
        return kwargs