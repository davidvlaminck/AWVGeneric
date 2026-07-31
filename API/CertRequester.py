from pathlib import Path

from requests import Response

from API.AbstractRequester import AbstractRequester


class CertRequester(AbstractRequester):
    def __init__(self, cert_path: str = None, key_path: str = None, first_part_url: str = ''):
        super().__init__(first_part_url=first_part_url)
        
        if not Path(cert_path).exists():
            raise FileNotFoundError(f"{cert_path} is not a valid path. Cert file does not exist.")
        if not Path(key_path).exists():
            raise FileNotFoundError(f"{key_path} is not a valid path. Key file does not exist.")
        
        self.cert_path = cert_path
        self.key_path = key_path
        self.first_part_url = first_part_url

    def get(self, url: str = '', **kwargs) -> Response:
        kwargs = self.modify_kwargs_for_bearer_token(kwargs)
        return super().get(url=url, cert=(self.cert_path, self.key_path), **kwargs)

    def post(self, url: str = '', **kwargs) -> Response:
        kwargs = self.modify_kwargs_for_bearer_token(kwargs)
        return super().post(url=url, cert=(self.cert_path, self.key_path), **kwargs)

    def put(self, url: str = '', **kwargs) -> Response:
        kwargs = self.modify_kwargs_for_bearer_token(kwargs)
        return super().put(url=url, cert=(self.cert_path, self.key_path), **kwargs)

    def patch(self, url: str = '', **kwargs) -> Response:
        kwargs = self.modify_kwargs_for_bearer_token(kwargs)
        return super().patch(url=url, cert=(self.cert_path, self.key_path), **kwargs)

    def delete(self, url: str = '', **kwargs) -> Response:
        kwargs = self.modify_kwargs_for_bearer_token(kwargs)
        return super().delete(url=url, cert=(self.cert_path, self.key_path), **kwargs)

    @staticmethod
    def modify_kwargs_for_bearer_token(kwargs: dict) -> dict:
        headers = kwargs.setdefault('headers', {})
        accept = headers.get('accept', '')
        headers['accept'] = (
            f"{accept}, application/json" if accept else "application/json"
        )
        headers['Content-Type'] = 'application/vnd.awv.eminfra.v1+json'
        return kwargs
