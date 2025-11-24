import base64
import datetime
import hashlib
import json
import logging
import math
import string
from pathlib import Path
from random import choice

import requests
from requests import Response

from API.AbstractRequester import AbstractRequester

class JWTRequester(AbstractRequester):
    _SHA256_DIGESTINFO_PREFIX = bytes.fromhex(
        "3031300d060960864801650304020105000420"
    )  # ASN.1 DigestInfo prefix for SHA-256

    def __init__(self, private_key_path: Path, client_id: str, first_part_url: str = ''):
        super().__init__(first_part_url=first_part_url)
        self.private_key_path = private_key_path
        self.client_id = client_id
        self.first_part_url = first_part_url
        self.oauth_token = ''
        self.expires = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=1)
        self.requested_at = self.expires

    def get(self, url='', **kwargs) -> Response:
        return super().get(url=url, **self._add_bearer_token_to_kwargs(kwargs))

    def post(self, url='', **kwargs) -> Response:
        return super().post(url=url, **self._add_bearer_token_to_kwargs(kwargs))

    def put(self, url='', **kwargs) -> Response:
        return super().put(url=url, **self._add_bearer_token_to_kwargs(kwargs))

    def patch(self, url='', **kwargs) -> Response:
        return super().patch(url=url, **self._add_bearer_token_to_kwargs(kwargs))

    def delete(self, url='', **kwargs) -> Response:
        return super().delete(url=url, **self._add_bearer_token_to_kwargs(kwargs))

    def get_oauth_token(self) -> str:
        if self.expires > datetime.datetime.now(datetime.timezone.utc):
            return self.oauth_token
        self.requested_at = datetime.datetime.now(datetime.timezone.utc)
        authentication_token = self.generate_authentication_token()
        self.oauth_token, expires_in = self.get_access_token(authentication_token)
        self.expires = self.requested_at + datetime.timedelta(seconds=expires_in) - datetime.timedelta(minutes=1)
        return self.oauth_token

    def _add_bearer_token_to_kwargs(self, kwargs: dict) -> dict:
        headers = kwargs.get('headers', {}).copy()
        headers['accept'] = (
            f"{headers['accept']}, application/json"
            if headers.get('accept') else 'application/json'
        )
        headers['authorization'] = f'Bearer {self.get_oauth_token()}'
        headers.setdefault('Content-Type', 'application/vnd.awv.eminfra.v1+json')
        kwargs['headers'] = headers
        return kwargs

    @staticmethod
    def _b64url_encode(b: bytes) -> str:
        return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")

    @staticmethod
    def _b64url_decode(s: str) -> bytes:
        s2 = s + "=" * (-len(s) % 4)
        return base64.urlsafe_b64decode(s2.encode("ascii"))

    @staticmethod
    def _int_from_b64url(s: str) -> int:
        return int.from_bytes(JWTRequester._b64url_decode(s), "big")

    @staticmethod
    def _i2osp(x: int, length: int) -> bytes:
        return x.to_bytes(length, "big")

    @classmethod
    def _rsa_pkcs1_v1_5_sign_sha256(cls, jwk: dict, message: bytes) -> bytes:
        digest = hashlib.sha256(message).digest()
        digest_info = cls._SHA256_DIGESTINFO_PREFIX + digest
        n_int = cls._int_from_b64url(jwk["n"])
        k = math.ceil(n_int.bit_length() / 8)
        t_len = len(digest_info)
        if k < t_len + 11:
            raise ValueError("Intended encoded message length too short")
        ps_len = k - t_len - 3
        em = b"\x00\x01" + (b"\xff" * ps_len) + b"\x00" + digest_info
        em_int = int.from_bytes(em, "big")
        d_int = cls._int_from_b64url(jwk["d"])
        s_int = pow(em_int, d_int, n_int)
        return cls._i2osp(s_int, k)

    @staticmethod
    def _int_from_b64url(s: str) -> int:
        return int.from_bytes(JWTRequester._b64url_decode(s), "big")

    @staticmethod
    def _i2osp(x: int, length: int) -> bytes:
        return x.to_bytes(length, "big")

    @classmethod
    def _rsa_pkcs1_v1_5_sign_sha256(cls, jwk: dict, message: bytes) -> bytes:
        digest = hashlib.sha256(message).digest()
        digest_info = cls._SHA256_DIGESTINFO_PREFIX + digest
        n_int = cls._int_from_b64url(jwk["n"])
        k = math.ceil(n_int.bit_length() / 8)
        t_len = len(digest_info)
        if k < t_len + 11:
            raise ValueError("Intended encoded message length too short")
        ps_len = k - t_len - 3
        em = b"\x00\x01" + (b"\xff" * ps_len) + b"\x00" + digest_info
        em_int = int.from_bytes(em, "big")
        d_int = cls._int_from_b64url(jwk["d"])
        s_int = pow(em_int, d_int, n_int)
        return cls._i2osp(s_int, k)

    def generate_authentication_token(self) -> str:
        self.requested_at = datetime.datetime.now(datetime.timezone.utc)
        payload = {
            'iss': self.client_id,
            'sub': self.client_id,
            'aud': 'https://authenticatie.vlaanderen.be/op',
            'exp': int((self.requested_at + datetime.timedelta(minutes=9)).timestamp()),
            'jti': ''.join(choice(string.ascii_lowercase) for _ in range(20))
        }
        header = {"alg": "RS256", "typ": "JWT"}
        header_b = json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        payload_b = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        signing_input = f"{self._b64url_encode(header_b)}.{self._b64url_encode(payload_b)}".encode("utf-8")
        with open(self.private_key_path, "r", encoding="utf-8") as f:
            private_key_json = json.load(f)
        signature = self._rsa_pkcs1_v1_5_sign_sha256(private_key_json, signing_input)
        return signing_input.decode("utf-8") + "." + self._b64url_encode(signature)

    def get_access_token(self, token: str) -> (str, int):
        url = 'https://authenticatie.vlaanderen.be/op/v1/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        request_body = {
            'grant_type': 'client_credentials',
            'scope': 'awv_toep_services',
            'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
            'client_id': self.client_id,
            "client_assertion": token
        }
        response = requests.post(url, data=request_body, headers=headers)
        if response.status_code != 200:
            logging.error(
                'Status: %s, Headers: %s, Error Response: %s',
                response.status_code, response.headers, response.content
            )
            raise RuntimeError(f'Could not get the access token: {response.content}')
        response_json = response.json()
        return response_json['access_token'], response_json['expires_in']
