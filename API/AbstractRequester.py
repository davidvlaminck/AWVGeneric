import abc

from requests import Session, Response


class AbstractRequester(Session, metaclass=abc.ABCMeta):
    def __init__(self, first_part_url: str = '', retries: int = 3):
        super().__init__()
        self.first_part_url = first_part_url
        
        if retries < 1:
            raise ValueError("retries must be at least 1")
        self.retries = retries

    @abc.abstractmethod
    def get(self, url: str = '', **kwargs) -> Response:
        response = None
        for _ in range(self.retries):
            response = super().get(url=self.first_part_url + url, **kwargs)
            if str(response.status_code).startswith('2'):
                return response
        error_details = self._get_error_details_from_response(response)
        raise RuntimeError(f"GET request failed after {self.retries} retries. Last response: {response}"
                           f"\nError details: {error_details}")

    @abc.abstractmethod
    def post(self, url: str = '', **kwargs) -> Response:
        response = None
        for _ in range(self.retries):
            response = super().post(url=self.first_part_url + url, **kwargs)
            if str(response.status_code).startswith('2'):
                return response
        error_details = self._get_error_details_from_response(response)
        raise RuntimeError(f"POST request failed after {self.retries} retries. Last response: {response}."
                           f"\nError details: {error_details}")

    @abc.abstractmethod
    def put(self, url: str = '', **kwargs) -> Response:
        response = None
        for _ in range(self.retries):
            response = super().put(url=self.first_part_url + url, **kwargs)
            if str(response.status_code).startswith('2'):
                return response
        error_details = self._get_error_details_from_response(response)
        raise RuntimeError(f"PUT request failed after {self.retries} retries. Last response: {response}"
                           f"\nError details: {error_details}")

    @abc.abstractmethod
    def patch(self, url: str = '', **kwargs) -> Response:
        response = None
        for _ in range(self.retries):
            response = super().patch(url=self.first_part_url + url, **kwargs)
            if str(response.status_code).startswith('2'):
                return response
        error_details = self._get_error_details_from_response(response)
        raise RuntimeError(f"PATCH request failed after {self.retries} retries. Last response: {response}"
                           f"\nError details: {error_details}")

    @abc.abstractmethod
    def delete(self, url: str = '', **kwargs) -> Response:
        response = None
        for _ in range(self.retries):
            response = super().delete(url=self.first_part_url + url, **kwargs)
            if str(response.status_code).startswith('2'):
                return response
        error_details = self._get_error_details_from_response(response)
        raise RuntimeError(f"DELETE request failed after {self.retries} retries. Last response: {response}"
                           f"\nError details: {error_details}")

    def _get_error_details_from_response(self, response: Response) -> object:
        try:
            error_details = response.json().get('message')
        except Exception:
            try:
                error_details = response.content.decode("utf-8")
            except Exception:
                error_details = response.content  # fallback to raw bytes
        return error_details
