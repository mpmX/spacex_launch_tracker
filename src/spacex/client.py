import httpx
import json
from typing import Optional, List, Dict, Any
from exceptions.external_api_exception import ExternalAPIException


class SpaceXAPIClient:
    """
    A client for interacting with the SpaceX API v4 (https://api.spacexdata.com/v4).
    Raises ExternalAPIException on request failures.
    """

    BASE_URL = "https://api.spacexdata.com/v4"

    def __init__(self, timeout: float = 10.0):
        """
        Initializes the SpaceXAPIClientSync.

        Args:
            timeout (float): Default timeout for HTTP requests in seconds.
        """
        self._client = httpx.Client(base_url=self.BASE_URL, timeout=timeout)

    def __enter__(self):
        self._client.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.__exit__(exc_type, exc_val, exc_tb)

    def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        """
        Helper method to make an HTTP request.
        Raises ExternalAPIException for common errors.

        Args:
            method (str): HTTP method (e.g., "GET", "POST").
            endpoint (str): API endpoint path (e.g., "/launches").
            **kwargs: Additional arguments to pass to the httpx request method.

        Returns:
            Any: The JSON response data if successful.

        Raises:
            ExternalAPIException: If the request fails or the API returns an error.
        """
        request_url = f"{self.BASE_URL}{endpoint}"  # For error reporting
        try:
            response = self._client.request(method, endpoint, **kwargs)
            response.raise_for_status()  # Raises HTTPStatusError for 4xx/5xx responses
            return response.json()
        except httpx.HTTPStatusError as e:
            raise ExternalAPIException(
                message=f"API returned an error status {e.response.status_code}",
                status_code=e.response.status_code,
                url=request_url,
                response_text=e.response.text,
                original_exception=e,
            )
        except httpx.TimeoutException as e:
            raise ExternalAPIException(
                message="Request timed out", url=request_url, original_exception=e
            )
        except httpx.RequestError as e:
            raise ExternalAPIException(
                message=f"A request error occurred: {e}",
                url=request_url,
                original_exception=e,
            )
        except json.JSONDecodeError as e:
            raise ExternalAPIException(
                message="Failed to decode JSON response",
                url=request_url,
                original_exception=e,
            )
        except Exception as e:
            raise ExternalAPIException(
                message=f"An unexpected error occurred: {e}",
                url=request_url,
                original_exception=e,
            )

    def get_launches(self) -> List[Dict[str, Any]]:
        """
        Fetches all launches.
        Endpoint: /launches

        Returns:
            List[Dict[str, Any]]: A list of launch objects.
        Raises:
            ExternalAPIException: If the request fails.
        """
        print("Fetching launches...")
        return self._request("GET", "/launches")

    def get_rockets(self) -> List[Dict[str, Any]]:
        """
        Fetches all rockets.
        Endpoint: /rockets

        Returns:
            List[Dict[str, Any]]: A list of rocket objects.
        Raises:
            ExternalAPIException: If the request fails.
        """
        print("Fetching rockets...")
        return self._request("GET", "/rockets")

    def get_launchpads(self) -> List[Dict[str, Any]]:
        """
        Fetches all launchpads.
        Endpoint: /launchpads

        Returns:
            List[Dict[str, Any]]: A list of launchpad objects.
        Raises:
            ExternalAPIException: If the request fails.
        """
        print("Fetching launchpads...")
        return self._request("GET", "/launchpads")

    def close(self):
        """
        Closes the underlying httpx.Client.
        """
        self._client.close()
