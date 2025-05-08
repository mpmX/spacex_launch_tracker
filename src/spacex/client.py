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


# --- Example Usage (updated to handle ExternalAPIException) ---
def main():
    with SpaceXAPIClient(timeout=5.0) as client:
        print("\n--- Getting all launches ---")
        try:
            launches = client.get_launches()
            if launches:  # Should always be true if no exception
                print(f"Successfully fetched {len(launches)} launches.")
                print(f"First launch name: {launches[0].get('name', 'N/A')}")

                first_launch_id = launches[0].get("id")
                if first_launch_id:
                    print(f"\n--- Getting launch by ID: {first_launch_id} ---")
                    try:
                        one_launch = client.get_one_launch(first_launch_id)
                        print(
                            f"Details for launch '{one_launch.get('name')}': Success ({one_launch.get('success', 'Unknown')})"
                        )
                    except ExternalAPIException as e:
                        print(f"Error fetching launch by ID {first_launch_id}: {e}")
            # No 'else' needed here as an exception would have been raised
        except ExternalAPIException as e:
            print(f"Error fetching launches: {e}")

        print("\n--- Getting all rockets ---")
        try:
            rockets = client.get_rockets()
            print(f"Successfully fetched {len(rockets)} rockets.")
            print(f"First rocket name: {rockets[0].get('name', 'N/A')}")
        except ExternalAPIException as e:
            print(f"Error fetching rockets: {e}")

        print("\n--- Getting all launchpads ---")
        try:
            launchpads = client.get_launchpads()
            print(f"Successfully fetched {len(launchpads)} launchpads.")
            print(f"First launchpad name: {launchpads[0].get('name', 'N/A')}")
        except ExternalAPIException as e:
            print(f"Error fetching launchpads: {e}")

        print(
            "\n--- Testing a non-existent endpoint (expecting ExternalAPIException) ---"
        )
        try:
            # Note: _request is an internal method, typically you'd test via a public method
            # or by creating a specific public method for a known bad endpoint for testing.
            # For this example, we'll directly call it to trigger the expected error.
            client._request("GET", "/nonexistentendpoint")
            print("This should not be printed if the exception was raised.")
        except ExternalAPIException as e:
            print(f"Correctly handled non-existent endpoint. Error: {e}")
            assert (
                e.status_code == 404
            )  # For SpaceX API, non-existent usually returns 404

        print(
            "\n--- Testing with an invalid launch ID (expecting ExternalAPIException) ---"
        )
        try:
            client.get_one_launch("invalid_id_format_or_nonexistent")
            print("This should not be printed if the exception was raised.")
        except ExternalAPIException as e:
            print(f"Correctly handled invalid launch ID. Error: {e}")
            assert (
                e.status_code == 404
            )  # For SpaceX API, non-existent ID usually returns 404

        print(
            "\n--- Simulating a timeout (requires server to be unresponsive or very short timeout) ---"
        )
        # This is harder to test reliably without controlling the server or network.
        # We'll use a very short timeout with a new client instance for this test.
        try:
            with SpaceXAPIClientSync(
                timeout=0.001
            ) as timeout_client:  # Extremely short timeout
                timeout_client.get_launches()
            print("This should not be printed if the timeout exception was raised.")
        except ExternalAPIException as e:
            print(f"Correctly handled timeout. Error: {e}")
            assert isinstance(e.original_exception, httpx.TimeoutException)


if __name__ == "__main__":
    # Combine the ExternalAPIException definition with the rest of the code
    # if you are running this as a single file.
    main()
