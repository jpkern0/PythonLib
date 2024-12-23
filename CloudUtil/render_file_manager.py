import requests
from typing import Optional, Tuple, Any


class RenderFileManager:
    """
    A class to handle Render file operations through the API endpoints, with a similar
    interface to S3Manager for easy interchangeability.
    """

    def __init__(self, service_url: str, api_key: str):
        """
        Initialize the Render file manager client.

        Args:
            service_url (str): URL of your Render file manager service
            api_key (str): API key for authentication
        """
        self.base_url = service_url.rstrip('/')
        self.headers = {"X-API-Key": api_key}

    def download_file(
            self,
            disk_path: str,
            local_file_path: str
    ) -> str:
        """
        Download a file from Render disk to local filesystem.

        Args:
            disk_path (str): Path/name of file on Render disk
            local_file_path (str): Path where to save the file locally

        Returns:
            str: Empty string if successful, error message if failed
        """
        try:
            response = requests.get(
                f"{self.base_url}/files/{disk_path}",
                headers=self.headers,
                params={"data_format": "pickle"}  # Use pickle for binary data
            )
            response.raise_for_status()
            content = response.json()["data"]

            with open(local_file_path, 'wb') as f:
                f.write(bytes(content))
            return ''
        except Exception as e:
            return f'[Render Download Error] {str(e)}'

    def upload_file(
            self,
            local_file_path: str,
            disk_path: str,
    ) -> str:
        """
        Upload a file from local filesystem to Render disk.

        Args:
            local_file_path (str): Path of the local file to upload
            disk_path (str): Destination path/name on Render disk

        Returns:
            str: Empty string if successful, error message if failed
        """
        try:
            with open(local_file_path, 'rb') as f:
                content = list(f.read())  # Convert bytes to list for JSON serialization

            response = requests.put(
                f"{self.base_url}/files/{disk_path}",
                headers=self.headers,
                json={
                    "data": content,
                    "data_format": "pickle"
                }
            )
            response.raise_for_status()
            return ''
        except Exception as e:
            return f'[Render Upload Error] {str(e)}'

    def put_object(
            self,
            disk_path: str,
            data: Any,
            data_format: str = 'text'
    ) -> str:
        """
        Put any Python object directly to Render disk.

        Args:
            disk_path (str): Destination path/name on Render disk
            data: Data to store (any Python object)
            data_format (str): Format to store data in ('json', 'text', or 'pickle')

        Returns:
            str: Empty string if successful, error message if failed
        """
        try:
            response = requests.put(
                f"{self.base_url}/files/{disk_path}",
                headers=self.headers,
                json={
                    "data": data,
                    "data_format": data_format
                }
            )
            response.raise_for_status()
            return ''
        except Exception as e:
            return f'[Render Put Error] {str(e)}'

    def get_object(
            self,
            disk_path: str,
            data_format: str = 'json'
    ) -> Tuple[Optional[Any], str]:
        """
        Get any Python object directly from Render disk.

        Args:
            disk_path (str): Path/name of file on Render disk
            data_format (str): Format data was stored in ('json', 'text', or 'pickle')

        Returns:
            Tuple[Optional[Any], str]: (data, error_message)
            - If successful: (data, "")
            - If failed: (None, error_message)
        """
        try:
            response = requests.get(
                f"{self.base_url}/files/{disk_path}",
                headers=self.headers,
                params={"data_format": data_format}
            )
            response.raise_for_status()
            return response.json()["data"], ''
        except Exception as e:
            return None, f'[Render Get Error] {str(e)}'

    def file_exists(self, disk_path: str) -> bool:
        """
        Check if a file exists on the Render disk.

        Args:
            disk_path (str): Path/name of file on Render disk

        Returns:
            bool: True if file exists, False otherwise
        """
        try:
            response = requests.get(
                f"{self.base_url}/files/{disk_path}",
                headers=self.headers
            )
            return response.status_code == 200
        except:  # noqa
            return False
