import boto3
from typing import Optional, Tuple


class S3Manager:
    """
    A class to handle AWS S3 operations including file uploads/downloads and direct text content operations.
    """

    def __init__(self, aws_access_key: str, aws_secret_key: str, region: str = 'us-west-2'):
        """
        Initialize the S3 client.

        Args:
            aws_access_key (str): AWS access key ID
            aws_secret_key (str): AWS secret access key
            region (str): AWS region (default 'us-west-2')
        """
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

    def download_file(
            self,
            bucket_name: str,
            s3_file_path: str,
            local_file_path: str
    ) -> str:
        """
        Download a file from AWS S3.

        Args:
            bucket_name (str): S3 bucket name
            s3_file_path (str): Path/name of file in S3
            local_file_path (str): Path where to save the file locally

        Returns:
            str: Empty string if successful, error message if failed
        """
        try:
            self.s3_client.download_file(bucket_name, s3_file_path, local_file_path)
            return ''
        except Exception as e:
            return f'[AWS Download Error] {str(e)}'

    def upload_file(
            self,
            bucket_name: str,
            local_file_path: str,
            s3_file_path: str,
            extra_args: Optional[dict] = None
    ) -> str:
        """
        Upload a file to AWS S3.

        Args:
            bucket_name (str): S3 bucket name
            local_file_path (str): Path of the local file to upload
            s3_file_path (str): Destination path/name in S3
            extra_args (Optional[dict]): Optional extra arguments for upload
                (e.g., ACL, ContentType, etc.)

        Returns:
            str: Empty string if successful, error message if failed
        """
        try:
            self.s3_client.upload_file(
                local_file_path,
                bucket_name,
                s3_file_path,
                ExtraArgs=extra_args
            )
            return ''
        except Exception as e:
            return f'[AWS Upload Error] {str(e)}'

    def put_text(
            self,
            bucket_name: str,
            s3_path: str,
            content: str,
            content_type: str = 'text/plain',
            encoding: str = 'utf-8'
    ) -> str:
        """
        Put text content directly to S3.

        Args:
            bucket_name (str): S3 bucket name
            s3_path (str): Destination path/name in S3
            content (str): Text content to store
            content_type (str): Content type (default 'text/plain')
            encoding (str): Text encoding (default 'utf-8')

        Returns:
            str: Empty string if successful, error message if failed
        """
        try:
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_path,
                Body=content.encode(encoding),
                ContentType=content_type
            )
            return ''
        except Exception as e:
            return f'[AWS Put Error] {str(e)}'

    def get_text(
            self,
            bucket_name: str,
            s3_path: str,
            encoding: str = 'utf-8'
    ) -> Tuple[Optional[str], str]:
        """
        Get text content directly from S3.

        Args:
            bucket_name (str): S3 bucket name
            s3_path (str): Path/name of file in S3
            encoding (str): Text encoding (default 'utf-8')

        Returns:
            Tuple[Optional[str], str]: (content, error_message)
            - If successful: (content, "")
            - If failed: (None, error_message)
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_path)
            content = response['Body'].read().decode(encoding)
            return content, ''
        except Exception as e:
            return None, f'[AWS Get Error] {str(e)}'

    def file_exists(self, bucket_name: str, s3_path: str) -> bool:
        """
        Check if a file exists in the S3 bucket.

        Args:
            bucket_name (str): S3 bucket name
            s3_path (str): Path/name of file in S3

        Returns:
            bool: True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=s3_path)
            return True
        except:  # noqa
            return False
