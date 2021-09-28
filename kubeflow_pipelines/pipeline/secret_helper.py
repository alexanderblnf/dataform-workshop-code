"""
Contains helpers to manage Google Secret Manager
"""

from google.cloud import secretmanager


class SecretManagerHelper:
    # pylint: disable=too-few-public-methods
    """Wrapper around Google Secret Manager."""

    project_id: str
    client: secretmanager.SecretManagerServiceClient

    def __init__(self, project_id: str):
        """Sets project and instantiates Secret Manager client"""
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()

    def get_secret(self, secret_name: str) -> str:
        """Using the secret name, fetches the secret value

        Args:
            secret_name (str): The name of the secret as defined in
                Google Secret Manager

        Returns:
            str: The value stored in the secret
        """
        name = (
            f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        )

        response = self.client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
