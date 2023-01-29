import os

import pytest
from pytest_docker_tools import build, container, fetch

postres_password = os.getenv("TAP_POSTGRES_PASSWORD", "mypass")
postgres_image = fetch(repository="postgres:14.6")
postgres_fixture = container(
    image="{postgres_image.id}",
    name="postgres-pytest",
    environment={"POSTGRES_PASSWORD": postres_password},
    ports={"5432/tcp": "5432"},
    scope="session",
)
