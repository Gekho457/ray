import docker

_client = None


def client():
    global _client
    if _client is None:
        _client = docker.from_env()
    return _client
