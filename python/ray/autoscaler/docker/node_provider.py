import copy
import filelock
import logging
import os
import threading
from types import ModuleType
from typing import Any, Dict, List, Optional

import docker
import yaml

from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME
from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler.node_provider import NodeProvider
from . import client
from .command_runner import DockerLocalCommandRunner

logger = logging.getLogger(__name__)
log_prefix = "DockerLocalNodeProvider: "


def container_status(node_id):
    try:
        container = client().containers.get(node_id)
        return container.status
    except docker.errors.NotFound:
        return "not found"


def stop_container(node_id):
    try:
        container = client().containers.get(node_id)
        container.stop()
    except docker.errors.NotFound:
        logger.warn(log_prefix + "Tried to stop container with name"
                    f"{node_id}, but no container with that name was found.")


def get_docker_run_kwargs(node_config, cluster_name):
    """Converts node_config into a dict of keyword arguments to be passed to
    docker run in DockerLocalNodeProvider.create_node().

    Prepares "ray-cluster-name" label for the container.
    Converts generate_name field of node_config into a container name by
    appending a UUID.
    """
    kwargs = copy.deepcopy(node_config)
    return kwargs


TAG_PATH = "/tmp/ray_docker_local"


class DockerLocalNodeTags():
    """Helper class used by DockerLocalNodeProvider to set and get node tags.

    Maintains a temp file with {container name: tags} mapping in yaml format.
    """

    def __init__(self, cluster_name):
        if not os.path.exists(TAG_PATH):
            os.mkdir(TAG_PATH)
        self.tag_path = os.path.join(TAG_PATH, f"cluster-{cluster_name}.tags")
        lock_path = os.path.join(TAG_PATH, f"cluster-{cluster_name}.lock")

        # Locks file operations between threads in a single process.
        self.lock = threading.Lock()
        # Locks file operations between processes.
        self.file_lock = filelock.FileLock(lock_path)

        with self.lock, self.file_lock:
            if not os.path.exists(self.tag_path):
                self._write({})

    def get(self, node_id):
        with self.lock, self.file_lock:
            return self._read()[node_id]

    def set(self, node_id, tags):
        with self.lock, self.file_lock:
            all_tags = self._read()
            if node_id not in all_tags:
                all_tags[node_id] = {}
            all_tags[node_id].update(tags)
            self._write(all_tags)

    def delete(self, node_id):
        with self.lock, self.file_lock:
            all_tags = self._read()
            try:
                all_tags.pop(node_id)
            except KeyError:
                logger.warning(
                    log_prefix + "Tried to delete tags of node with "
                    f"id {node_id}, but the node's tags are already gone.")
            self._write(all_tags)

    def _read(self):
        with open(self.tag_path, "r") as tag_file:
            return yaml.safe_load(tag_file)

    def _write(self, all_tags):
        with open(self.tag_path, "w") as tag_file:
            yaml.dump(all_tags, tag_file, default_flow_style=False)


class DockerLocalNodeProvider(NodeProvider):
    """NodeProvider for local Ray experiments and tests.

    Provides a Docker container on local machine for each Ray node.
    """

    def __init__(self, provider_config: Dict[str, Any],
                 cluster_name: str) -> None:
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.tag_manager = DockerLocalNodeTags(cluster_name)

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return a list of node ids filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to nodes() to
        serve single-node queries (e.g. is_running(node_id)). This means that
        nodes() must be called again to refresh results.

        Examples:
            >>> provider.non_terminated_nodes({TAG_RAY_NODE_KIND: "worker"})
            ["node-1", "node-2"]
        """
        filters = {"label": f"{TAG_RAY_CLUSTER_NAME}={self.cluster_name}"}
        all_node_ids = [
            container.name
            for container in client().containers.list(filters=filters)
        ]
        return [
            node_id for node_id in all_node_ids
            if tag_filters.items() <= self.node_tags(node_id).items()
        ]

    def is_running(self, node_id: str) -> bool:
        """Return whether the specified node is running."""
        return container_status(node_id) == "running"

    def is_terminated(self, node_id: str) -> bool:
        """Return whether the specified node is terminated."""
        return container_status(node_id) != "running"

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict)."""
        return self.tag_manager.get(node_id)

    def internal_ip(self, node_id: str) -> str:
        """Returns the internal ip (Ray ip) of the given node."""
        container_info = client().api.inspect_container(node_id)
        return container_info["NetworkSettings"]["Networks"]["bridge"][
            "IPAddress"]

    def create_node(self, node_config: Dict[str, Any], tags: Dict[str, str],
                    count: int) -> None:
        """Creates a number of nodes within the namespace."""

        # Add cluster_name label to container run arguments.
        docker_run_kwargs = copy.deepcopy(node_config)
        cluster_label = {TAG_RAY_CLUSTER_NAME: self.cluster_name}
        docker_run_kwargs.update({"labels": cluster_label})

        for _ in range(count):
            container = client().containers.run(**docker_run_kwargs)
            self.set_node_tags(container.name, tags)
            logger.info(log_prefix +
                        f"Created node with id '{container.name}'")

    def set_node_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        """Sets the tag values (string dict) for the specified node."""
        self.tag_manager.set(node_id, tags)

    def terminate_node(self, node_id: str) -> None:
        """Terminates the specified node."""
        logger.info(log_prefix + "NodeProvider: "
                    "{}: Terminating node".format(node_id))
        stop_container(node_id)
        self.tag_manager.delete(node_id)

    def terminate_nodes(self, node_ids: List[str]) -> None:
        """Terminates a set of nodes. May be overridden with a batch method."""
        for node_id in node_ids:
            self.terminate_node(node_id)

    @staticmethod
    def bootstrap_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the cluster config by adding env defaults if needed."""
        return cluster_config

    def get_command_runner(self,
                           log_prefix: str,
                           node_id: str,
                           auth_config: Dict[str, Any],
                           cluster_name: str,
                           process_runner: ModuleType,
                           use_internal_ip: bool,
                           docker_config: Optional[Dict[str, Any]] = None
                           ) -> CommandRunnerInterface:
        return DockerLocalCommandRunner(log_prefix, node_id)

    def prepare_for_head_node(
            self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Returns a new cluster config with custom configs for head node."""
        return cluster_config

    @staticmethod
    def fillout_available_node_types_resources(
            cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Fills out missing "resources" field for available_node_types."""
        return cluster_config
