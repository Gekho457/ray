import logging
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

from ray.autoscaler._private.command_runner import _with_environment_variables
from ray.autoscaler.command_runner import CommandRunnerInterface
from . import client

logger = logging.getLogger(__name__)


def with_bash(cmd: str) -> List[str]:
    return ["/bin/bash", "-c", "--", cmd]


class DockerLocalCommandRunner(CommandRunnerInterface):
    def __init__(self, log_prefix: str, node_id: str) -> None:
        self.log_prefix = log_prefix
        self.node_id = node_id
        self._home_cached = None  # type: Optional[str]

    def run(self,
            cmd: Optional[str] = None,
            timeout: int = 120,
            exit_on_fail: bool = False,
            port_forward: Optional[List[Tuple[int, int]]] = None,
            with_output: bool = False,
            environment_variables: Optional[Dict[str, object]] = None,
            run_env: str = "auto",
            ssh_options_override_ssh_key: str = "",
            shutdown_after_run: bool = False) -> Optional[str]:
        cmd = cmd or ""  # Appeasing mypy
        if environment_variables:
            cmd = _with_environment_variables(cmd, environment_variables)
        final_cmd = with_bash(cmd)

        logger.info(self.log_prefix + "Running \"{}\" in container {}.".format(
            cmd, self.node_id))
        container = client().containers.get(self.node_id)
        # TODO(dmitri): Probably should configure args passed to exec_run more
        # carefully. Refine this later, if needed.
        exit_code, output = container.exec_run(final_cmd)
        if exit_code != 0:
            logger.error(self.log_prefix + "Docker exec command failed. See "
                         "command output below.")
            print(output.decode())
            if exit_on_fail:
                sys.exit(1)
        # TODO(dmitri): Container stdout and stderr are printed to stdout.
        # Refine this later, if needed.
        print(output.decode())

        if with_output:
            return output.decode().strip()  # type: ignore
        else:
            return None

    def run_rsync_up(self,
                     source: str,
                     target: str,
                     options: Optional[Dict[str, Any]] = None) -> None:
        """Rsync files up to the cluster node.

        Args:
            source (str): The (local) source directory or file.
            target (str): The (remote) destination path.
        """
        target = self._remote_path(target)
        self._docker_cp(source, target)

    def run_rsync_down(self,
                       source: str,
                       target: str,
                       options: Optional[Dict[str, Any]] = None) -> None:
        """Rsync files down from the cluster node.

        Args:
            source (str): The (remote) source directory or file.
            target (str): The (local) destination path.
        """
        source = self._remote_path(source)
        self._docker_cp(source, target)

    def _docker_cp(self, source: str, target: str) -> None:
        logger.info(self.log_prefix + f"Copying {source} to {target}")
        cmd = with_bash(f"docker cp {source} {target}")
        try:
            subprocess.check_call(cmd)
        except Exception:
            logger.exception(self.log_prefix + "docker cp failed.")

    def _remote_path(self, path: str) -> str:
        return ":".join([self.node_id, self._expand_user(path)])

    def _expand_user(self, path: str) -> str:
        if path[0] == "~":
            return self._home + path[1:]
        else:
            return path

    @property
    def _home(self) -> str:
        if not self._home_cached:
            self._home_cached = self.run("printenv HOME", with_output=True)
        return self._home_cached  # type: ignore

    def remote_shell_command_str(self) -> str:
        """Return the command the user can use to open a shell."""
        return f"docker exec -it {self.node_id} /bin/bash"
