import logging
import subprocess
import sys
from typing import Any, Dict, Optional

from ray.autoscaler._private.command_runner import _with_environment_variables
from ray.autoscaler.command_runner import CommandRunnerInterface
from . import client

logger = logging.getLogger(__name__)


def with_bash(cmd):
    return ["/bin/bash", "-c", "--", cmd]


class DockerLocalCommandRunner(CommandRunnerInterface):
    def __init__(self, log_prefix, node_id):
        self.log_prefix = log_prefix
        self.node_id = node_id
        self._home_cached = None

    def run(self,
            cmd,
            timeout=120,
            exit_on_fail=False,
            port_forward=None,
            with_output=False,
            environment_variables=None,
            run_env="auto",
            ssh_options_override_ssh_key="",
            shutdown_after_run=False):
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
        # Refined this later, if needed.
        print(output.decode())

        if with_output:
            return output.decode().strip()

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

    def _docker_cp(self, source, target):
        logger.info(self.log_prefix + f"Copying {source} to {target}")
        cmd = with_bash(f"docker cp {source} {target}")
        try:
            subprocess.check_call(cmd)
        except Exception:
            logger.exception(self.log_prefix + "docker cp failed.")

    def _remote_path(self, path):
        return ":".join([self.node_id, self._expand_user(path)])

    def _expand_user(self, path):
        if path[0] == "~":
            return self._home + path[1:]
        else:
            return path

    @property
    def _home(self):
        if not self._home_cached:
            self._home_cached = self.run("printenv HOME", with_output=True)
        return self._home_cached

    def remote_shell_command_str(self) -> str:
        """Return the command the user can use to open a shell."""
        return f"docker exec -it {self.node_id} /bin/bash"
