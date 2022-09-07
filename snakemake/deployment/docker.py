__author__ = "Johannes Köster"
__copyright__ = "Copyright 2022, Johannes Köster"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

import subprocess
import shutil
import os
import hashlib
from distutils.version import LooseVersion

import snakemake
from snakemake.deployment.conda import Conda
from snakemake.common import (
    is_local_file,
    parse_uri,
    lazy_property,
    SNAKEMAKE_SEARCHPATH,
)
from snakemake.exceptions import WorkflowError
from snakemake.logging import logger


SNAKEMAKE_MOUNTPOINT = "/mnt/snakemake"


class Image:
    def __init__(self, url, dag, is_containerized):
        if " " in url:
            raise WorkflowError(
                "Invalid docker image URL containing " "whitespace."
            )

        self.singularity = Docker()

        self.url = url
        self._img_dir = dag.workflow.persistence.container_img_path
        self.is_containerized = is_containerized

    @property
    def is_local(self):
        return is_local_file(self.url)

    @lazy_property
    def hash(self):
        md5hash = hashlib.md5()
        md5hash.update(self.url.encode())
        return md5hash.hexdigest()

    def pull(self, dryrun=False):
        self.singularity.check()
        if self.is_local:
            return
        if dryrun:
            logger.info("Docker image {} will be pulled.".format(self.url))
            return
        logger.debug("Docker image location: {}".format(self.path))
        if not os.path.exists(self.path):
            logger.info("Pulling Docker image {}.".format(self.url))
            try:
                cmd = [
                        "nerdctl",
                        "pull",
                        self.url,
                    ]
                logger.debug(cmd)
                p = subprocess.check_output(
                    cmd,
                    cwd=self._img_dir,
                    stderr=subprocess.STDOUT,
                )
            except subprocess.CalledProcessError as e:
                raise WorkflowError(
                    "Failed to pull Docker image "
                    "from {}:\n{}".format(self.url, e.stdout.decode())
                )

    @property
    def path(self):
        if self.is_local:
            return parse_uri(self.url).uri_path
        return os.path.join(self._img_dir, self.hash) + ".simg"

    def __hash__(self):
        return hash(self.hash)

    def __eq__(self, other):
        return self.url == other.url


def shellcmd(
    img_path,
    cmd,
    args="",
    quiet=False,
    envvars=None,
    shell_executable=None,
    container_workdir="None",
    is_python_script=False,
):  

    """Execute shell command inside singularity container given optional args
    and environment variables to be passed."""

    logger.debug(f"args: {args}")

    if envvars:
        envvars = " ".join(
            "DOCKERENV_{}={}".format(k, v) for k, v in envvars.items()
        )
    else:
        envvars = ""

    if shell_executable is None:
        shell_executable = "sh"
    else:
        # Ensure to just use the name of the executable, not a path,
        # because we cannot be sure where it is located in the container.
        shell_executable = os.path.split(shell_executable)[-1]

    docker_cmd = "nerdctl"

    if is_python_script:
        # mount host snakemake module into container
        args += " --bind {}:{}".format(SNAKEMAKE_SEARCHPATH, SNAKEMAKE_MOUNTPOINT)

    if container_workdir:
        logger.debug(container_workdir)
        args += " -w {}".format(container_workdir)
        args += " -v{}:/data".format(os.getcwd())
    else:
        logger.debug("No container workdir set.")

    cmd = "{} {} run --rm  {} {} {} -c '{}'".format(
        envvars,
        docker_cmd,
        args,
        img_path,
        shell_executable,
        cmd.replace("'", r"'\''"),
    )
    logger.debug(cmd)
    return cmd


class Docker:
    instance = None

    def __new__(cls):
        if cls.instance is not None:
            return cls.instance
        else:
            inst = super().__new__(cls)
            cls.instance = inst
            return inst

    def __init__(self):
        self.checked = False
        self._version = None

    @property
    def version(self):
        assert (
            self._version is not None
        ), "bug: Docker version accessed before check() has been called"
        return self._version

    def check(self):
        if not self.checked:
            if not shutil.which("nerdctl"):
                raise WorkflowError(
                    "The Docker command has to be "
                    "available in order to use Docker "
                    "integration."
                )
            try:
                v = subprocess.check_output(
                    ["nerdctl", "--version"], stderr=subprocess.PIPE
                ).decode()
            except subprocess.CalledProcessError as e:
                raise WorkflowError(
                    "Failed to get Docker version:\n{}".format(e.stderr.decode())
                )
            if v.startswith("apptainer"):
                v = v.rsplit(" ", 1)[-1]
                if not LooseVersion(v) >= LooseVersion("1.0.0"):
                    raise WorkflowError("Minimum apptainer version is 1.0.0.")
            else:
                v = v.rsplit(" ", 1)[-1]
                if v.startswith("v"):
                    v = v[1:]
                if not LooseVersion(v) >= LooseVersion("0.22.2"):
                    raise WorkflowError("Minimum Docker version is 0.22.2.")
            self._version = v
