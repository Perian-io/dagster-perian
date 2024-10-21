import shutil
import subprocess
import sys
from pathlib import Path

import docker
from dagster import op, In
from perian import ProviderQueryInput, CreateJobRequest, InstanceTypeQueryInput, CpuQueryInput, MemoryQueryInput, Size, \
    AcceleratorQueryInput, Name, RegionQueryInput, DockerRunParameters, DockerRegistryCredentials

from dagster_perian.resources import PerianJobMetadata


@op(required_resource_keys={"perian_job_manager"})
def create_perian_job(context):
    config = context.op_config

    image_name = config.get("image_name")
    tag = config.get("tag", "latest")  # Default to "latest" if not specified
    docker_username = config.get("docker_username")
    docker_password = config.get("docker_password")
    docker_repository = config.get("docker_repository")


    job_request = CreateJobRequest(
        requirements=InstanceTypeQueryInput(
            cpu=CpuQueryInput(cores=config.get("cores")),
            ram=MemoryQueryInput(size=Size(config.get("memory"))),
            accelerator=AcceleratorQueryInput(
                no=config.get("accelerators"),
                name=Name(config.get("accelerator_type")),
            ),
            region=RegionQueryInput(location=config.get("country_code")),
            provider=ProviderQueryInput(name_short=config.get("provider")),
        ),
        docker_run_parameters=DockerRunParameters(
            image_name=image_name,
            image_tag=tag,
        ),
        docker_registry_credentials=DockerRegistryCredentials(
            url=docker_repository,
            username=docker_username,
            password=docker_password
        ),
        auto_failover_instance_type=True,
    )
    job_metadata = context.resources.perian_job_manager.create_job(job_request)
    return job_metadata


@op(
    required_resource_keys={"perian_job_manager"},
    ins={"job_metadata": In(PerianJobMetadata)},
)
def get_perian_job(context, job_metadata):
    job = context.resources.perian_job_manager.get_job_status(job_metadata.job_id)
    context.log.info(f"Perian Job {job_metadata.job_id} status: {job.status}")
    context.log.info(f"Perian Job {job_metadata.job_id} logs: {job.logs}")
    return job


@op(
    required_resource_keys={"perian_job_manager"},
    ins={"job_metadata": In(PerianJobMetadata)},
)
def cancel_perian_job(context, job_metadata):
    context.resources.perian_job_manager.cancel_job(job_metadata.job_id)
    context.log.info(f"Cancelled Perian Job {job_metadata.job_id}")

@op(required_resource_keys={"perian_job_manager"})
def containerize_codebase(context):
    config = context.op_config
    path = config.get("path")
    dependency_file_path = config.get("dependency_file")
    python_version = config.get("python_version")
    image_name = config.get("image_name")
    tag = config.get("tag", "latest")
    docker_username = config.get("docker_username")
    docker_password = config.get("docker_password")
    docker_repository = config.get("docker_repository")
    command = config.get("command")
    parameters = config.get("parameters")


    directory = Path(path)
    if not directory.is_dir():
        raise ValueError(f"Provided path '{path}' is not a valid directory.")

    dockerfile_path = directory / "Dockerfile"

    if not dependency_file_path:
        try:
            _get_environment_packages(context, directory)
        except subprocess.CalledProcessError as e:
            context.log.error(f"Error generating requirements.txt: {e}")
            return
    else:
        try:
            _copy_requirements_file(context, dependency_file_path, path)
        except FileNotFoundError as e:
            context.log.error(f"The specified dependency file '{dependency_file_path}' does not exist.")
            return
        except Exception as e:
            context.log.error(f"Error copying requirements.txt: {e}")
            return


    _create_docker_file(context, dockerfile_path, python_version, command, parameters)

    client = docker.from_env()

    try:
        _build_docker_image(context, client, path, image_name, tag)
    except docker.errors.BuildError as e:
        context.log.error(f"Error building image: {e}")
        return

    context.log.info("Logging in to Docker Hub...")
    try:
        client.login(username=docker_username, password=docker_password)
        context.log.info("Successfully logged in to Docker Hub.")
    except docker.errors.APIError as e:
        context.log.error(f"Error logging in to Docker Hub: {e}")
        return

    try:
        repository_name = _tag_image_with_repository(context, client, docker_username, image_name, tag)
    except docker.errors.ImageNotFound as e:
        context.log.error(f"Error finding image to tag: {e}")
        return

    try:
        _push_image_to_repository(context, client, repository_name, tag)
    except docker.errors.APIError as e:
        context.log.error(f"Error pushing image: {e}")

def _get_environment_packages(context, directory):
    requirements_path = directory / "requirements.txt"
    with open(requirements_path, "w") as requirements_file:
        # Run pip freeze and capture the output
        installed_packages = subprocess.check_output(
            [sys.executable, "-m", "pip", "freeze"],
            universal_newlines=True
        )
        requirements_file.write(installed_packages)
        context.log.info(f"Created requirements.txt at {requirements_path}")


def _copy_requirements_file(context, dependency_file_path, path):
    shutil.copy(dependency_file_path, path)
    context.log.info(f"Copied requirements.txt from '{dependency_file_path}' to '{path}'")


def _create_docker_file(context, dockerfile_path, python_version, command, parameters):
    with open(dockerfile_path, "w") as dockerfile:
        dockerfile.write(f"""
        # Use a basic Python image
        FROM python:{python_version}

        # Set working directory inside the container
        WORKDIR /app

        # Copy contents from host to the container
        COPY . /app

        # Install dependencies
        RUN pip install --no-cache-dir -r requirements.txt || echo "No requirements.txt file found"

        # Set default command
        CMD ["{command}", "{parameters}"]
        """)

    context.log.info(f"Temporary Dockerfile created at {dockerfile_path}")


def _build_docker_image(context, client, path, image_name, tag):
    context.log.info(f"Building Docker image '{image_name}:{tag}' from {path}...")
    image, build_logs = client.images.build(
        path=path,
        tag=f"{image_name}:{tag}",
        rm=True,
    )
    for log in build_logs:
        context.log.info(log.get('stream', '').strip())
    context.log.info(f"Successfully built image '{image_name}:{tag}'")


def _tag_image_with_repository(context, client, docker_username, image_name, tag):
    repository_name = f"{docker_username}/{image_name}"
    context.log.info(f"Tagging image '{image_name}:{tag}' as '{repository_name}:{tag}'...")
    client.images.get(f"{image_name}:{tag}").tag(repository_name, tag)
    context.log.info(f"Image tagged as '{repository_name}:{tag}'")
    return repository_name

def _push_image_to_repository(context, client, repository_name, tag):
    context.log.info(f"Pushing image '{repository_name}:{tag}' to Docker Hub...")
    push_logs = client.images.push(repository_name, tag=tag, stream=True, decode=True)
    for log in push_logs:
        context.log.info(log.get('status', '').strip())  # Log push output
    context.log.info(f"Successfully pushed image '{repository_name}:{tag}'")