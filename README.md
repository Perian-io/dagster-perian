# README for Dagster Integration with Perian

## Introduction

This Dagster integration allows you to easily dockerize your codebase and execute it on the Perian platform, our serverless GPU environment. By leveraging this integration, you can streamline your workflow, enabling efficient code deployment and execution without the need for managing infrastructure.

### Features

- **Dockerization**: Automatically packages your codebase into a Docker container.
- **Serverless Execution**: Runs your Docker containers on Perian’s scalable GPU infrastructure.
- **Ease of Use**: Simple commands to get your project up and running quickly.

## Prerequisites

- Docker installed on your local machine.
- Access to the Perian platform (ensure your account is set up).
- Dagster installed in your project (follow the [Dagster installation guide](https://docs.dagster.io/getting-started/install-dagster)).


## Ops

This integration introduces the following Dagster ops:

1. **`containerize_codebase`**: 
   - Packages your codebase into a Docker container, including the installation of any necessary dependencies.
   - **Parameters**:
     - `path`: Directory of the codebase.
     - `dependency_file`: Path to a requirements file (optional).
     - `python_version`: Version of Python to use in the Docker image.
     - `image_name`: Name for the Docker image.
     - `tag`: Tag for the Docker image (default is "latest").
     - `docker_username`, `docker_password`, `docker_repository`: Credentials for Docker Hub.
     - `command`: Command to run in the container.
     - `parameters`: Parameters for the command.

2. **`create_perian_job`**: 
   - Creates a job on the Perian platform using the Docker image built from your codebase.
   - **Parameters**:
     - `image_name`: Name of the Docker image.
     - `tag`: Tag of the Docker image.
     - `docker_username`, `docker_password`, `docker_repository`: Credentials for Docker Hub.
     - `cores`, `memory`, `accelerators`, `accelerator_type`, `country_code`, `provider`: Configuration for the job instance.

3. **`get_perian_job`**: 
   - Retrieves the status and logs of a job running on the Perian platform.
   - **Input**: 
     - `job_metadata`: Metadata containing the job ID.

4. **`cancel_perian_job`**: 
   - Cancels a running job on the Perian platform.
   - **Input**: 
     - `job_metadata`: Metadata containing the job ID.

## Installation

1. Install dagster-perian package:

   ```bash
    pip install dagster-perian
    ```
   or
   ```bash
    poetry add dagster-perian
    ```


## Example

Here’s an example of how to define a Dagster job that utilizes the containerize_codebase and create_perian_job ops:


```python
from dagster import job
from perian_job.resources import perian_job_resource
from perian_job.operations import create_perian_job, containerize_codebase

@job(resource_defs={"perian_job_manager": perian_job_resource})
def perian_job_flow():
    containerize_codebase()
    create_perian_job()

```yaml
resources:
  perian_job_manager:
    config:
      api_url: "https://api.perian.cloud"
      organization: "<your-perian-organization-name>"
      token: "<your-perian-token>"

ops:
  create_perian_job:
    config:
      accelerators: 1
      accelerator_type:"A100"
      image_name: "<your-image-name>"
      tag: "<your-image-tag>"
      docker_username: "<your-docker-username>"
      docker_password: "<your-docker-password>"

  containerize_codebase:
    config:
      path: "<your-code-base-to-be-dockerized>"
      image_name: "<your-image-name>"
      tag: "<your-image-tag>"
      python_version: "<python-version-to-be-used>"
      docker_username: "<your-docker-username>"
      docker_password: "<your-docker-password>"
      command: "<running-script-or-module>"
      parameters: "<command-parameters>"


