from dataclasses import dataclass

from perian import (
    ApiClient,
    Configuration,
    CreateJobRequest,
    JobApi
)

from dagster import resource, InitResourceContext, Field


@dataclass
class PerianJobMetadata:
    job_id: str


class PerianJobManager:
    def __init__(self, api_url: str, organization: str, token: str):
        self.api_url = api_url
        self.organization = organization
        self.token = token
        self.config = Configuration(host=self.api_url)

    def _build_headers(self):
        return {
            "X-PERIAN-AUTH-ORG": self.organization,
            "Authorization": f"Bearer {self.token}",
        }

    def create_job(self, job_request: CreateJobRequest):
        with ApiClient(self.config) as api_client:
            api_instance = JobApi(api_client)
            response = api_instance.create_job(
                create_job_request=job_request,
                _headers=self._build_headers(),
            )
        return PerianJobMetadata(job_id=response.id)

    def get_job_status(self, job_id: str):
        with ApiClient(self.config) as api_client:
            api_instance = JobApi(api_client)
            response = api_instance.get_job_by_id(
                job_id=str(job_id),
                _headers=self._build_headers(),
            )
        if not response.jobs:
            raise ValueError(f"Perian job not found: {job_id}")
        return response.jobs[0]

    def cancel_job(self, job_id: str):
        with ApiClient(self.config) as api_client:
            api_instance = JobApi(api_client)
            response = api_instance.cancel_job(
                job_id=str(job_id),
                _headers=self._build_headers(),
            )
        if response.status_code != 200:
            raise ValueError(f"Failed to cancel Perian job: {response.text}")


@resource(
    config_schema={
        "api_url": Field(str, is_required=True, description="Perian API URL"),
        "organization": Field(str, is_required=True, description="Perian Organization"),
        "token": Field(str, is_required=True, description="Perian Token"),
    }
)
def perian_job_resource(context: InitResourceContext):
    return PerianJobManager(
        api_url=context.resource_config["api_url"],
        organization=context.resource_config["organization"],
        token=context.resource_config["token"],
    )
