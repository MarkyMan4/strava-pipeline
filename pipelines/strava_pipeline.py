import time
from typing import Any, Dict

import dlt
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


class OAuth2ClientCredentialsStrava(OAuth2ClientCredentials):
    def build_access_token_request(self) -> Dict[str, Any]:
        return {
            "headers": {
                "Content-Type": "application/json",
            },
            "json": {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                **self.access_token_request_data,
            },
        }


@dlt.source
def strava_source() -> Any:
    auth = OAuth2ClientCredentials(
        access_token_url=dlt.secrets["sources.strava.access_token_url"],
        client_id=str(dlt.secrets["sources.strava.client_id"]),
        client_secret=dlt.secrets["sources.strava.client_secret"],
        access_token_request_data={
            "grant_type": "refresh_token",
            "refresh_token": dlt.secrets["sources.strava.refresh_token"],
        },
    )
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.strava.com/api/v3",
            "auth": auth,
        },
        # The default configuration for all resources and their endpoints
        "resource_defaults": {
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "per_page": 1000,
                },
            },
        },
        "resources": [
            {
                # TODO make this SCD
                "name": "athlete",
                "endpoint": {
                    "path": "/athlete",
                },
            },
            {
                "name": "activities",
                "primary_key": "id",
                "endpoint": {
                    "path": "/athlete/activities",
                    # api docs https://developers.strava.com/docs/reference/#api-Activities-getLoggedInAthleteActivities
                    "params": {
                        "after": {
                            "type": "incremental",
                            "cursor_path": "start_date",
                            "initial_value": "2022-01-01T00:00:00Z",
                            # query param needs to be passed as epoch
                            "convert": lambda dt: int(
                                time.mktime(time.strptime(dt, "%Y-%m-%dT%H:%M:%SZ"))
                            ),
                        }
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


def load_strava(db_file_path: str = "running.duckdb") -> None:
    pipeline = dlt.pipeline(
        pipeline_name="strava_api",
        destination=dlt.destinations.duckdb(db_file_path),
        dataset_name="strava",
    )

    load_info = pipeline.run(strava_source())
    print(load_info)
