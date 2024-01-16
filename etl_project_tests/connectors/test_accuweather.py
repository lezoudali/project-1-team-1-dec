from dotenv import load_dotenv
import os
from etl_project.connectors.accuweather import AccuWeatherApiClient
import pytest


@pytest.fixture
def setup():
    load_dotenv()


def test_accuweather_client_api(setup):
    accuweather_client = AccuWeatherApiClient(
        api_key=os.environ.get("API_KEY")
    )
    data = accuweather_client.get_forecast(
        location_key=60449,
        forecast_days=5
    )

    assert type(data) == list
    assert len(data) > 0
