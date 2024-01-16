from dotenv import load_dotenv
import requests


class AccuWeatherApiClient:
    """
    Client for getting data from AccuWeather API.
    """
    def __init__(self, api_key: str):
        self.base_url = "https://dataservice.accuweather.com"
        if api_key is None:
            raise Exception("API key cannot be set to None.")
        self.api_key = api_key

    def get_forecast(
            self, location_key: int, forecast_days: int
    ) -> list[dict]:
        """Extract forecast data from AccuWeather API.

        Args:
            location_key: provide an integer based on your desired location. The Locations 
                API can be used to obtain the location key for your desired location.
            forecast)days: provide an integer based on the desired number of forecasted days.
                Possible values are 1, 5, 10, and 15.
        
        Returns:
            A list of dictionaries with forecast data.
        
        Raises:
            Exception when it is not possible to extract data from the API.
        """
        forecast_url = f"{self.base_url}/forecasts/v1/daily/{forecast_days}day/{location_key}"
        params = {
            "apikey": self.api_key,
            "language": "en-us",
            "details": "true",
            "metric": "true"
        }
        response = requests.get(url=forecast_url, params=params)
        if response.status_code == 200 and response.json().get("DailyForecasts") is not None:
            return response.json().get("DailyForecasts")
        else:
            raise Exception(
                f"Failes to extract data from AccuWeather API. Status Code: {response.status_code}. Response: {response.text}"
            )
