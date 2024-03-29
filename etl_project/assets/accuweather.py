from etl_project.connectors.accuweather import AccuWeatherApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData
import pandas as pd


def extract_forecast_weather(
    accuweather_client: AccuWeatherApiClient,
    location_key: int,
    forecast_days: int
) -> pd.DataFrame:
    """
    Extract forecast data from AccuWeather API.

    The forecast data is extracted from a desired location based on the location_key.
    
    The forecast_days argument define the number of forecast days returned. Possible
    values are 1, 5, 10, and 15.
    """
    data = accuweather_client.get_forecast(
        location_key=location_key,
        forecast_days=forecast_days
    )
    return pd.json_normalize(data=data)


def raw_data_transform(
    df_forecast: pd.DataFrame,
    location_key: str
) -> pd.DataFrame:
    """
    Perform transformation on dataframe returned from the extract_forecast_weather() function.
    """
    # transformation 1 -> filter columns
    columns_of_interest = [
        "Date",
        "AirAndPollen",
        "Sun.Rise",
        "Sun.Set",
        "Moon.Rise",
        "Moon.Set",
        "Moon.Phase",
        "Temperature.Minimum.Value",
        "Temperature.Minimum.Unit",
        "Temperature.Maximum.Value",
        "Temperature.Maximum.Unit",
        "RealFeelTemperature.Minimum.Value",
        "RealFeelTemperature.Minimum.Unit",
        "RealFeelTemperature.Maximum.Value",
        "RealFeelTemperature.Maximum.Unit",
        "Day.HasPrecipitation",
        "Day.PrecipitationProbability",
        "Day.ThunderstormProbability",
        "Day.RainProbability",
        "Day.SnowProbability",
        "Day.IceProbability",
        "Night.HasPrecipitation",
        "Night.PrecipitationProbability",
        "Night.ThunderstormProbability",
        "Night.RainProbability",
        "Night.SnowProbability",
        "Night.IceProbability",
        "Day.Wind.Speed.Value",
        "Day.Wind.Speed.Unit",
        "Day.Wind.Direction.Degrees",
        "Day.Wind.Direction.English",
        "Night.Wind.Speed.Value",
        "Night.Wind.Speed.Unit",
        "Night.Wind.Direction.Degrees",
        "Night.Wind.Direction.English",
        "Day.CloudCover",
        "Night.CloudCover"
    ]

    df_clean_forecast = df_forecast[columns_of_interest]

    # transformation 2 -> renaming fields
    renaming_fields_map = {
        "Date": "date",
        "Sun.Rise": "sunrise_time",
        "Sun.Set": "sunset_time",
        "Moon.Rise": "moonrise_time",
        "Moon.Set": "moonset_time",
        "Moon.Phase": "moon_phase",
        "Temperature.Minimum.Value": "minimum_temperature_value",
        "Temperature.Minimum.Unit": "minimum_temperature_unit",
        "Temperature.Maximum.Value": "maximum_temperature_value",
        "Temperature.Maximum.Unit": "maximum_temperature_unit",
        "RealFeelTemperature.Minimum.Value": "minimum_real_feel_temperature_value",
        "RealFeelTemperature.Minimum.Unit": "minimum_real_feel_temperature_unit",
        "RealFeelTemperature.Maximum.Value": "maximum_real_feel_temperature_value",
        "RealFeelTemperature.Maximum.Unit": "maximum_real_feel_temperature_unit",
        "Day.HasPrecipitation": "day_has_precipitation",
        "Day.PrecipitationProbability": "day_precipitation_probability",
        "Day.ThunderstormProbability": "day_thunderstorm_probability",
        "Day.RainProbability": "day_rain_probability",
        "Day.SnowProbability": "day_snow_probability",
        "Day.IceProbability": "day_ice_probability",
        "Night.HasPrecipitation": "night_has_precipitation",
        "Night.PrecipitationProbability": "night_precipitation_probability",
        "Night.ThunderstormProbability": "night_thunderstorm_probability",
        "Night.RainProbability": "night_rain_probability",
        "Night.SnowProbability": "night_snow_probability",
        "Night.IceProbability": "night_ice_probability",
        "Day.Wind.Speed.Value": "day_wind_speed_value",
        "Day.Wind.Speed.Unit": "day_wind_speed_unit",
        "Day.Wind.Direction.Degrees": "day_wind_direction_degrees",
        "Day.Wind.Direction.English": "day_wind_direction_english_abbreviation",
        "Night.Wind.Speed.Value": "night_wind_speed_value",
        "Night.Wind.Speed.Unit": "night_wind_speed_unit",
        "Night.Wind.Direction.Degrees": "night_wind_direction_degrees",
        "Night.Wind.Direction.English": "night_wind_direction_english_abbreviation",
        "Day.CloudCover": "day_percentage_cloud_cover",
        "Night.CloudCover": "night_percentage_cloud_cover"
    }

    df_clean_forecast = df_clean_forecast.rename(columns=renaming_fields_map)

    # transformation 3 -> convert "Date" field to date (and not timestamp)
    df_clean_forecast["date"] = pd.to_datetime(df_clean_forecast["date"]).dt.date

    # transformation 4 -> clean AirAndPollen column
    # there are 5 different type of entries in the AirAndPollen column
    for index in range(6):
        temp_df = pd.json_normalize(df_clean_forecast["AirAndPollen"].apply(lambda x: x[index]))
        # rename columns
        temp_df[f"{temp_df['Name'][0].lower()}_category"] = temp_df["Category"]
        # join with clean dataframe
        df_clean_forecast = pd.concat([df_clean_forecast, temp_df[f"{temp_df['Name'][0].lower()}_category"]], axis=1)

    # Drop original AirAndPollen column
    df_clean_forecast.drop(columns="AirAndPollen", inplace=True)

    # Add location_key as column
    df_clean_forecast["location_key"] = location_key

    # transformation 5 -> create column to show if precipitation is going to happen (night or day)
    df_clean_forecast["has_precipitation"] = df_clean_forecast["day_has_precipitation"] | df_clean_forecast["night_has_precipitation"]

    # transformation 6 -> create column to show the number of hours between sun rise and sun set
    df_clean_forecast["time_between_sunset_and_sunrise"] = (
        pd.to_datetime(df_clean_forecast["sunset_time"]) 
        - pd.to_datetime(df_clean_forecast["sunrise_time"])
    )

    # transformation 7 -> create column to show which period of the day (day or night) is windier
    df_clean_forecast.loc[df_clean_forecast["day_wind_speed_value"] >= df_clean_forecast["night_wind_speed_value"], "windier_period"] = "day"
    df_clean_forecast["windier_period"].fillna("night", inplace=True)

    return df_clean_forecast


def staging_upsert_load(
    dataframe: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData
) -> None:
    postgresql_client.upsert(
        data=dataframe.to_dict(orient="records"),
        table=table,
        metadata=metadata
    )
