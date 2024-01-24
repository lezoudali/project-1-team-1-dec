from dotenv import load_dotenv
import os
import yaml
from pathlib import Path
from sqlalchemy import (
    Table,
    Column,
    MetaData,
    String,
    Integer,
    Float,
    Boolean,
    DateTime,
    Date,
)
from etl_project.connectors.accuweather import AccuWeatherApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assets.accuweather import (
    extract_forecast_weather,
    extract_current_weather,
    raw_data_transform,
    raw_current_conditions_transform,
    staging_upsert_load,
    uv_index_category
)
from etl_project.assets.transform_load import transform_load
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus

if __name__ == "__main__":
    # load environment variables
    load_dotenv()

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f"Missing {yaml_file_path} file.")

    postgresql_client = PostgreSqlClient(
        server_name=os.environ.get("POSTGRES_HOST"),
        database_name=os.environ.get("POSTGRES_DB"),
        username=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        port=os.environ.get("POSTGRES_PORT"),
    )

    config = pipeline_config.get("config")
    pipeline_name = pipeline_config.get("name")
    pipeline_logging = PipelineLogging(pipeline_name, config.get("log_folder_path"))
    metadata_logger = MetaDataLogging(
        pipeline_name,
        postgresql_client,
        config,
        f"{pipeline_name}_pipeline_logs",
    )

    metadata_logger.log()
    try:
        # extract raw data
        pipeline_logging.logger.info("Extracting raw data from AccuWeather")
        accuweather_client = AccuWeatherApiClient(api_key=os.environ.get("API_KEY"))
        df_forecast = extract_forecast_weather(
            accuweather_client=accuweather_client,
            location_key=config.get("location_key"),
            forecast_days=config.get("forecast_days"),
        )
        df_current_conditions = extract_current_weather(
            accuweather_client = accuweather_client, 
            location_name = config.get("location_name")
            )

        # transform raw data
        pipeline_logging.logger.info("Transforming raw data into dataframe")
        df_clean_forecast = raw_data_transform(
            df_forecast=df_forecast,
            location_key=config.get("location_key")
            )
        df_clean_current_conditions = raw_current_conditions_transform(
            df_current_conditions = df_current_conditions, 
            uv_index_category_func = uv_index_category
            )

        # load data to staging
        pipeline_logging.logger.info("Loading data into staging DB table")

        staging_metadata = MetaData()
        staging_table = Table(
            config.get("staging_table_name"),
            staging_metadata,
            Column("date", Date, primary_key=True),
            Column("location_key", Integer, primary_key=True),
            Column("sunrise_time", DateTime),
            Column("sunset_time", DateTime),
            Column("moonrise_time", DateTime),
            Column("moonset_time", DateTime),
            Column("moon_phase", String),
            Column("minimum_temperature_value", Float),
            Column("minimum_temperature_unit", String),
            Column("maximum_temperature_value", Float),
            Column("maximum_temperature_unit", String),
            Column("minimum_real_feel_temperature_value", Float),
            Column("minimum_real_feel_temperature_unit", String),
            Column("maximum_real_feel_temperature_value", Float),
            Column("maximum_real_feel_temperature_unit", String),
            Column("day_has_precipitation", Boolean),
            Column("day_precipitation_probability", Integer),
            Column("day_thunderstorm_probability", Integer),
            Column("day_rain_probability", Integer),
            Column("day_snow_probability", Integer),
            Column("day_ice_probability", Integer),
            Column("night_has_precipitation", Boolean),
            Column("night_precipitation_probability", Integer),
            Column("night_thunderstorm_probability", Integer),
            Column("night_rain_probability", Integer),
            Column("night_snow_probability", Integer),
            Column("night_ice_probability", Integer),
            Column("day_wind_speed_value", Float),
            Column("day_wind_speed_unit", String),
            Column("day_wind_direction_degrees", Integer),
            Column("day_wind_direction_english_abbreviation", String),
            Column("night_wind_speed_value", Float),
            Column("night_wind_speed_unit", String),
            Column("night_wind_direction_degrees", Integer),
            Column("night_wind_direction_english_abbreviation", String),
            Column("day_percentage_cloud_cover", Integer),
            Column("night_percentage_cloud_cover", Integer),
            Column("airquality_category", String),
            Column("grass_category", String),
            Column("mold_category", String),
            Column("ragweed_category", String),
            Column("tree_category", String),
            Column("uvindex_category", String),
            Column("has_precipitation", Boolean),
            Column("time_between_sunset_and_sunrise", String),
            Column("windier_period", String),
        )

        staging_upsert_load(
            dataframe=df_clean_forecast,
            postgresql_client=postgresql_client,
            table=staging_table,
            metadata=staging_metadata,
        )

        # create serving table based off staging table
        pipeline_logging.logger.info("Creating serving DB table")
        serving_metadata = MetaData()
        serving_table = Table(
            config.get("serving_table_name"),
            serving_metadata,
            Column("date", Date, primary_key=True),
            Column("count_precipitations_next_five_days", Integer),
        )

        transform_load(
            environment_path=config.get("transform_template_path"), 
            postgresql_client=postgresql_client,
            source_table_name=config.get("staging_table_name"), 
            target_table_name=serving_table,
            metadata=serving_metadata,
        )
        #metadata_logger.log(
        #     status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs() 
        #     )
        
        staging_metadata_current_conditions = MetaData()
        staging_table_current_conditions = Table(
            config.get("staging_table_name_current_conditions"),
            staging_metadata_current_conditions,
            Column("date", Date, primary_key=True),
            Column("location_key", Integer),
            Column("location_name", String),
            Column('weather_text', String),
            Column('has_precipitation', Float),
            Column('precipitation_type', String),
            Column('precipitation_value', Float),
            Column('temperature', Float),
            Column('real_feel_temperature', Float),
            Column('relative_humidity', Float),
            Column('wind_dewpoint_direction', String),
            Column('wind_speed', Float),
            Column('uv_index', Integer),
            Column('visibility', Float),
            Column('pressure', Float),
            Column('precipition_value', Float),
            Column('uv_index_category', String)
        )
        staging_upsert_load(
            dataframe=df_clean_current_conditions,
            postgresql_client=postgresql_client,
            table=staging_table_current_conditions,
            metadata=staging_metadata_current_conditions,
        )
        
        # create serving table based off staging table
        pipeline_logging.logger.info("Creating serving DB table for current conditions")
        serving_metadata_current_conditions = MetaData()
        serving_table_current_conditions = Table(
            config.get("serving_table_name_current_conditions"),
            serving_metadata_current_conditions,
            Column("date", Date, primary_key=True),
            Column("location_key", Integer, primary_key=True),
            Column("location_name", String, primary_key=True),
            Column('uv_index_category', String),
        )
        
        transform_load(
            environment_path=config.get("transform_template_path_current_conditions"),
            postgresql_client=postgresql_client,
            source_table_name=config.get("staging_table_name_current_conditions"),
            target_table_name=serving_table_current_conditions,
            metadata=serving_metadata_current_conditions,
        )
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )
        
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )  # log error
    finally:
        pipeline_logging.logger.handlers.clear()

    # TODO: write project-plan.MD and commit and request PR
    # TODO: create logging
    # TODO: create pytest
