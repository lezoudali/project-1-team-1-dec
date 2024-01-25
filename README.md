# Team 1 - Pipeline to serve weather forecast data

## Datasets
| Source name | Source type | Source documentation |
| - | - | - |
| AccuWeather Location API | REST API | [Documentation](https://developer.accuweather.com/accuweather-locations-api/apis) |
| AccuWeather Forecast API | REST API | [Documentation](https://developer.accuweather.com/accuweather-forecast-api/apis) |


## Techniques Applied
- The raw data was retrieved from the AccuWeather Forecast API via full extract pattern.
- Once the raw data is stored in-memory, some basic transformations such as filtering and renaming are applied.
- The data is then loaded to a Staging table via upsert pattern.
- Once the data is in Staging, it is extract once again via incremental pattern to calculate some metrics. The metrics calculation are stores in Jinja templates.
- Finally, after the metrics have been calculated, the data is loaded to a Serving table via upsert pattern.


## Local Setup

### Account Setup
- First, it is necessary to create an account in the AccuWeather API in order to get an API key. The key is needed to access all endpoints.
- After you have created an account, send a request to the AccuWeather Location API to get a location key for your desired location. The location key is used in other endpoints to retrieve weather data. Note that this step is not part of the script and should be performed separately.
- Once you have retrieved the location key for your desired location, please update the `etl_project/pipelines/accuweather.yaml` file accordingly.

### Local run
- Create a `.env` file following the template provided in the `.env.sample` file
- Go to the folder directory and run the following command:
```
docker compose up
```
