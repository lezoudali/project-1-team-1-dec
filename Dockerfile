FROM python:3.9.9-slim

ENV API_KEY=""
ENV API_KEY=""
ENV POSTGRES_HOST=""
ENV POSTGRES_PORT=""
ENV POSTGRES_DB=""
ENV POSTGRES_USER=""
ENV POSTGRES_PASSWORD=""


WORKDIR /app

RUN pip install --upgrade pip


COPY . .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "etl_project.pipelines.accuweather"]