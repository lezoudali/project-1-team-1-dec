FROM --platform=linux/amd64 python:3.9.9-slim


WORKDIR /app

RUN pip install --upgrade pip


COPY . .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "etl_project.pipelines.accuweather"]