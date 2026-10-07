# syntax=docker/dockerfile:1
# Runs the Dagster UI and scheduler for OrbitWatch: http://localhost:3000
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ORBITWATCH_DATA_DIR=/data \
    DAGSTER_HOME=/dagster_home \
    ORBITWATCH_DBT_DIR=/app/dbt
RUN useradd --create-home --uid 10001 app && mkdir -p /data /dagster_home && chown -R app /data /dagster_home
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY dbt ./dbt
RUN pip install --no-cache-dir ".[dev]" && chown -R app /app
USER app
# Parse the dbt project at build time so the image starts with a ready manifest.
RUN python -c "import orbitwatch.definitions"
VOLUME ["/data", "/dagster_home"]
EXPOSE 3000
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "orbitwatch.definitions"]
