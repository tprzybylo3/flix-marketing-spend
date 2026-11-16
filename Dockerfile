# Image that runs the pipeline: installs the locked dependencies with uv, then runs dbt build.
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.13.8 /uv /bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked
COPY . .
RUN mkdir -p output
ENV DBT_PROFILES_DIR=/app PATH="/app/.venv/bin:$PATH"
CMD ["dbt", "build"]
