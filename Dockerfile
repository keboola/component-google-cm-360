FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONIOENCODING utf-8

WORKDIR /code/

# Copy dependency files first (layer caching)
COPY pyproject.toml .
COPY uv.lock .

# Install dependencies into system Python (no venv in Docker)
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"

# install gcc to be able to build packages - e.g. required by regex, dateparser, also required for pandas
RUN apt-get update && apt-get install -y build-essential && uv sync --all-groups --frozen

COPY src/ src
COPY tests/ tests
COPY scripts/ scripts
COPY deploy.sh .

CMD ["python", "-u", "src/component.py"]
