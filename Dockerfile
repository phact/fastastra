FROM python:3.10 AS requirements-stage

WORKDIR /tmp

# Install uv (Python package manager)
RUN pip install uv

# Copy pyproject and lockfile for export
COPY ./pyproject.toml ./uv.lock* /tmp/

# Export pinned dependencies to requirements.txt
RUN uv export --format requirements-txt --output-file requirements.txt --no-hashes

FROM python:3.10

WORKDIR /code

# Copy requirements.txt and install Python dependencies
COPY --from=requirements-stage /tmp/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY . /code/

CMD ["sh", "-c", "uvicorn examples.todo:app  --host 0.0.0.0 --port ${PORT:-${WEBSITES_PORT:-8080}}"]
