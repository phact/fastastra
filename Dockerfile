FROM python:3.10 AS requirements-stage

WORKDIR /tmp

# Install Poetry
RUN pip install poetry

COPY ./pyproject.toml ./poetry.lock* /tmp/

RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

FROM python:3.10

WORKDIR /code

# Copy requirements.txt and install Python dependencies
COPY --from=requirements-stage /tmp/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY . /code/

CMD ["sh", "-c", "uvicorn examples.todo:app  --host 0.0.0.0 --port ${PORT:-${WEBSITES_PORT:-8080}}"]
