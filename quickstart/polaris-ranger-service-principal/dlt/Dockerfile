FROM python:3.14-slim@sha256:0741d101873c12ab927e6f8653feb8862b9bd58771177acb1b885b95141f91b4

WORKDIR /tests
COPY requirements.txt /tests/requirements.txt
RUN pip install --no-cache-dir -r /tests/requirements.txt

# Run pytest as a non-root user; /tests is owned by it so pytest can write
# its .pytest_cache next to the test file.
RUN useradd --uid 65532 --create-home app && chown -R app:app /tests
COPY --chown=app:app test_dlt_load_paths.py /tests/test_dlt_load_paths.py
USER 65532
CMD ["pytest", "-q", "-s", "/tests/test_dlt_load_paths.py"]
