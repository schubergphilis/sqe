FROM python:3.14-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6

WORKDIR /tests
COPY requirements.txt /tests/requirements.txt
RUN pip install --no-cache-dir -r /tests/requirements.txt

# Run pytest as a non-root user; /tests is owned by it so pytest can write
# its .pytest_cache next to the test file.
RUN useradd --uid 65532 --create-home app && chown -R app:app /tests
COPY --chown=app:app test_dlt_load_paths.py /tests/test_dlt_load_paths.py
USER 65532
CMD ["pytest", "-q", "-s", "/tests/test_dlt_load_paths.py"]
