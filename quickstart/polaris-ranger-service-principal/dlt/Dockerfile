FROM python:3.14-slim

WORKDIR /tests
COPY requirements.txt /tests/requirements.txt
RUN pip install --no-cache-dir -r /tests/requirements.txt

COPY test_dlt_load_paths.py /tests/test_dlt_load_paths.py
CMD ["pytest", "-q", "-s", "/tests/test_dlt_load_paths.py"]

