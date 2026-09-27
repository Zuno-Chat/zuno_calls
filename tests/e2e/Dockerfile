# Throwaway image for tests/e2e: the module installed into the deployed Synapse.
FROM ghcr.io/element-hq/synapse:v1.161.0
COPY pyproject.toml README.md /src/
COPY src /src/src
RUN pip install --no-cache-dir /src && python -c "import zuno_calls"
