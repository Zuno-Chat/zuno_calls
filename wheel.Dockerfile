# syntax=docker/dockerfile:1
# Carrier image for the built wheel: FROM scratch, so it holds /*.whl and
# nothing else. The docker repo's synapse/Dockerfile installs from it. The
# build stage runs on the build host, so every platform variant is the same
# pure-Python wheel with no emulation.
FROM --platform=$BUILDPLATFORM python:3.13-slim AS build
COPY pyproject.toml README.md LICENSE /src/
COPY src /src/src
RUN pip wheel --no-cache-dir --no-deps -w /dist /src

FROM scratch
COPY --from=build /dist/ /
