FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir uv

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY relay ./relay

# The build context has no .git, so pin the version from the release tag;
# untagged builds fall back to the fallback_version in pyproject.toml.
ARG SETUPTOOLS_SCM_PRETEND_VERSION
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

# The image ships the `aws` extra by default (D10).
RUN uv pip install --system --no-cache '.[aws]'

# non-root (D28)
RUN useradd -m relay
USER relay

ENV RELAY_CONFIG=/config/config.yaml
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3)"

ENTRYPOINT ["relay"]
CMD ["--config", "/config/config.yaml"]
