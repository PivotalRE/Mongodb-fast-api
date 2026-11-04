# Railway builds this file: the property agent (sheet Analyze button + Claude app plug-in).
# The older MongoDB prospecting API is not run for now; its build file is Dockerfile.mongo-api.
FROM python:3.12-slim

WORKDIR /app
COPY property-agent/pyproject.toml property-agent/README.md ./
COPY property-agent/src ./src
RUN pip install --no-cache-dir .
COPY property-agent/agent_app ./agent_app

RUN useradd --create-home agent
USER agent

# Settings: ANTHROPIC_API_KEY, PIVOTAL_SERVICE_TOKEN, optional PIVOTAL_MCP_TOKEN
# (see property-agent/agent_app/service.py).
EXPOSE 8000
# Port 8000, the same as the old API, so the existing Railway domain keeps pointing at it.
CMD ["uvicorn", "--app-dir", "agent_app", "--factory", "service:app_factory", "--host", "0.0.0.0", "--port", "8000"]
