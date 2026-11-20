# One image serves the API, the live WebSocket and the UI on one port (context = project root):
#   docker build -t operator-console .
#   docker run -p 8090:8090 -v console-data:/data operator-console        -> http://localhost:8090
# Base images are overridable (registry mirrors): --build-arg NODE_IMAGE=... --build-arg PYTHON_IMAGE=...
ARG NODE_IMAGE=node:22-alpine
ARG PYTHON_IMAGE=python:3.13-slim

# ------------------------------------------------------------------------------- UI build
FROM ${NODE_IMAGE} AS ui-build
WORKDIR /ui
COPY frontend/package.json frontend/package-lock.json* frontend/.npmrc* ./
RUN --mount=type=cache,target=/root/.npm \
    if [ -f package-lock.json ]; then npm ci --no-audit --no-fund; else npm install --no-audit --no-fund; fi
COPY frontend/ ./
# tsc --noEmit runs first (npm run build), so a type error fails the image build
RUN VITE_OUT_DIR=/ui/dist npm run build

# ------------------------------------------------------------------------------- service
FROM ${PYTHON_IMAGE} AS service
ENV PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 APP_DATA_DIR=/data
WORKDIR /app
COPY backend/pyproject.toml ./
COPY backend/server ./server
RUN --mount=type=cache,target=/root/.cache/pip pip install -e .
# the built UI lands where the SPA fallback looks for it (server/static)
COPY --from=ui-build /ui/dist ./server/static
EXPOSE 8090
VOLUME ["/data"]
HEALTHCHECK --interval=10s --timeout=3s --retries=30 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8090/api/health', timeout=2)" || exit 1
CMD ["python", "-m", "server", "--host", "0.0.0.0", "--port", "8090"]
