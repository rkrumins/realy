PYTHON ?= python3

.PHONY: install dev-api dev-ui test build run image

install:   ## dependencies for both halves
	cd frontend && npm install
	cd backend && $(PYTHON) -m pip install -e ".[dev]"

dev-api:   ## API + live WebSocket on :8090, reloading on change
	cd backend && $(PYTHON) -m server --reload

dev-ui:    ## Vite on :5173 with hot reload, proxying /api (and the WebSocket) to :8090
	cd frontend && npm run dev

test:      ## type-check, unit tests, API tests
	cd frontend && npx tsc --noEmit && npx vitest run
	cd backend && $(PYTHON) -m pytest -q

build:     ## UI into backend/server/static (served by the backend's SPA fallback)
	cd frontend && npm run build

run: build ## the production shape locally: one process, one port
	cd backend && $(PYTHON) -m server --port 8090

image:
	docker build -t operator-console .
