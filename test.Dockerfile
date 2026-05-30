# syntax=docker/dockerfile:1
#
# test.Dockerfile — builds the project (frontend assets + Go code) and runs the
# test suite inside a container. Used by the `tests` service in
# docker-compose.yml and by CI; nothing on the host besides Docker is required.
# (Named test.Dockerfile, not Dockerfile.test, because .gitignore ignores *.test.)

# Stage 1: build the frontend assets that the `ui` package embeds via
# //go:embed frontend/dist. Without these the Go build cannot compile the ui
# package (and therefore the whole module).
FROM node:20-bookworm AS frontend
WORKDIR /app/ui/frontend
COPY ui/frontend/package*.json ./
RUN npm install
COPY ui/frontend/ ./
RUN npm run build

# Stage 2: the test runner. The Debian-based golang image ships gcc, which both
# the race detector and the CGO sqlite driver (mattn/go-sqlite3) require.
FROM golang:1.25-bookworm AS test
ENV CGO_ENABLED=1
WORKDIR /src

# Cache module downloads on their own layer.
COPY go.mod go.sum ./
RUN go mod download

# Source, then overlay the freshly built frontend so //go:embed resolves.
COPY . .
COPY --from=frontend /app/ui/frontend/dist ./ui/frontend/dist

# The entrypoint runs `go test` against one or more backends (sqlite|postgres|
# mysql|all). Invoked via bash so the script needs no executable bit.
ENTRYPOINT ["bash", "scripts/docker-test-entrypoint.sh"]
CMD ["sqlite"]
