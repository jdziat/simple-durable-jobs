FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/chaostest ./cmd/chaostest

FROM alpine:3.21
RUN adduser -D -H chaostest
USER chaostest
ENTRYPOINT ["/chaostest"]
COPY --from=build /out/chaostest /chaostest
