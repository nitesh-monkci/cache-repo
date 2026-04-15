FROM golang:1.26-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -ldflags="-s -w" -o /app/cache-server ./cmd/cache-server


FROM alpine:3.23

RUN apk add --no-cache ca-certificates && \
    addgroup -S cache && adduser -S cache -G cache

COPY --from=builder /app/cache-server /app/cache-server

USER cache

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget -qO- --no-check-certificate https://localhost/health || exit 1

ENTRYPOINT ["/app/cache-server"]
