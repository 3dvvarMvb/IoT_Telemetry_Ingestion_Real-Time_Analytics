FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download || true

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/ingest-service .

FROM alpine:3.20
RUN adduser -D -u 1000 appuser
USER appuser

WORKDIR /app
COPY --from=builder /app/ingest-service /app/ingest-service

EXPOSE 8082

HEALTHCHECK --interval=5s --timeout=3s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:8082/health || exit 1

ENTRYPOINT ["/app/ingest-service"]
