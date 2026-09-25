FROM golang:1.26 AS builder

WORKDIR /src

COPY analytics/go.mod analytics/go.sum ./analytics/

WORKDIR /src/analytics

RUN go mod download

COPY analytics/ .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/analytics .


FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /out/analytics .

COPY analytics/migrations ./migrations

CMD ["./analytics"]