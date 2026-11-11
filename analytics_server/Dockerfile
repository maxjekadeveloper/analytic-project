FROM golang:1.26 AS builder

WORKDIR /src

COPY analytics_server/go.mod analytics_server/go.sum ./analytics_server/

WORKDIR /src/analytics_server

RUN go mod download

COPY analytics_server/ .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/analytics_server .


FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /out/analytics_server .

COPY analytics_server/migrations ./migrations

CMD ["./analytics_server"]