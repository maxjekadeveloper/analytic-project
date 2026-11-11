FROM golang:1.26 AS builder

WORKDIR /src

COPY primary_server/go.mod primary_server/go.sum ./primary_server/

WORKDIR /src/primary_server

RUN go mod download

COPY primary_server/ .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/primary_server .


FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /out/primary_server .

COPY frontend /frontend

CMD ["./primary_server"]