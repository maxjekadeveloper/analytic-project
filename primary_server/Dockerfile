FROM golang:1.26 AS builder

WORKDIR /src

COPY server/go.mod server/go.sum ./server/

WORKDIR /src/server

RUN go mod download

COPY server/ .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/server .


FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /out/server .

COPY frontend /frontend

CMD ["./server"]