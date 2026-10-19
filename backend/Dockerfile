FROM golang:1.25.2-alpine AS builder

WORKDIR /src/backend

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/im-backend ./cmd/server

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /out/im-backend /app/im-backend
COPY --from=builder /src/backend/configs /app/configs

ENV CONFIG_PATH=/app/configs/config.yaml

EXPOSE 8081 50052

ENTRYPOINT ["/app/im-backend"]
