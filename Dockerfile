FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
COPY internal/ ./internal/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o orchestrator .

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/orchestrator .
EXPOSE 1080
CMD ["./orchestrator"]
