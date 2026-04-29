# ===============================
# BUILD STAGE
# ===============================
FROM golang:1.22-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o app

# ===============================
# RUNTIME STAGE
# ===============================
FROM alpine:latest

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/app .
COPY config.yaml .
COPY migrations ./migrations

EXPOSE 8080

CMD ["./app"]