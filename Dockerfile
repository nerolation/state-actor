# Build stage
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o state-actor .

# Runtime stage
FROM alpine:3.19

LABEL org.opencontainers.image.title="State Actor"
LABEL org.opencontainers.image.description="High-performance Ethereum state generator"
LABEL org.opencontainers.image.source="https://github.com/nerolation/state-actor"
LABEL org.opencontainers.image.licenses="MIT"

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/state-actor /usr/local/bin/

# Default output directory
VOLUME /output

ENTRYPOINT ["state-actor"]
CMD ["--help"]
