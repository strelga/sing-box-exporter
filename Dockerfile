# ── Build stage ──────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /sing-box-exporter .

# ── Runtime stage ───────────────────────────────────────────
FROM alpine:3.20

RUN apk --no-cache add ca-certificates

COPY --from=builder /sing-box-exporter /usr/local/bin/sing-box-exporter

EXPOSE 9120

ENTRYPOINT ["sing-box-exporter"]
