FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY main.go .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o payments-api .

FROM alpine:3.19

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/payments-api .

EXPOSE 8083

CMD ["./payments-api"]
