FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY main.go .

RUN go mod tidy && go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o checkout .

FROM alpine:3.19

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /app/checkout .

EXPOSE 8082

CMD ["./checkout"]
