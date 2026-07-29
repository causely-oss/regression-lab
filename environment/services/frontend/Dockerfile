FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY main.go .

RUN go mod tidy && go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o frontend .

FROM alpine:3.19

WORKDIR /app

COPY --from=builder /app/frontend .

EXPOSE 8080

CMD ["./frontend"]
