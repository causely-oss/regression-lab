FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go .
RUN go mod tidy && go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o service .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/service .
EXPOSE 8098
CMD ["./service"]
