FROM golang:alpine AS builder

WORKDIR /go/src/app
COPY . .

RUN go get -d -v ./...
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /go/bin/app -v master/main.go

FROM alpine:latest

COPY --from=builder /go/bin/app /app

CMD ["/app"]