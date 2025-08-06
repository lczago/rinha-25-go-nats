FROM golang:1.24 AS builder
RUN go version

WORKDIR /app
ADD . .

RUN go get -d -v ./...
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o rinha-25 .


FROM gcr.io/distroless/base-debian12:debug AS run
WORKDIR /app

COPY --from=builder /app/rinha-25 .

ENTRYPOINT [ "./rinha-25" ]