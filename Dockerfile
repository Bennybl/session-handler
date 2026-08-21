FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/server ./cmd/server && \
    CGO_ENABLED=0 go build -trimpath -o /out/migrate ./cmd/migrate

FROM alpine:3.22

RUN addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=build /out/server /out/migrate ./
USER app
ENTRYPOINT ["/app/server"]
