# Imagem única para os dois modos (spec 001, seção 9.3).
# O ENTRYPOINT é o binário; o modo vem dos args:
#   docker run audit:latest api
#   docker run audit:latest migrate

FROM golang:1.26.1-alpine AS build

WORKDIR /src

# Camada de dependências separada: só invalida quando go.mod/go.sum mudam.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/audit \
    ./cmd

FROM gcr.io/distroless/static-debian12:nonroot

ARG VERSION=dev

COPY --from=build /out/audit /audit

ENV AUDIT_VERSION=${VERSION} \
    HTTP_ADDR=:8080

EXPOSE 8080
USER nonroot:nonroot

ENTRYPOINT ["/audit"]
# Sem args o binário imprime o help e sai com código 2.
CMD ["api"]
