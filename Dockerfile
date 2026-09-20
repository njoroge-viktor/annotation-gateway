# Build stage
FROM golang:1.23-alpine AS build

WORKDIR /src

# Copy the module files first so dependency resolution is cached independently
# of source changes. There are no third-party dependencies, but this keeps the
# layer ordering correct if any are added later.
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /gateway ./cmd/gateway

# Runtime stage: scratch, since a static binary needs nothing else. No shell
# and no package manager means nothing to exploit if the process is breached.
FROM scratch

COPY --from=build /gateway /gateway

# Run unprivileged. The numeric id avoids needing /etc/passwd in the image.
USER 65534:65534

EXPOSE 8080
ENTRYPOINT ["/gateway"]
CMD ["-addr", ":8080"]
