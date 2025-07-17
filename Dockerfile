# --- Stage 1: The Builder ---
# This stage uses the official Go image to build the application binary.
FROM golang:1.22-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files first to leverage Docker's layer caching.
# This layer is only rebuilt if your dependencies change.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of your application's source code
COPY . .

# Build the application, creating a statically linked binary. This is crucial
# for running in a minimal container like Alpine or scratch.
# The output binary will be located at /app/broker.
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /app/broker ./cmd/broker/main.go


# --- Stage 2: The Final Production Image ---
# This stage starts from a minimal base image (Alpine) and copies
# only the compiled binary from the builder stage.
FROM alpine:latest

# It's a good security practice to run the application as a non-root user.
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Set the user for all subsequent commands
USER appuser

# Set the working directory for the final image
WORKDIR /home/appuser

# Copy the compiled binary from the 'builder' stage
COPY --from=builder /app/broker .

# Create a directory for the persistent data (logs, segments, etc.)
# This directory can be mounted as a volume from the host.
RUN mkdir data

# Expose the ports that the broker uses for communication.
# 8080: Default API port
# 7946: Default Memberlist/Gossip port
# 8946: Default Raft port (cluster port + 1000)
EXPOSE 8080 7946 8946

# The command that will be executed when the container starts.
# The command-line flags (--node-id, --seeds, etc.) will be provided
# when running the container via `docker run` or `docker-compose`.
ENTRYPOINT ["./broker"]
