FROM golang:1.25

WORKDIR /app

# Install protobuf compiler and git
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install protobuf Go plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

# Make Go installed binaries available
ENV PATH="/go/bin:${PATH}"

# Copy everything
COPY gocode/sdcc.proto .
COPY gocode/utility.go .
COPY gocode/commonComm.go .
COPY gocode/conNodeCode.go .
COPY gocode/proxyCode.go .
COPY gocode/snapshotCode.go .

# Generate go.mod inside the image if not already present
RUN if [ ! -f go.mod ]; then go mod init gocode; fi

# Print debug information
RUN ls -la /app
RUN which protoc
RUN which protoc-gen-go
RUN which protoc-gen-go-grpc

# Generate protobuf files
RUN protoc \
  --go_out=. \
  --go-grpc_out=. \
  sdcc.proto

# Clean and verify dependencies
RUN go mod tidy

# Last check
RUN ls -la /app