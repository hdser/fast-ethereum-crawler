FROM debian:bullseye-slim

# Install required dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    libpcre3 \
    libpcre3-dev \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the project files
COPY . .

# Build the project
RUN make -j4 update && \
    make -j4

# Create a directory for results
RUN mkdir -p /app/results

# Command to run the crawler
CMD ["./run.sh"]