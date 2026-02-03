# LazyFS Docker image for Kifa durability testing.
#
# Provides a pre-built LazyFS environment for testing true durability
# guarantees. LazyFS is a FUSE filesystem that maintains its own page
# cache, enabling simulation of power failure by dropping unsynced data.
#
# Usage:
#   docker build -t kifa-lazyfs -f docker/lazyfs/Dockerfile .
#   docker run --privileged kifa-lazyfs ./target/release/examples/crash-test --lazyfs

FROM debian:bookworm

# Install build dependencies and FUSE 3.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    g++ \
    git \
    ca-certificates \
    libfuse3-dev \
    libfuse3-3 \
    fuse3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Rust toolchain.
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --default-toolchain nightly --profile minimal

# Clone and build LazyFS.
WORKDIR /opt
RUN git clone --depth 1 https://github.com/dsrhaslab/lazyfs.git

WORKDIR /opt/lazyfs/libs/libpcache
RUN ./build.sh

WORKDIR /opt/lazyfs/lazyfs
RUN ./build.sh

# Configure FUSE to allow user mounts.
RUN echo "user_allow_other" >> /etc/fuse.conf

# Copy mount scripts to PATH.
RUN cp /opt/lazyfs/lazyfs/scripts/*.sh /usr/local/bin/ && \
    chmod +x /usr/local/bin/*.sh

# Create directories for LazyFS mount.
RUN mkdir -p /tmp/lazyfs.root /tmp/lazyfs.mnt

# Copy LazyFS configuration.
COPY docker/lazyfs/lazyfs.toml /etc/lazyfs.toml

# Copy entrypoint script.
COPY docker/lazyfs/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set working directory for Kifa.
WORKDIR /kifa

# Copy project files.
COPY . .

# Build Kifa in release mode.
RUN cargo build --release --workspace --all-targets

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["./target/release/examples/crash-test", "--lazyfs"]
