# Use Ubuntu as the base image
FROM ubuntu:latest

# Set the working directory inside the container
WORKDIR /app

# Install necessary packages
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    grep \
    coreutils \
    && rm -rf /var/lib/apt/lists/*

# Copy the count.sh script into the container
COPY count.sh /app/count.sh

# Make the script executable
RUN chmod +x /app/count.sh

# Set the entrypoint to run the script
ENTRYPOINT ["bash", "/app/count.sh"]
