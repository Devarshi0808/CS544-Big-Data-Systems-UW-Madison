FROM ubuntu:24.04
# Using this to avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Installing the required packages
RUN apt-get update && apt-get install -y python3-pip

# Set the working directory to root
WORKDIR /

# Copy the required files directly to root
COPY inputs /inputs/
COPY partitions /partitions/
COPY wins /wins/
COPY server.py /
COPY client.py /
COPY matchdb.proto /
COPY requirements.txt /

# Change permissions for the files
RUN chmod -R +x inputs/ partitions/ wins/
RUN chmod +x server.py client.py matchdb.proto requirements.txt

# Install dependencies
RUN pip3 install -r requirements.txt --break-system-packages

# Generate the gRPC stubs
RUN python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. matchdb.proto

EXPOSE 5440

# Entrypoint
CMD ["python3", "-u", "/server.py"]
