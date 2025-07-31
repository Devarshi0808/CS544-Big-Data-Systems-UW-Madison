# Start from the official lightweight Python image
FROM python:3.9-slim

# Set the default working directory inside the container
WORKDIR /app

# Copy the application files to the working directory
COPY . .

# Install required Python packages
RUN pip install --no-cache-dir grpcio==1.66.1 grpcio-tools==1.66.1 numpy protobuf==5.27.2 pyarrow==17.0.0 setuptools==75.1.0

# Make port 5440 available for communication
EXPOSE 5440

# Define the command to run the gRPC server
CMD ["python3", "server.py"]
