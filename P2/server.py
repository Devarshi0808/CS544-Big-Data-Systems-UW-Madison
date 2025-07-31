import grpc
import pandas as pd
import matchdb_pb2
import matchdb_pb2_grpc
from concurrent import futures
import sys
import socket


class MatchCountServicer(matchdb_pb2_grpc.MatchCountServicer):
    def __init__(self, csv_file):
        # Load the CSV file containing match data
        self.data = pd.read_csv(csv_file)

    def GetMatchCount(self, request, context):
        # Filter the dataset based on the request
        filtered_data = self.data

        # Apply filters if fields are not empty
        if request.country:
            filtered_data = filtered_data[filtered_data['country'] == request.country]
        if request.winning_team:
            filtered_data = filtered_data[filtered_data['winning_team'] == request.winning_team]

        # Return the count of the filtered data
        num_matches = len(filtered_data)

        # Create and return the response
        return matchdb_pb2.GetMatchCountResp(num_matches=num_matches)


def get_partition_and_file():
    """
    Determine whether the server is running in wins-server-1 or wins-server-2,
    and choose the correct partition accordingly.
    """
    # Get the IP address of the current container
    my_ip = socket.gethostbyname(socket.gethostname())

    # Get the IP addresses of wins-server-1 and wins-server-2
    server1_ip = socket.gethostbyname("wins-server-1")
    server2_ip = socket.gethostbyname("wins-server-2")

    # Determine which partition this server should serve
    if my_ip == server1_ip:
        print("This server is wins-server-1, serving part_0.csv")
        return "/partitions/part_0.csv"
    elif my_ip == server2_ip:
        print("This server is wins-server-2, serving part_1.csv")
        return "/partitions/part_1.csv"
    else:
        raise RuntimeError("Could not determine partition - unknown server IP")


def serve(csv_file=None, port=5440):
    """
    Start the gRPC server, serving data from the appropriate CSV file
    and listening on port 5440.
    """
    if csv_file is None:
        # If no CSV file is provided, detect the correct one based on the container name
        csv_file = get_partition_and_file()

    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Add the MatchCountServicer to the server
    matchdb_pb2_grpc.add_MatchCountServicer_to_server(MatchCountServicer(csv_file), server)

    # Bind the server to the specified port (default is 5440)
    server.add_insecure_port(f'[::]:{port}')

    # Start the server
    print(f'Server started on port {port}, serving {csv_file}')
    server.start()

    # Keep the server running
    server.wait_for_termination()


if __name__ == '__main__':
    # If command-line arguments are provided, use them
    if len(sys.argv) == 3:
        csv_file = sys.argv[1]
        port = int(sys.argv[2])
        serve(csv_file, port)
    else:
        # Otherwise, automatically detect the partition and use default port 5440
        serve()
