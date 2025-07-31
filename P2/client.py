import grpc
import csv
import sys
from collections import OrderedDict
import matchdb_pb2
import matchdb_pb2_grpc

# Define the simple_hash function to determine partition based on country
def simple_hash(country):
    out = 0
    for c in country:
        out += (out << 2) - out + ord(c)
    return out

# Custom LRU cache implementation
class LRUCache:
    def __init__(self, max_size=10):
        self.cache = OrderedDict()
        self.max_size = max_size

    def get(self, key):
        if key in self.cache:
            # Move the key to the end to mark it as recently used
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

    def put(self, key, value):
        if key in self.cache:
            # Move the key to the end to mark it as recently used
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.max_size:
            # Pop the first (least recently used) item
            self.cache.popitem(last=False)

# Function to get match count
def get_match_count(winning_team, country, server0_stub, server1_stub):
    # Determine which server(s) to query based on the country
    if country:
        partition = simple_hash(country) % 2
        if partition == 0:
            # Query server 0 only if the country is in partition 0
            response = server0_stub.GetMatchCount(matchdb_pb2.GetMatchCountReq(
                winning_team=winning_team, country=country))
            return response.num_matches
        else:
            # Query server 1 only if the country is in partition 1
            response = server1_stub.GetMatchCount(matchdb_pb2.GetMatchCountReq(
                winning_team=winning_team, country=country))
            return response.num_matches
    else:
        # Query both servers when country is empty
        response0 = server0_stub.GetMatchCount(matchdb_pb2.GetMatchCountReq(
            winning_team=winning_team, country=country))
        response1 = server1_stub.GetMatchCount(matchdb_pb2.GetMatchCountReq(
            winning_team=winning_team, country=country))
        return response0.num_matches + response1.num_matches

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 client.py <SERVER_0> <SERVER_1> <INPUT_FILE>")
        sys.exit(1)

    server0_address = sys.argv[1]
    server1_address = sys.argv[2]
    input_file = sys.argv[3]

    # Create gRPC channels and stubs for both servers
    channel0 = grpc.insecure_channel(server0_address)
    channel1 = grpc.insecure_channel(server1_address)
    server0_stub = matchdb_pb2_grpc.MatchCountStub(channel0)
    server1_stub = matchdb_pb2_grpc.MatchCountStub(channel1)

    # Initialize the custom LRU cache
    cache = LRUCache(max_size=10)

    # Open the input CSV file
    with open(input_file, mode='r') as file:
        reader = csv.DictReader(file)

        # Loop through each row in the input file
        for row in reader:
            winning_team = row['winning_team']
            country = row['country']

            # Create a cache key using the query parameters
            cache_key = (winning_team or "", country or "")

            # Check if the result is already in the cache
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                # Result found in the cache, print with a star
                print(f"{cached_result}*")
            else:
                # Result not in cache, make the gRPC call
                count = get_match_count(winning_team or "", country or "", server0_stub, server1_stub)
                print(count)
                # Store the result in the cache
                cache.put(cache_key, count)

if __name__ == '__main__':
    main()
