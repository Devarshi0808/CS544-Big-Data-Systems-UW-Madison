import grpc
from concurrent import futures
import os
import threading
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

import table_pb2_grpc
import table_pb2

# Thread-safe dictionary to store file names for CSV and Parquet formats
storage = {'csv_files': [], 'parquet_files': []}
storage_lock = threading.Lock()  # Lock to ensure thread-safe operations

class TableService(table_pb2_grpc.TableServicer):
    """
    gRPC Service for handling file uploads and column summation.
    """

    def Upload(self, request, context):
        # Assign file ID based on current storage length, ensuring thread-safety with lock
        with storage_lock:
            file_id = len(storage['csv_files']) + 1
        
        # Define filenames for CSV and Parquet
        csv_file = f"uploaded_{file_id}.csv"
        parquet_file = f"uploaded_{file_id}.parquet"

        try:
            # Save the uploaded CSV data to a file
            with open(csv_file, "wb") as f:
                f.write(request.csv_data)

            # Convert CSV to Parquet
            csv_data = csv.read_csv(csv_file)
            pq.write_table(csv_data, parquet_file)

            # Update storage safely
            with storage_lock:
                storage['csv_files'].append(csv_file)
                storage['parquet_files'].append(parquet_file)

            return table_pb2.UploadRes(error="")

        except Exception as ex:
            return table_pb2.UploadRes(error=str(ex))

    def ColSum(self, request, context):
        # Initialize column summation
        column_name = request.column
        data_format = request.format
        column_sum = 0

        try:
            if data_format == "csv":
                with storage_lock:
                    for csv_file in storage['csv_files']:
                        csv_data = csv.read_csv(csv_file)
                        available_columns = csv_data.column_names
                        
                        # Check if the column exists in the current CSV file
                        if column_name in available_columns:
                            column_values = csv_data[column_name]
                            for chunk in column_values.chunks:
                                column_sum += sum(chunk.to_pylist())

            elif data_format == "parquet":
                with storage_lock:
                    for parquet_file in storage['parquet_files']:
                        try:
                            # Retrieve metadata to check for the column
                            metadata = pq.read_metadata(parquet_file)
                            if column_name in metadata.schema.names:
                                table_data = pq.read_table(parquet_file, columns=[column_name])
                                column_values = table_data[column_name]
                                for chunk in column_values.chunks:
                                    column_sum += sum(chunk.to_pylist())
                        except KeyError:
                            # Skip if column is not found in the file
                            continue

            return table_pb2.ColSumRes(total=column_sum, error="")

        except Exception as ex:
            return table_pb2.ColSumRes(error=str(ex))


def run_server():
    """
    Function to start the gRPC server.
    """
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    table_pb2_grpc.add_TableServicer_to_server(TableService(), grpc_server)
    grpc_server.add_insecure_port("[::]:5440")
    grpc_server.start()
    print("gRPC server listening on port 5440")
    grpc_server.wait_for_termination()


if __name__ == "__main__":
    run_server()
