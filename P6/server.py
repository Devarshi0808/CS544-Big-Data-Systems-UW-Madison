import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import ConsistencyLevel
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
from cassandra import InvalidRequest, Unavailable


class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        try:
            self.cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
            self.session = self.cluster.connect()

            self.session.execute("DROP KEYSPACE IF EXISTS weather")

            self.session.execute("""
                CREATE KEYSPACE weather
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
            """)

            self.session.set_keyspace('weather')

            self.session.execute("""
                CREATE TYPE IF NOT EXISTS station_record (
                    tmin int,
                    tmax int
                )
            """)

            self.session.execute("""
                CREATE TABLE IF NOT EXISTS stations (
                    id text,
                    date date,
                    name text static,
                    record frozen<station_record>,
                    PRIMARY KEY (id, date)
                ) WITH CLUSTERING ORDER BY (date ASC)
            """)

            self.spark = SparkSession.builder.appName("p6").getOrCreate()

            self.load_station_data()

            print("Server started")  

        except Exception as e:
            print(f"Error in initialization: {str(e)}")
            raise

    def load_station_data(self):
        try:
            file_path = "ghcnd-stations.txt"
            stations_df = self.spark.read.text(file_path)

            print(f"Total records loaded: {stations_df.count()}")

            stations_df = stations_df.select(
                F.trim(F.substring(F.col("value"), 1, 11)).alias("id"),    # Station ID
                F.trim(F.substring(F.col("value"), 39, 2)).alias("state"), # State
                F.trim(F.substring(F.col("value"), 42, 30)).alias("name")  # Station Name
            )

            wisconsin_stations = stations_df.filter(F.col("state") == "WI")
            print(f"Wisconsin stations found: {wisconsin_stations.count()}")

            wi_rows = wisconsin_stations.select("id", "name").collect()

            for row in wi_rows:
                query = "INSERT INTO weather.stations (id, name) VALUES (?, ?)"
                prepared = self.session.prepare(query)
                self.session.execute(prepared, (row["id"], row["name"].strip()))

            print(f"Inserted {len(wi_rows)} stations for Wisconsin.")

        except InvalidRequest as e:
            print(f"Invalid request while loading station data: {str(e)}")
        except Exception as e:
            print(f"Error in load_station_data: {str(e)}")
            raise

    def StationSchema(self, request, context):
        try:
            query = "DESCRIBE TABLE weather.stations"
            result = self.session.execute(query)
            schema = next(iter(result)).create_statement
            return station_pb2.StationSchemaReply(schema=schema, error="")
        except InvalidRequest as e:
            error_message = f"Invalid schema request: {str(e)}"
            print(error_message)
            return station_pb2.StationSchemaReply(schema="", error=error_message)
        except Exception as e:
            print(f"Error in StationSchema: {str(e)}")
            return station_pb2.StationSchemaReply(schema="", error=str(e))

    def StationName(self, request, context):
        try:
            query = "SELECT name FROM weather.stations WHERE id = ? LIMIT 1"
            prepared = self.session.prepare(query)
            row = self.session.execute(prepared, (request.station,)).one()

            if row:
                return station_pb2.StationNameReply(name=row.name, error="")
            else:
                return station_pb2.StationNameReply(name="", error="Station not found")
        except InvalidRequest as e:
            error_message = f"Invalid request: {str(e)}"
            print(error_message)
            return station_pb2.StationNameReply(name="", error=error_message)
        except Exception as e:
            print(f"Error in StationName: {str(e)}")
            return station_pb2.StationNameReply(name="", error=str(e))

    def RecordTemps(self, request, context):
        try:
            date = datetime.strptime(request.date, '%Y-%m-%d').date()

            query = """
                INSERT INTO weather.stations (id, date, record)
                VALUES (?, ?, {tmin: ?, tmax: ?})
            """
            prepared = self.session.prepare(query)
            prepared.consistency_level = ConsistencyLevel.ONE

            self.session.execute(prepared, (request.station, date, request.tmin, request.tmax))

            return station_pb2.RecordTempsReply(error="")
        except InvalidRequest as e:
            error_message = f"Invalid request: {str(e)}"
            print(error_message)
            return station_pb2.RecordTempsReply(error=error_message)
        except Unavailable as e:
            print(f"RecordTemps failed with Unavailable exception: {str(e)}")
            return station_pb2.RecordTempsReply(error="unavailable")
        except NoHostAvailable as e:
            print(f"Error in RecordTemps (NoHostAvailable): {str(e)}")
            return station_pb2.RecordTempsReply(error="unavailable")
        except Exception as e:
            print(f"Error in RecordTemps: {str(e)}")
            return station_pb2.RecordTempsReply(error=str(e))

    def StationMax(self, request, context):
        try:
            query = """
                SELECT max(record.tmax) as max_temp 
                FROM weather.stations 
                WHERE id = ?
            """
            prepared = self.session.prepare(query)
            prepared.consistency_level = ConsistencyLevel.THREE 

            row = self.session.execute(prepared, (request.station,)).one()

            if row and row.max_temp is not None:
                return station_pb2.StationMaxReply(tmax=row.max_temp, error="")
            else:
                return station_pb2.StationMaxReply(tmax=0, error="No temperature records found")
        except InvalidRequest as e:
            error_message = f"Invalid request: {str(e)}"
            print(error_message)
            return station_pb2.StationMaxReply(tmax=0, error=error_message)
        except Unavailable as e:
            print(f"StationMax failed with Unavailable exception: {str(e)}")
            return station_pb2.StationMaxReply(tmax=0, error="unavailable")
        except NoHostAvailable as e:
            print(f"Error in StationMax (NoHostAvailable): {str(e)}")
            return station_pb2.StationMaxReply(tmax=0, error="unavailable")
        except Exception as e:
            print(f"Error in StationMax: {str(e)}")
            return station_pb2.StationMaxReply(tmax=0, error=str(e))

def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=9),
        options=[("grpc.so_reuseport", 0)],
    )
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port('0.0.0.0:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
