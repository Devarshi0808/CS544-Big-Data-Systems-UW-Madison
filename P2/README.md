# P2 (5% of grade): Counting Wins with gRPC and an LRU Cache

## Overview

In this project, you'll build an application that lets you count
international football wins matching some criteria (team, country,
combination, etc.).  The dataset is hash partitioned by the country
column.  There are two partitions, and you will use Docker Compose to create
a cluster of 2 workers, each responsible for one of the partitions.

A client will communicate with the servers via gRPC.  Depending on the
query, the client will need to identify the correct server that will
have the data to count the wins.  Some queries will require querying
both servers and summing their answer if the data to be counted is
split across the servers.  The client will have a built-in LRU cache
so that it can sometimes avoid asking servers for the same information
as was asked before.

Learning Objectives:

* deploy multiple containers using Docker compose
* write programs that communicate via gRPC
* implement an LRU Cache
* access data that is hash partitioned

Before starting, please review the [general project directions](../projects.md).

## Clarifications/Corrections

* 23 Sept, 2024: `autograder.py` uploaded. Folders `inputs`, `outputs` and `wins` changed. Minor typos fixed in `README.md`.
* 23 Sept, 2024: `check_sub.py` updated.

## Part 1. gRPC and Protobufs

Create a `.proto` file called `matchdb.proto`.  It should have a
single service, called `MatchCount`, that contains a single RPC,
called `GetMatchCount`.

`GetMatchCount` should accept two arguments, both strings: "country"
and "winning_team".  It should return "num_matches", an `int32`.

The messages for `GetMatchCount`'s arguments and return value should
be called `GetMatchCountReq` and `GetMatchCountResp`, respectively.

You should build `matchdb_pb2.py` and `matchdb_pb2_grpc.py` files from
your `.proto` file using the commands shown in lecture:

* https://docs.google.com/document/d/1aqa5dMxDovp8bmGYjPLPd029DSTGSONbVX5cSemk7tw/edit#heading=h.m1eyvd1zo4pf
* https://git.doit.wisc.edu/cdis/cs/courses/cs544/f24/main/-/tree/main/lec/06-networking

Further documentation is available here:

* https://grpc.io/docs/languages/python/quickstart/
* https://grpc.io/docs/languages/python/basics/

## Part 2. Server

Implement your server in `server.py`. It will be started like this:

```sh
python -u server.py <PATH_TO_CSV_FILE> <PORT>
```

### Data

Your server should first read in the CSV.  You can read the CSV
however you like (for example, with Python's built in csv module, with
pandas, etc).  Just make sure that in part 4 your Dockerfile installs
whatever packages are necessary for your code.  We will only test your
server within a container, created from an image built from the
Dockerfile you give us, so you do not need to worry about what we have
installed on our virtual machine.

The CSV files were created from the "International football results
from 1872 to 2024" dataset hosted on Kaggle ([Full Dataset located
here](https://www.kaggle.com/datasets/martj42/international-football-results-from-1872-to-2017)). We
simplified the dataset to include the columns relevant to your server:

1. `winning_team`: The name of the team that won.
2. `country`: The country where the match was played.

We also partitioned the dataset into two smaller files, part_0.csv and
part_1.csv.  To do so, we computed a hash over the country column.
Rows where the resulting hash value was even (that is,
simple_hash(country)%2 == 0) were written to part_0.csv; rows with odd
values went to part_1.csv.

### gRPC

Your server should implement the MatchCount service specified in your
mathdb.proto file, and it should start serving on the port specified
in the command line arguments.

When a `MatchRequest` is received, you need to return the number of
matches matching the filters.  Empty strings (`""`) means you should
ignore that filter.  For example, `winning_team="England" and
country=""` means that you should count all the rows where England won,
in any country, and return the result.

After implementing `server.py`, run two servers instances, each with
one of the partitions:

```sh
python3 -u server.py partitions/part_0.csv 50051 &> server0.log &
python3 -u server.py partitions/part_1.csv 50052 &> server1.log &
```

Reminders:
* the "-u" makes prints unbuffered, so output immediately shows up in the .log files
* you can use "tail -f server0.log" to watch the output
* if you change your server code, remember to kill and restart for the changes to take effect

Feel free to print any debugging info from your server that you like
(we will grade the server based on it's gRPC implementation, not any
printed output).

## Part 3. Client

### Counting Wins

Now, you'll write a client program that communicates with the servers
via gRPC.  Like the servers, the client will answer questions about
how many win rows match given filters.  However, your client is not
allowed to access the partition data directly.  Instead, based on the
filters, it must determine which server(s) to communicate with to obtain
the answer.

For a query like `winning_team="Argentina" and country="United
States"`, the client must recognize that all rows with country="United
States" are in partition 1, so it should only communicate with the
server for that partititon.  For a query like
`winning_team="Argentina" and country=""`, you need to count all rows
where Argentina won, regardless of what country the game was played
in.  In this case, the relevant rows may appear in both partitions, so
the client should get a count from each gRPC server, and add those two
counts together to get a total count.

The data was partitioned with this function (we adapted this code from
a JavaScript example
[here](https://stackoverflow.com/questions/6122571/simple-non-secure-hash-function-for-javascript)):

```python
def simple_hash(country):
    out = 0
    for c in country:
        out += (out << 2) - out + ord(c)
    return out
```

**Important Requirement:** your client must not make gRPC calls to a
server that is guaranteed not to have the requested data, based on the
partitioning.

Write your client in a `client.py` program  that can be invoked like this:

```sh
python3 client.py <SERVER_0> <SERVER_1> <INPUT_FILE>
```

For example, if your servers are still running using the command from
part 2, you should be able to launch the client like this:

```sh
python3 client.py localhost:50051 localhost:50052 inputs/input_0.csv
```

Your client should open the input CSV and loop over the rows.  Each
row will may specify a filter for winning_team and/or country.  The
client should make the necessary gRPC calls to count the wins matching
rows, then print the count to standard out (a regular print).

Consider this example input:

```
winning_team,country
,El Salvador
Jordan,China PR
Brazil,Switzerland
Finland,Finland
,Switzerland
,Iran
,Thailand
,Niger
Venezuela,Brazil
Brazil,Switzerland
Jordan,China PR
,Niger
,El Salvador
```

In the cases where there is no `country` field present, then you are
getting the number of matches in total across both servers.

If you want to print anything extra (say for debugging purposes),
please use standard error.  We will grade based on your standard out,
and ignore anything from standard error.

### Caching

Sometimes, the same filter combinations may appear more than once in
an input file.  The dataset does not change, so a repeat call to the
servers is avoidable.

Your client should implement an LRU cache of size **10** to remember
the win counts.

Modify your printed output so that any time an answer is found in the
cache (without needing to make additional gRPC calls), a star (`*`)
is shown after the count.  Here is what the output should look like
for the example input shown earlier:

```
228
8
7
108
511
311
700
88
7
7*
8*
88*
228*
```

The last four lines, `7*`, `8*`, `88*`, and `228*` are where the cache
was used instead of making requests to the server. All of the input we
will use to test your solution are located in the `input`
directory.

To test your implementation, run the `client.py` with an input
file of your choosing. For example:

```sh
python3 -u client.py localhost:50051 localhost:50052 inputs/input_0.csv
```

You will find the expected output in `outputs` directory. Run several
inputs and compare the outputs.

## Part 4. Docker Deployment

You will use Docker Compose to run the two gRPC servers.  We will
provide the docker-compose.yml file (in the `wins` directory), but you
must write a Dockerfile to build an image, named `p2`, with your code
and dependencies (e.g., Python packages) inside.  Take a moment to
look at the provided compose file now.

Your `Dockerfile` should build an image that (1) contains code for both your client and server,
(2) datasets for both data partitions, and (3) the input files.

Here are the steps to start your cluster:

1. Build the docker file `docker build . -t p2`
2. `cd wins` (because the docker-compose.yml file is there)
3. Run the following command in the terminal to start the gRPC servers: `docker compose up -d`

The `docker-compose.yml` file is in a directory named `wins`, and the
service is called `server`.  Thus, the two replicas will be named
`wins-server-1` and `wins-server-2`.  The names start from 1, but our
partitions start from 0, so server 1 will serve partition 0, and
server 2 will serve partition 1.

Previously, you ran the server like this: `python -u server.py <PATH_TO_CSV_FILE> <PORT>`.

Now that we're deploying in a container, make those arguments
optional, so that `python -u server.py` is also valid.  Here's what
you should use for the file and port when they are not specified on
the command line:

* **file**: determine whether the server is running in a container
named "wins-server-1" or "wins-server-2", and choose the correct
partition accordingly.  Checking the name inside a container is a bit
tricky, but can be done with the `socket` module that comes with
Python.  First, check the IP assigned to the container with
`socket.gethostbyname(socket.gethostname())`.  Then, check the IPs for
the two names with `socket.gethostbyname("wins-server-1")` and `socket.gethostbyname("wins-server-2")`.
The server.py should compare these IPs to determine
whether it's role to be server 1 (partition 0) or server 2 (partition 1).
* **port**: use 5440.  Note that only a single program, the server, should run in each container, so we don't need to worry about port collisions.

There are two ways you can run the client to use these servers.  From
outside any container, you can take advantage of the port forwarding
settings in the compose file to run commands like this:

```bash
python3 client.py localhost:5000 localhost:5001 inputs/input_0.csv
```

Alternatively, you can start client.py inside a container (this is how
we will grade it).  When you did "docker compose up -d" the first
time, you have have noticed that Compose created a virtual network
called "wins_default".  If you run client.py inside a new container,
it will need to be on that network.  You can do that with the
"--net=????" option, like this:

```bash
docker run --net=wins_default p2 python3 /client.py wins-server-1:5440 wins-server-2:5440 /inputs/input_0.csv
```

Notice that when we run the client.py inside a network, we use the
server names ("wins-server-X") instead of localhost.

## Grading

Copy `autograde.py` to your working directory 
then run `python3 -u autograde.py` to test your work.
You can safely assume that we will use a python environment that has
python libaries for gRPC installed.
The test result will be written to `score.json` file in your directory.

This will probably be your grade, but autograders are imperfect, so we
reserve the right to deduct further points.  Some cases are when
students achieve the correct output by hardcoding, or not using an
approach we specifically requested.

After pushing your code to the designated GitLab repository, 
you can also verify your submission. 
To do so, simply copy `check_sub.py` to your working directory and run 
the command `python3 check_sub.py`

## Submission

You have some flexibility in how your organize your project files.
However, we need to be able to easily run your code.  In order to be
graded, please ensure to push anything necessary so that we'll be able
to run your client and server as follows:

1. `git clone YOUR_REPO`
2. `cd YOUR_REPO`
3. copy in tester code, inputs, and wins/docker-compose.yml
4. `docker build . -t p2`
5. `cd wins`
6. `docker compose up -d`
7. `docker run --net=wins_default p2 python3 /client.py wins-server-1:5440 wins-server-2:5440 /inputs/SOME_INPUT.csv`

Step 3 means you don't need to include a few of the files provided
(though it won't cause harm if you do include them).  In addition to
the files needed to run your code, you should include your
matchdb.proto (not just the .py files generated from it).

If you worked with a partner, there should only be one submission
repo, with at least one commit by each partner.
