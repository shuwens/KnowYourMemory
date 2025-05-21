## Send Recieve Connection

The send receive connection is simple connection using RDMA send and receive verbs.


<!--  TODO Add detailed prtocol description and Performance Model -->

### Benchmark

<!--  TODO Add Benchmark results -->


#### Run benchmark

You will first have to compile the connection code.

    make

To then run the benchmark with the following options

    ./bin/sendrecv [OPTION...]

        --srq          Whether to use a shard receive queue
        --bw           Whether to test bandwidth
        --lat          Whether to test latency
        --client       Whether to as client only
        --server       Whether to as server only
    -i, --address arg  IP address to connect to
    -n, --iters arg    Number of exchanges (default: 1000)
    -s, --size arg     Size of message to exchange (default: 1024)
        --batch arg    Number of messages to send in a single batch. Only
                       relevant for bandwidth benchmark (default: 20)


So to run a latency benchmark sending 10000 messages of size 2 kB you will have to run

    ./bin/sendrecv --lat --server -n 10000 -s 2048 -i 10.233.0.1

to start the server, where `10.233.0.1` is the IP of the device you want to run it on. And

    ./bin/sendrecv --lat --client -n 10000 -s 2048 -i 10.233.0.1

to start the client. The recorded latency for every message will then be written to 
`data/sr_test_lat_send_N10000_S2048_<datetime>.csv`

You can also start the server and client in the same process, but this might cause concurrency issues

    ./bin/sendrecv --lat -n 10000 -s 2048 -i 10.233.0.1
