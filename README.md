# CCaas: Concurrency Control As a Service


ubuntu:

apt install libtool make autoconf g++-11 zlib1g-dev libgoogle-perftools-dev libssl-dev

apt install g++

versions: protobuf(v3.20.1), zmq(v3/v4)

Current Version is Epoch-Based Sharded Multi-Master OCC

go-ycsb: https://anonymous.4open.science/r/go-ycsb-6B12 when connecting to CCaaS, you need set the config in workloads/CCaaS_config.yml

openGauss-MOT: https://anonymous.4open.science/r/CCaaS-MOT-620B (execution and storage) add a config to send requests to CCaaS

when connecting to storage, you need set the ip and the push down threads in the CCaaS/Storage_config.xml
