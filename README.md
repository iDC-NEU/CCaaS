# CCaaS: Concurrency Control As a Service

Paper Link: https://vldb.org/pvldb/vol18/p2761-zhou.pdf
论文链接： https://vldb.org/pvldb/vol18/p2761-zhou.pdf

CCaaS proposes "**decoupling concurrency control into independent services**," establishing a cloud-native database system with a three-tier architecture (**execution-concurrency control-storage**). This allows the execution and storage layers to focus on their respective tasks, while the concurrency control layer can independently scale resources based on demand.
CCaaS提出并“**发控制解耦为独立的服务**”，建立一个三层架构（**执行-并发控制-存储**）的云原生数据库系统。这样，执行和存储层可以专注于各自的任务，而并发控制层亦可根据需求进行独立资源伸缩。

Based on cloud-native design principles (functional decoupling), and following the core principle of the transactional CC algorithm—handling concurrent reads and writes or write-write conflicts—the operations of different engines are abstracted into two basic operations: read and write. This allows CC to be decoupled and operated as an independent service, enabling it to be connected to multiple engines with different data models. This approach makes CC services more reusable and allows for independent upgrades and evolution, thereby improving development agility.
考虑到云原生设计原则（即功能解耦），根据事务的CC算法的核心原理，即处理并发读写或写写冲突，**将不同引擎的操作抽象为两项基本操作：读与写**，使得CC可以被解耦并作为**独立服务**，可以实现将其连接到具有不同数据模型的多个引擎。这种方法使CC服务更易于复用，并能独立升级和演进，从而提升开发敏捷性。


#**system deployment 系统部署**
ubuntu:

apt install libtool make autoconf g++-11 zlib1g-dev libgoogle-perftools-dev libssl-dev

apt install g++

versions: protobuf(v3.20.1), zmq(v3/v4)

Current Version is Epoch-Based Sharded Multi-Master OCC

#**benchmarks**
go-ycsb: https://anonymous.4open.science/r/go-ycsb-6B12 when connecting to CCaaS, you need set the config in workloads/CCaaS_config.yml
ycsb/TPC-C: See official website.

#**connected execution/storage engines**
openGauss-MOT: [https://anonymous.4open.science/r/CCaaS-MOT-620B](https://github.com/Mister-Star/CCaas-MOT) (execution and storage) add a config to send requests to CCaaS

when connecting to storage, you need set the ip and the push down threads in the CCaaS/Storage_config.xml
new edition：https://github.com/Mister-Star/Taas-Sharding.git

