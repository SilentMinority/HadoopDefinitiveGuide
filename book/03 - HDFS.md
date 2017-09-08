## 三、HDFS

### 3.1 HDFS设计理念

- 超大文件

  HDFS被设计为存储大型文件，几百MB、GB、TB，甚至PB级的文件存储

- 流式数据访问

  HDFS构建思路是一次写入，多次读取是最高效的访问模式。数据被写入后，会经常被进行分析计算。大多计算都会涉及到数据的全部或大部分。因此读取全部数据的延迟比读取第一条数据的延迟更加重要。

- 商用硬件

  Hadoop设计为可以在普通商用级别设备上运行。

- 低延迟的数据访问

  要求低延迟的应用不适合运行在HDFS上。

- 大量的小文件

  NameNode会将文件的元数据保存到内存中，因此大量的小文件会导致NameNode内存紧张。HDFS不适合存储大量小文件。

- 多用户写入，任意修改文件

  HDFS同一文件同一时刻只支持一个Writer，同样只支持在文件末尾追加内容，不支持任意位置修改文件。

### 3.2 HDFS概念

#### 3.2.1 Block

HDFS将文件划分为相同大小的块进行保存，称为Block，每一个Block都有一个Unique ID。同一文件的Block可能存储在不同机器、不同机架上。Block的大小默认是64MB。HDFS会按Block在不同的DataNode进行备份存储（默认备份3份）。按块进行抽象存储带来以下好处。

- **加快寻址** - 块大小设为64MB，远大于普通文件，寻址速度加快
- **文件大小不受单一节点磁盘限制** - 文件大小可以超过单个DataNode的磁盘
- **简化存储系统设计** - 元数据管理、故障恢复只考虑到Block，简化设计
- **提高容灾能力** - HDFS会对Block进行备份，某个Block丢失时，会从其他备份进行恢复。这对用户是透明的

#### 3.2.2 NameNode & DataNode

HDFS集群节点有两种角色，NameNode和DataNode。NameNode作为管理角色，DataNode作为工作角色。另外HDFS提供Client端供用户使用HDFS，Client对用户提供标准POSIX文件系统接口。

NameNode管理着整个文件系统的Namespace，保存文件系统树和文件的元信息。包括所有的目录、文件，以及文件与Block的关系信息。这些NameNode不保存Block的位置信息，位置信息在集群启动时由DataNode上报。

DataNode负责存储Block，接收Client的读写请求和NameNode的调度，并定时向NameNode上报Block信息。

NameNode故障数据丢失，会导致文件系统整个丢掉。它的单点故障方案有两种。一种是在多个文件系统中保存元数据信息，一般是远程挂载一个网络文件系统。NameNode会实时同步、原子性的把元数据操作写到这些文件系统中。另一种方案是配置SecondaryNameNode，SNN可以辅助合并Image文件和EditLog，并保存Image文件副本。NameNode故障时，可以从SNN拷贝Image文件，但因为SNN总是落后于NN，这样会导致一部分最新数据丢失。

##### 3.2.2.2 Block Caching

DataNode可以把经常访问的Block缓存到内存（堆外内存）中，默认一个Block只缓存中一个DataNode的内存中，但可以根据文件配置。对于MapReduce、Spark等相似的框架，这可以节省许多读取磁盘的消耗。用户和应用程序可以通过发送缓存命令让DataNode缓存指定Block。

#### 3.2.3 HDFS Federation

对于超大HDFS集群，NameNode的内存可能成为系统瓶颈。这是可以引入HDFS联邦（Federation）特性，即多个NameNode。每个NameNode负责管理一部分命名空间卷（比如A管理`/user`，B管理`/share`）。联邦中的NameNode相互之间并不通信，DataNode可能存储着多个命名空间卷的Block，因此DataNode需要将Block位置信息注册到每一个NameNode。

#### 3.2.4 HDFS 高可用

HDFS 2.x 引入高可用机制来快速从NameNode故障恢复。大概要点如下。

- 增加一台StandbyNameNode作为备用节点，通过Zookeeper实现与ActiveNameNode日志同步。
- DataNode需要向两台NameNode报告Block位置和改变信息。
- Client端需要特殊机制来处理故障转移，保证对用户透明。

同时，HDFS 引入Fence机制来避免故障转移时出现多个ActiveNameNode的情况。

### 3.3 命令行接口

HDFS 提供了Shell Like的命令行接口，[参考](http://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-common/CommandsManual.html)。

#### 3.3.1 HDFS文件访问权限

HDFS权限与POSIX类似，权限分为R/W/X。HDFS默认按照用户和用户组进行权限认证，可以通过配置`dfs.permissions.enabled`开启额外权限认证。

### 3.4 Hadoop文件系统

Hadoop有一个抽象文件系统概念，HDFS只是其中一个实例。Hadoop文件系统接口支持Local（`file://`）、HDFS（`hdfs://`）、FTP（`ftp://`）、S3（`S3n://`）等等许多文件系统。这些系统都可以用Hadoop的接口来访问。比如`hadoop dfs -ls file:///` Shell命令，即是列出本地文件系统的根目录。
Hadoop FS提供了Java、HTTP、C语言、FUSE等多种方式的接口。

##### HTTP接口

HTTP接口分为两种模式，Direct模式和Proxy模式。Direct模式下，用户直接访问运行在NameNode和DataNode上的Web服务，默认端口分别是 50070和 50050。Proxy模式下，用户的所有访问都通过一个或多个代理服务器分发到HDFS集群中的机器上。
![HadoopFS - HTTP API](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\HadoopFS - HTTP API.png)

### 3.5 JAVA接口

参考Hadoop示例项目。

### 3.6 数据流

#### 3.6.1 HDFS文件读取

![HDFS - DataFlow - Read File](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\HDFS - DataFlow - Read File.png)

Java接口从HDFS读取文件流程如上图。分步解释一下。

1. Client端调用DFS对象的open()方法打开文件。
2. DFS对象通过RPC调用NameNode。NameNode返回文件初始几块Block的位置，Block会按照与Client的距离排序。
3. DFS类返回一个FSDataInputStream对象（封装了DFSDataInputStream对象），这个对象负责与NameNode、DataNode通信。Client调用FSDataInputStream的read()方法读取文件。
4. DFSDataInputStream对象连接距离最近的DataNode，读取Block中的数据。Client通过反复调用read()方法连续读取数据。
5. Block读取到末端时，DFSDataInputStream对象连接下一个Block的最佳DataNode，读取数据。这个过程对Client是透明的，Client端只需要读取连续的流。DFSDataInputStream也会根据Client的需求像NameNode请求下一批Block的位置。
   在读取过程中，如果DFSDataInputStream与DataNode通信错误，DFSDataInputStream会从另一个最近的DataNode读取数据。
   DFSDataInputStream还会检查从DataNode获取数据的校验和（*checksum*）。如果发现损坏的Block，DFSDataInputStream会在从其他DataNode读取之前通知NameNode。
6. Client端读取完成，调用FSDataInputStream的close()方法关闭数据流。

##### 3.6.1.TIP DataNode距离计算

Hadoop无法自动侦测节点间距离，需要用户手动配置。配置`topology.script.file.name`项指定[机架感知](https://www.google.com/search?&q=hdfs+%E6%9C%BA%E6%9E%B6+%E9%85%8D%E7%BD%AE)的可执行程序，一般是脚本文件。

- 同一节点上的进程 = 0
- 同一机架上不同节点 = 2
- 统一数据中心不同机架 = 4
- 不同数据中心（设想，尚未实现） = 6

#### 3.6.2 HDFS文件写入

![HDFS - DataFlow - Write File](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\HDFS - DataFlow - Write File.png)

Java接口写入文件流程：

1. Client调用DistributedFileSystem对象的create()方法创建文件。

2. DFS对象RPC连接NameNode在Namespace中创建一个文件。NameNode检查文件不存在并且Client有创建该文件的权限，如果不通过向Client抛出一个IOException。检查通过记录一条创建文件的记录。

3. DFS对象返回给Client一个FSDataOutputStream对象（封装了DFSOutputStream对象），这个对象负责与NameNode、DataNode通信。
   Client写入数据时，DFSOutputStream对象负责将数据分成一个个的数据包，加入“数据队列（*data queue*）”。DataStreamer消费队列，要求NameNode从DataNode列表中挑选合适的DataNode，来给新的Block和Block备份分配位置。

4. 挑选出的DataNodes组成一条管线（*pipeline*），DataStreamer将数据包流式传输到一个DataNode，第一个DataNode存储数据包并传递给下一个，以此类推。

5. DFSOutputStream内部维护一个“确认队列（*ack queue*）”，所有DataNode都确认收到数据包后，才会从队列中移出这个包。
   在数据传递过程中，如果有DataNode出现失败，HDFS会执行以下操作。

   - 将 *ack queue* 队列中的数据包加入到 *data queue* 最前面，保证故障DataNode后面的DataNode不会错误数据包
   - 为在其它正常DataNode上的Block重新指定新的Unique ID，并发送给NameNode。这样故障DataNode恢复后会删除错误的Block。
   - 从 *pipeline* 中删除故障的DataNode，继续写入到剩余正常的DataNode中。
   - NameNode发现出错Block副本数量不足时，会安排在另一个节点上创建新的副本。后续数据块接受正常处理。

   如果出现多个DataNode故障，写操作只要达到了最小副本数（`dfs.namenode.replication.min` 默认 1），就认为成功。Block会被异步进行复制备份，以达到副本因子数（`dfs.replication` 默认 3）。

6. Client完成数据写入后，调用FSOutputStream的close()方法。DFSOutputStream会将剩余数据全部写入管线中，在等到所有数据包的ACK确认后，连接NameNode发送完成信号。NameNode此时已知道文件由哪些Block组成，在等到所有Block达到最小复制数后，返回Client成功。

##### 3.6.2.TIP Block副本位置选择策略

HDFS默认策略如下，可以手动实时配置。

1. 如果Client在某台DataNode上，第一个副本放在Client运行的节点上；否则放在随机一个节点上
2. 第二个副本放在于第一个不同机架、随机选择的节点上
3. 第三个副本放在于第二个相同机架、随机选择的节点上
4. 其他副本放在集群随机选择的节点上，一般不会在同一机架放置太多副本
5. 复本位制确定后，会根据网络拓扑创建管线，进行数据传输，如下图
   ![HDFS - DataFlow - Write File - Pipeline](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\HDFS - DataFlow - Write File - Pipeline.png)

#### 3.6.3 一致性模型

一致性模型描述了文件的读/写可见性。HDFS与POSIX与一些不同。
文件创建后，立即就可以在Namespace中看到。

```java
Path p = new Path("p");
fs.create(p);
assertThat(fs.exists(p), is(true));
```

写书到文件的数据不能保证立即可见，即使Client进行了flush。

```java
Path p = new Path("p");
OutputStream out = fs.create(p);
out.write("content".getBytes("UTF-8"));
out.flush();
assertThat(fs.getFileStatus(p).getLen(), is(0L));
```

超过一个Block的数据被写入后，Block对新的Reader就可见了。当前正在写的Block对Reader不可见。

FSDataOutputStream提供了hflush()方法强制将所有Buffer Flush到所有DataNode。hflush()成功后，HDFS保证文件到当前点的内容被写到Pipeline中所有的DataNode上，并对所有新的Reader可见。
但要注意，HDFS不保证数据被写入到了所有DataNode的磁盘上，数据可能在DataNode的内存中，这可能会导致数据丢失。

```java
Path p = new Path("p");
FSDataOutputStream out = fs.create(p);
out.write("content".getBytes("UTF-8"));
out.hflush();
assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
```

HDFS提供了hsync()方法，来提供数据落到磁盘的强一致性保证。

```java
FileOutputStream out = new FileOutputStream(localFile);
out.write("content".getBytes("UTF-8"));
out.flush(); // flush to operating system
out.getFD().sync(); // sync to disk
assertThat(localFile.length(), is(((long) "content".length())));
```

HDFS关闭文件时进行了隐性的hsync()调用。

```java
Path p = new Path("p");
OutputStream out = fs.create(p);
out.write("content".getBytes("UTF-8"));
out.close();
assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
```

### 3.7 Flume和Sqoop导入数据

略

### 3.8 Distcp复制数据

Hadoop Distcp命令可以在不同HDFS集群间复制文件或目录。

```shell
% hadoop distcp file1 file2
% hadoop distcp dir1 dir2
```

复制目录时，如果dir2已存在，复制后目录结构是 `dir2/dir1`。如果 dir2 不存在，会自动创建，结果与前者向同。可以使用 -overwrite、-update、-delete 指定文件冲突策略。

Distcp本质是一个MapReduce Job，没有Reduce阶段，Map阶段输出直接进入目标集群。-m [number : Int] 指定MapTask的个数。
Distcp要求两个集群的Hadoop版本需要相同，否则会复制失败。不同版本间复制可以通过webhdfs协议实现。

```shell
% hadoop distcp webhdfs://namenode1:50070/foo webhdfs://namenode2:50070/foo
```

##### 集群均衡

由于复制时，Block的第一个副本总是会保存在目标集群运行MapTask的节点上，容易导致集群数据倾斜。应该合理设置MapTask数量，保证集群均衡。