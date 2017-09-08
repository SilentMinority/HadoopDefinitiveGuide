## 六、MapReduce工作机制

### 6.1 MapReduce Job 运行剖析

Job对象的submit()方法可以提交运行Job；waitForCompletion()方法会在当前没有提交的情况下提交并等待运行结束，并输出运行信息。

从高层面看，有五种主要的独立实体。

- Client - 提交Job
- YARN Resource Manager - 协调集群内资源分配
- YARN Node Manager - 加载并监控机器上的Container
- Application Master - 协调一个Job下的Task运行，运行在Container中，由Resource Manager规划、Node Manager管理
- DFS - 通常是HDFS，在不同实体间共享Job文件

![MapReduce - Mechanism](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\MapReduce - Mechanism.png)

#### 6.1.1 Job提交

submit()方法创建一个JobSubmitter实体，并调用其submitJobInternal()方法。waitForCompletion()方法每秒检查Job进度，将变化打印到客户端Console。Job完毕后展示Job Counter，失败打印错误信息。
JobSubmitter的提交流程如下。

- 向Resource Manager请求新的Application ID（Step 2）
- 检查Job输出定义是否正确，例如输出目录是否未设置或已存在等错误。出错向MapReduce程序抛出异常
- 计算Job的 Input Split，如果发生例如输入目录不存在等错误导致无法计算。出错向MapReduce程序抛出异常
- 将Job所需文件拷贝至HDFS上，以Job ID命名的目录下。包括Job JAR、配置文件、Input Split等（Step 3）。Job JAR的复制系数很高（`mapreduce.client.submit.file.replication`配置，默认 10），方便Node Manager运行计算时读取。
- 调用submitApplication()提交Job至Resource Manager（Step 4）

#### 6.1.2 Job初始化

Resource Manager接收到submitApplication()的调用时，它将这个请求转给YARN Scheduler。Scheduler负载分配一个Container并加载Application Master进程（Step 5a、5b）。Application Master进程受Node Manager管理。
Application Master的Main Class是MRAppMaster。它会创建一批订阅对象（bookkeeping objects）来监控Job的进度，以此接收到Task的进度和完成报告（Step 6）。然后它会从HDFS检索Input Split（Step 7）。之后会为每一个Input Split创建一个MapTask，并根据 `mapreduce.job.reduces`（Job.setNumReduceTasks() 方法可以设置）定义的数量来创建ReduseTask。Task ID在此时设定。
Application Master决定如何运行Job。如果Job足够小（MapTask少于10个，中有一个Reducer，输入数据小于一个HDFS Block），Application Master会将Task放在自己所在的JVM运行。这种Task称为 *uber task* 。*uber task* 的临界条件可以通过 `mapreduce.job.ubertask.maxmaps`、 `mapreduce.job.ubertask.maxreduces` 和`mapreduce.job.ubertask.maxbytes`进行设置。
最后，在运行任何Task之前，Application Master会调用OutputCommitter（默认是FileOutputCommitter）的setupJob()方法，为Task创建输出目录和临时工作目录。

#### 6.1.3 Task分配

如果Job不是 *uber task* ，Application Master会为Task向Resource Manager请求Container（Step 8）。MapTask优先级比ReduceTask高。至少有 5% MapTask完成后，才会为ReduceTask请求资源。
Application Master会尽量让MapTask遵守数据本地性，ReduceTask则不会。数据本地性分为同节点、同机架、不同机架。对于具体Job，可以通过观察Job Counter决定分别在每个级别上运行多少Task。
资源请求同时会指定Task需要的CPU Core和内存数量。默认每个Task需要一个CPU Core、1024M内存。每个Job可以通过`mapreduce.map.memory.mb`、`mapreduce.reduce.memory.mb`、`mapreduce.map.cpu.vcores` 和 `mapreduce.reduce.cpu.vcores` 具体设置。

#### 6.1.4 Task执行

一个Task被YARN Scheduler分配完资源后，Application Master会联系对应的Node Manager启动Container（Step 9a、9b）。Task通过一个Java应用来运行，应用的Main Class是`org.apache.hadoop.mapred.YarnChild`。在运行Task之前，应用会先本地化Task所需要的资源，包括Job JAR、配置文件等等。最后，运行Task。
YarnChild运行在一个独立的JVM上，所以Task的任何异常（包括JVM崩溃）都不会影响到NodeManager的运行。
每个Task都可以执行安装和提交（setup and commit）操作，它们与任务本身在同一个JVM中运行，并由OutputCommitter确定。对于基于文件的Job，Commit操作将Task输出从临时目录移动到最终目录。有Commit协议保证推测执行不会导致结果重复。

##### Streaming

Streaming运行特殊Map和Reduce任务，目的是启动用户提供的可执行文件并与之进行通信。
Streaming Task使用标准输入输出与进程（可以用任何语言编写）通信。在执行任务期间，Java进程将输入键值对传递给外部进程，外部进程通过用户定义的map或reduce函数运行它，并将输出的键值对传递回Java进程。 从Node Manager的角度来看，就像子进程运行Map或Reduce代码本身一样。

![MapReduce - Mechanism - Streaming](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\MapReduce - Mechanism - Streaming.png)

#### 6.1.5 进度和状态更新

Job需要报告的状态：运行状态、Task进度（百分比）、Job Counter的值、状态消息（可以由用户代码设置）。
MapTask的百分比指已处理数据占输入数据的百分比。ReduceTask百分比需要考虑复制、排序、计算三个阶段，比如Task已执行Reducer输入的一半，那么进度是6分之5。因为已完成复制、排序两个阶段（三分之二），一半的处理（六分之一）。

Task也有一组计数器，负责对Task内的事件计数。计数器要么内置于框架，要么由用户自己开发。
Task的子进程会与Application Master通信，报告进度和状态。
Job执行期间，客户端每秒从Application Master拉取最新状态报告， `mapreduce.client.progressmonitor.pollinterval`可以设置这个周期。也可以调用Job.getStatus()方法获取JobStatus实例，其中包含所有状态信息。

![MapReduce - Mechanism - Status](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\MapReduce - Mechanism - Status.png)

#### 6.1.7 Job完成

Application Master收到最后一个Task完成的报告时，会将Job状态置为Successful。Job对象拉取到后，会返回给客户端，打印完成信息、计数器信息、waitForCompletion()返回。
Application Master也可以向`mapreduce.job.end-notification.url`配置的地址发起HTTP回调。
最后，Application Master和Task Container会清理中间数据、调用OutputCommitter的commitJob()方法。Job信息被存档到Job History Server。

### 6.2 失败处理

#### 6.2.1 Task失败

考虑几种失败的情况。

- 用户代码出错，导致Map/Reduce Task失败。此时Task的JVM会向Application Master报告错误，然后退出。错误信息输出到用户日志。Application Master标记Attempt失败，释放Container。
  对于Streaming Task，Streaming进程以非 0 状态码结束即表示失败。由 `stream.non.zero.exit.is.failure` 属性控制，默认为 `true`。
- JVM出错导致失败。Node Manager通知Application Master失败事件，AM标记Attempt失败。
- Task挂起。Application Master长时间没有收到Task的进度更新，会标记Attempt失败，并杀死Task进程的JVM。超时周期由 `mapreduce.task.timeout`属性控制，单位毫秒，可根据Job设置。默认10分钟。
  Timeout设为 0，即禁用超时机制。挂起任务将用就占用集群资源，尽量避免这种设置。

Application Master会对发生失败Attempt的Task进行Reschedule，并尽量避免在曾经失败的节点上重新运行。同一Task失败上限默认 4次，超过将导致Job失败。上限可以设置， `mapreduce.map.maxattempts`和 `mapreduce.reduce.maxattempts`。
对于某些Job，可能容许部分Task失败，以利用成功Task产生的数据。可以使用 `mapreduce.map.failures.maxpercent` 和 `mapreduce.reduce.failures.maxpercent`设置，允许发生失败Task的最大百分比。
Task可能会因为重复执行（推测执行）、Job失败被杀死，此时不计入失败数。
用户也可以通过Web UI或命令行手动杀死Job和Task。

6.2.2 Application Master失败

Application Master失败时，也可以进行重新运行。
YARN的`yarn.resourcemanager.am.max-attempts`规定了在其上运行的任何类型应用，单个Application Master的最大重试次数。`mapreduce.am.max-attempts`规定了MapReduce应用单个Application Master的最大重试次数。要修改MapReduce应用AM的重试次数上限，必须同时修改这两个值。

Application Master的恢复机制。AM定时向Resource Manager发送心跳，RM发现AM失败后，会重新分配一个Container启动新的AM。并使用Job History恢复已经运行的Task，不再对其重新运行。`yarn.app.mapreduce.am.job.recovery.enable`控制恢复机制开关，默认为 true。

MapReduce Client端通过AM获取作业进度，AM失败后Client需要向RM请求新的AM的地址，继续获取进度。

#### 6.2.3 Node Manager失败

Resource Manager超过规定时长（`yarn.resourcemanager.nm.liveness-monitor.expiry-interval-ms`，毫秒，默认 10分钟）没有收到NodeManager的心跳信息，认为NM出现故障，标记失败。将其从节点资源池移出，不再分配Task。
NM失败后，其上的所有AM和运行中Task也会失败，按之前所述恢复。注意，属于未完成Job的已完成的MapTask也会被安排重新运行，因为ReduceTask无法从NM读取中间数据。
一个NM上失败的Task达到一定数量，RM会将其拉黑（Blacklist），尽量不往其分配Task。阈值由Job属性`mapreduce.job.maxtaskfailures.per.tracker`控制，默认为 3。注意，Blacklist不会跨Application使用。

#### 6.2.4 Resource Manager失败

默认配置下，RM失败会出现单点故障，导致所有Job丢失，不可恢复。

配置HA后，集群运行两个RM，Active和Standby。故障时，Standby接管集群并且不会对客户端造成明显中断。
正在运行的Application状态信息会保存在高可用状态存储（Zookeeper或HDFS）中，Standby可以从其恢复Application。Node Manager信息不会一同存储，而是在NM发送首次心跳时进行快速重建。注意，Task信息由Application Master管理，因此无需恢复。
Standby读取Application信息后，会重启集群中所有在运行的Application Master，这次重启不会计入AM重试次数。

Standby 到 Active的转换是由Failover Controller完成的。默认FC使用Zookeeper的选举机制保证同一时刻只有一个AM，其配置嵌入到Resource Manager中，易于配置。而在 HDFS HA中，FC必须是一个独立进程。也可以配置手动Failover，强烈不建议这样做。

Client和Node Manager必须处理同时存在两个Resource Manager的情况，它们会采用轮询方式链接每一个RM，直到找到Active One。如果Active故障，Client和Node Manager会重试直到Standby变为Active。

### 6.3 任务调度器

### 6.4 Shuffle和排序

#### 6.4.1 Map部分

![MapReduce - Shuffle & Sort - Map](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\MapReduce - Shuffle & Sort - Map.png)

MapTask的输出数据写道磁盘要经过几个步骤。

- 每个MapTask都有一个[环形缓冲区](http://bigdatadecode.club/MapReduce%E6%BA%90%E7%A0%81%E8%A7%A3%E6%9E%90--%E7%8E%AF%E5%BD%A2%E7%BC%93%E5%86%B2%E5%8C%BA.html)，Task的输出首先进入缓冲区。缓冲区大小由 `mapreduce.task.io.sort.mb`控制，默认100M。当缓冲区被占用空间比达到阈值（`mapreduce.map.sort.spill.percent`，默认0.8或者80%），一个后台线程会将缓冲区内容溢写（spill）到磁盘。此时MapTask仍会继续向缓冲区写入输出数据。如果缓冲区被占满，MapTask会被阻塞，直到溢写完成。溢写以循环方式写入由`mapreduce.cluster.local.dir`属性指定的目录下，特定于作业的子目录中。
- 在写磁盘之前，线程首先对MapTask输出数据进行分区（Partition），并在内存中按Key进行排序。如果有Combiner，Combiner就在排序后输出的数据上运行。
- 缓冲区每次达到阈值都会创建一个Spill File，因此MapTask处理完所有输入数据后，可能存在多个Spill File。在Task结束前，这些文件会被Merge到一个已分区且已排序的文件中。 `mapreduce.task.io.sort.factor`属性控制一次最多Merge几个Stream，默认 10。
- 如果存在至少`mapreduce.map.combine.minspills`（默认 3）个Spill File，Combiner会在输出文件写入到磁盘之前再次运行。如果Spill File少于 3个，认为Combiner带来的数据传输量减少不值得再次调用Combiner。
- 对MapTask输出进行压缩减少传输数据量。 `mapreduce.map.output.compress`设置为 true 开启压缩，`mapreduce.map.output.compress.codec`指定压缩Codec。参见 4.2 节。
- Reducer使用HTTP方式从Mapper获取其结果。用于进行文件分区的线程最大数**由** `mapreduce.shuffle.max.threads`控制，该属性针对每个Node Manager，并非每个MapTask。默认值为 0，即该机器上处理器个数的两倍。

#### 6.4.2 Reduce部分

ReduceTask的步骤。

- 任意一个MapTask完成后，ReduceTask就会开始复制数据。ReduceTask有 `mapreduce.reduce.shuffle.parallelcopies`个复制线程并行复制数据，默认为5。
  MapTask完成后，会通过心跳机制通知Application Master。ReduceTask有一个线程周期性的向AM请求已完成MapTask的Host，直到拿到所有MapTask的Host。
  为避免Reducer故障导致数据丢失，MapTask所在的节点不会在ReduceTask获取完数据后就执行删除操作。而是等到Job执行完毕后，由Application Master通知删除。
- 如果MapTask输出数据足够小，ReduceTask会将其复制到JVM内存中。`mapreduce.reduce.shuffle.input.buffer.percent`属性控制JVM堆内存用于缓冲数据大小的百分比。如果较大，会复制到磁盘。如果缓冲数据占用内存空间达到阈值（`mapreduce.reduce.shuffle.merge.percent`），或者复制的MapTask输出数据达到阈值（`mapreduce.reduce.merge.inmem.threshold`），缓冲数据会被Merge并溢写到磁盘。如果指定了Combiner，在Merge时会执行Combiner，减少写磁盘数据的大小。
- 后台线程会将拷贝来的文件合并为更大的排序好的文件。压缩过的文件会在内存中解压。
- 所有MapTask的数据都拷贝完后，进入合并排序阶段，将所有数据合并排序。合并操作会循环进行，比如有50个文件，合并因子（`mapreduce.task.io.sort.factor`，默认 10）为10，将会合并5次，每次合并10个文件，最后成为5个大文件。
- 最后的 5个文件数据不再合并一个文件，直接进入Reduce计算阶段。最后的合并可以来自内存和磁盘片段混合（ This final merge can come from a mixture of in-memory and on-disk segments）。
- Reduce计算阶段，Reduce函数会对每个Key进行调用。如果最终结果是输出到HDFS的，那么HDFS上的第一个副本Block会保存在当前Node Manager所在的节点。
- 注：合并阶段，文件合并的过程实际更加微妙，并不是如例子所说。合并目标是合并最小数量的文件以满足最后一轮合并的合并因子。比如有40个文件，第一轮合并4个文件，后面3轮每轮合并10个，那么有 1+3+6=10 个文件，第五轮交给Reduce计算阶段。
  注意这不会改变合并的次数，只是一个优化措施，用来最小化写入到磁盘的数据量。
  ![MapReduce - Shuffle & Sort - Reduce - Merge](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\MapReduce - Shuffle & Sort - Reduce - Merge.png)

#### 6.4.3 配置调优

##### Map端

| 属性                                  | 类型        | 默认                                       | 描述                                       |
| ----------------------------------- | --------- | ---------------------------------------- | ---------------------------------------- |
| mapreduce.task.io.sort.mb           | int       | 100                                      | Mapper输出缓冲区大小，单位MB                       |
| mapreduce.map.sort.spill.percent    | float     | 0.80                                     | Mapper缓冲区空间占比阈值，超过执行溢写                   |
| mapreduce.task.io.sort.factor       | int       | 10                                       | 排序时，一次最多合并的Stream数，Mapper/Reducer通用。该项增加到100也很常见 |
| mapreduce.map.combine.minspills     | int       | 3                                        | 运行Combiner所需的最少溢写文件数                     |
| mapreduce.map.output.compress       | boolean   | false                                    | MapTask输出是否压缩                            |
| mapreduce.map.output.compress.codec | ClassName | org.apache.hadoop.io.compress.DefaultCodec | 压缩编码解码器                                  |
| mapreduce.shuffle.max.threads       | int       | 0                                        | 每个Node Manager上，将MapTask输出发送到Reducer的工作线程数。该项为集群设置，不能为Job单独指定。 |

总的原则是给Shuffle提供尽可能大的内存空间，同时要确保Map函数和Reduce函数有足够的内存来运行。因此编写这两个函数时要尽量少使用内存。
运行Map/Reduce Task的JVM的大小由 `mapred.child.java.opts`控制，在任务节点上应该将这个值设的尽可能大。
在Map端，尽量避免溢写以获得最佳性能，一次是最理想情况。如果能估算MapTask输出的大小，可以调整`mapreduce.task.io.sort.*`各项来优化。尤其是应该增大`mapreduce.task.io.sort.mb`。有一个MapReduce计数器可以记录溢写磁盘的次数，计数中包括了Map/Reduce两部分的次数，参见 8.1 节。

##### Reduce端

| 属性                                       | 类型    | 默认   | 描述                                       |
| ---------------------------------------- | ----- | ---- | ---------------------------------------- |
| mapreduce.reduce.shuffle.parallelcopies  | int   | 5    | 从Mapper复制数据的线程数                          |
| mapreduce.reduce.shuffle.maxfetchfailures | int   | 10   | 从Mapper复制一份数据，用时超过此时间会报告一个错误             |
| mapreduce.task.io.sort.factor            | int   | 10   | 排序时一次最多合并的Stream数，Map/Reduce通用           |
| mapreduce.reduce.shuffle.input.buffer.percent | float | 0.70 | Shuffle阶段，复制Map数据占JVM堆空间最大百分比            |
| mapreduce.reduce.shuffle.merge.percent   | float | 0.66 | Map输出缓冲区使用比例（`mapred.job.shuffle.input.buffer.percent`定义）的阈值，到达后启动合并和溢写 |
| mapreduce.reduce.merge.inmem.threshold   | int   | 1000 | Map输出数据数量阈值，到达后启动合并和溢写。如果设为0或更小，溢写行为由`mapreduce.reduce.shuffle.merge.percent`控制 |
| mapreduce.reduce.input.buffer.percent    | float | 0.0  | Reduce过程中，内存中保存的Map输出占整个空间的百分比上限。Reduce计算阶段开始时，内存中Map输出不能大于这个值。默认情况下，Reduce计算阶段开始时，所有Map输出都应该合并到磁盘，以为Reduce计算腾出内存。但如果Reduce计算所需内存较小，也可以适当增大这个值以减少磁盘读写次数。 |

Reduce阶段，所有中间数据全部驻留内存，性能最佳。在默认情况下这是不可能的，因为所有内存一般预留给Reduce函数。但如果Reduce函数对内存需求不大，将`mapreduce.reduce.merge.inmem.threshold`设为 0，`mapreduce.reduce.input.buffer.percent`设为 1（或一个低一些的值），可以带来更好的性能表现。
更常见的情况是，Hadoop使用默认为4K的缓冲区，这是很低的，应该在集群中增大这个值（ `io.file.buffer.size`）。

### 6.5 任务执行

#### 6.5.1 任务执行环境

MapTask和ReduceTask可以通过Hadoop提供的环境相关信息中，获得任务相关的信息。通过调用 `Mapper` 或者 `Reducer` 的`configure()`方法来获取。

| 属性                        | 类型      | 描述              | 示例                                       |
| ------------------------- | ------- | --------------- | ---------------------------------------- |
| mapreduce.job.id          | string  | Job ID          | job\_200811201130\_0004                  |
| mapreduce.task.id         | string  | Task ID         | task\_200811201130\_0004\_m\_000003      |
| mapreduce.task.attempt.id | string  | Task Attempt ID | attempt\_200811201130\_0004\_m\_000003\_0 |
| mapreduce.task.partition  | int     | Job中作业的序号       | 3                                        |
| mapreduce.task.ismap      | boolean | 是否MapTask       | true                                     |

##### Streaming环境变量

Hadoop将配置属性设为Streaming程序的环境变量，并将非数字字母的字符替换为下划线保证名称合法性。如下面的Python语句所示。

```python
os.environ["mapreduce_job_id"]
```

#### 6.5.2 推测执行

当某个Task运行比预期慢时，Hadoop会尽量检测原因，并启动另一个相同的Task Attempt作为备份。这就是推测执行。
备份执行完成后，原Task会被终止；同理，原Task先完成备份会被终止。
推测执行是一种优化措施，不能保证Task一定成功。如果是因为软件缺陷导致Task失败，推测执行Task可能会因为相同的缺陷反复失败。

推测执行默认是开启的，可以为Job单独指定，也可以为MapTask、ReduceTask单独指定。

| 属性                                       | 类型      | 默认值                                      | 描述                                       |
| ---------------------------------------- | ------- | ---------------------------------------- | ---------------------------------------- |
| mapreduce.map.speculative                | boolean | true                                     | MapTask是否开启                              |
| mapreduce.reduce.speculative             | boolean | true                                     | ReduceTask是否开启                           |
| yarn.app.mapreduce.am.job.speculator.class | Class   | org.apache.hadoop.mapreduce.v2.app.speculate.DefaultSpeculator | Speculator类实现了推测执行策略                     |
| yarn.app.mapreduce.am.job.task.estimator.class | Class   | org.apache.hadoop.mapreduce.v2.app.speculate.LegacyTaskRuntimeEstimator | Speculator实例使用TaskRuntimeEstimator的实现来得到Task执行时间的估计值 |

哪些情况需要关闭推测执行？

- 在运行繁忙的集群中，推测执行会降低整个集群的效率，关闭集群推测执行，让用户根据Job的具体需求决定是否开启。
- 对ReduceTask的推测执行，会大幅增加集群内数据传输，可以考虑关闭。
- 对非幂等的Task，需要关闭推测执行。

#### 6.5.3 关于OutputCommitter

Hadoop使用输出协议保证Job的Task全部成功或失败。具体动作由OutputCommitter执行。可以使用`job.getOutputFormatClass().newInstance().getOutputCommitter().getClass()`接口确认，默认使用FileOutputCommitter。可以自行定制。

OutputCommitter的代码。

```java
public abstract class OutputCommitter {
    public abstract void setupJob(JobContext jobContext) throws IOException;

    public void commitJob(JobContext jobContext) throws IOException {
    }

    public void abortJob(JobContext jobContext, JobStatus.State state)
            throws IOException {
    }

    public abstract void setupTask(TaskAttemptContext taskContext)
            throws IOException;

    public abstract boolean needsTaskCommit(TaskAttemptContext taskContext)
            throws IOException;

    public abstract void commitTask(TaskAttemptContext taskContext)
            throws IOException;

    public abstract void abortTask(TaskAttemptContext taskContext)
            throws IOException;
}
```

- setupJob() - 在Job运行前调用，执行初始化操作。FileOutputCommitter会为任务创建最终输出目录`${mapreduce.output.fileoutputformat.outputdir}` 和临时工作目录`${mapreduce.output.fileoutputformat.outputdir}/_temporary`。
- commitJob() - Job成功后调用。FileOutputCommitter会删除临时工作目录，在最终输出目录创建*_SUCCESS*文件以告知文件系统Client端Job已完成。
- abortJob() - Job失败或被终止时调用，删除临时工作目录。
- setupTask() - 在Task执行前调用。FileOutputCommitter不做任何事情，Task的临时输出目录是在写任务输出时创建的。
- needsTaskCommit() - 返回false可以关闭Task提交。Task提交阶段是可选的，关闭后框架不需要再为Task运行分布式提交协议，也不会再调用commitTask()和abortTask()。FileOutputCommitter会在Task没有输出时跳过commit阶段。
- commitTask() - 在Task成功时调用。FileOutputCommitter会将Task临时输出目录（带有Attempt Index）内容移动到最终输出目录。
- abortTask() - 在Task失败时调用。FileOutputCommitter会删除Task临时输出目录。

##### Task附属文件（Task side-effect files）

总体意思就是Task Attempt写的文件在临时目录中，成功的会移动到输出目录，其他的会被删除。

#### 6.5.4 JVM重用

YARN中即对于*uber task*的特殊处理，参见6.1.4.

#### 6.5.5 Skip Mode

对于Bad Record的跳过处理，YARN不支持。