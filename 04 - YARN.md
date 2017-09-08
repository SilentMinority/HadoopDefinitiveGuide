## 四、YARN

Apache YARN是一款Hadoop集群资源管理系统，在Hadoop 2.x引入。YARN提供了请求和使用集群资源的API，但用户一般不会直接使用这些API，而是使用计算框架（MapReduce、Spark等等）提供的高级API。

![YARN - Layer](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\YARN - Layer.png)

### 4.1 YARN应用运行剖析

YARN有两种类型的节点：每集群一个 *ResouceManager*， 负责管理整个集群的资源使用。每节点一个 *NodeManager*，负责加载和监控 *Container* 。
*Container* 负责使用一组有限的资源（CPU、内存）运行Application指定的进程。一个Container可以是一个Unix进程或一个Linux cgroup。

![](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\YARN - Application.png)

YARN Application运行流程。

1. Client 连接ResouceManager，提交YARN Application，请求运行一个ApplicationMaster。
2. RM找一个NodeManager，启动一个Container加载ApplicationMaster。
   AM加入到集群中运行是Cluster模式。Non-Cluster模式下，AM在集群外（比如Client的JVM中）运行。
3. AM启动后的动作取决于Application，它可以在当前结点直接进行计算并返回结果。上图中AM向RM请求更多的资源。
4. Application在请求道的资源上进行分布式计算。
5. TIP，YARN本身不提供任何方法让Application的各部分之间（Client、AM、Process）进行通信，需要Application自行将运行状态和计算结果传递回Client。

### 4.2 资源请求

YARN对于资源请求有一个较松散的模型，对于一组Container的一次请求可以表达在每台设备上请求的资源数量，以及对于Container的局部约束。
严格的局部约束保证集群带宽被充分利用，因此YARN允许Application为其请求的Container指定局部约束。
有时局部约束无法被满足，此时无法分配资源，或者可能约束会放松。比如请求的节点正在运行其他Container，此时YARN会尝试在同机架其他节点，甚至不同机架节点上启动Container。

通常情况下，Application会在要处理的HDFS Block的副本所在的节点上请求Container。失败时选择同机架其他节点，再失败选择其它机架。

YARN Application可以在其运行的任何时候请求资源。比如Spark在作业最初请求所需的全部资源，就是粗粒度资源调度。MapReduce在map阶段不会请求reduce阶段需要的资源，而是在map结束后再请求，同时在出现Task失败时会请求资源RerunTask，就是细粒度资源调度。

### 4.3 Application生命周期

Application生命周期的时间跨度非常大，从几秒钟到几天甚至几个月都有，因此按照时间长度对Application归类意义不大。一般按照Application映射用户程序的方式进行分类。

- 最简单的，每个用户Job一个Application。典型的如MapReduce。
- 第二种模式是为每个工作流或用户会话（可能不相关）的Job运行一个应用程序。这种方式下，Container可以在Job间复用，并且可以缓存不同Job的中间数据，因此比第一种更有效率。典型如Spark。
- 第三种是一个长时间运行的Application被多个用户共享。这种方式避免了启动Application Master的消耗，因此对用户请求的响应延迟非常低。典型如Apache Slider、Impala。Impala设置一个代理Application，Impala实例与这个代理Application通信请求集群资源。

### 4.4 YARN程序开发

可以使用Apache Slider、Apache Twill或者YARN原生API进行YARN程序开发。
Slider 下，用户可以独立于其他用户运行自己的Application。Slider提供了对Application使用节点数量的控制，可以暂停Application，修改节点数量然后恢复。
Twill 提供了编程模型，允许用户将集群处理定义为*JavaRunnable*的继承并运行到Container中。Twill提供了实时日志（发回Client端）和消息命令（Client发到Container）。
也可以直接使用YARN API，YARN提供了实例脚本。

### 4.5 YARN与MapReduce 1 对比

Hadoop从2.x引入YARN，之后的MapReduce称为MapReduce 2。

#### 1. 集群角色对比

| MR 1        | YARN                                     |
| ----------- | ---------------------------------------- |
| JobTracker  | Resource manager, application master, timeline server |
| Tasktracker | Node manager                             |
| Slot        | Container                                |

- MR 1中，JobTracker负责对集群中所有Job的执行进行规划（分配到匹配的TaskTracker），监控任务进度（跟踪Task；Rerun失败或过慢Task；Task记账管理，比如计数），保存执行完成Job的历史（也可以启动一个Job History Server负责这一部分工作）。
  YARN中，上述任务被分别分配给ResourceManager和ApplicationMaster。
  另外，YARN引入了TimelineServer代替JobHistoryServer，但目前TS尚不能存储MR的执行历史，因此JHS仍未被代替。
- YARN使用Node Manager代替了Task Tracker。

#### 2. 更多优点

- 扩展性

  MR 1 最高支持4000节点，40000 Task。YARN最高支持 10000节点，100000 Task。

- 可用性

  MR 1 中由于Job Tracker负责整个集群状态的监控，导致其内存变动十分复杂，高可用实现也随之变得困难。YARN将Application管理交给了Application Master，使HA方案变得简单不少。

- 利用率

  MR 1 中，Task运行单元称为Slot。Slot大小是固定不可分割的，每个TaskTracker内的Slot在配置完后也不可修改。并且Slot分为Map Slot和Reduce Slot，两者不同用。这样导致了经常出现资源浪费。
  YARN中Container对MapTask和ReduceTask是通用的，并进行了更好的颗粒度划分适应不同大小资源要求的Task。

- 多租户

  从某种角度来说，YARN带来的最大好处，是使Hadoop可以通过YARN适配多种分布式计算框架，而不是只能用于MapReduce。在一个YARN集群上运行多个不同版本的MapReduce甚至是可能的。

### 4.6 资源调度

YARN有三种资源调度器。FIFO Scheduler、Capacity Scheduler、Fair Scheduler。

- FIFO - 先到先服务，YARN默认选择项。按照Job提交的顺序分配资源。集群资源被占满时，后续Job需要等待。可能出现大型Job阻塞集群。
- Capacity - YARN会为小型作业保留一个单独的资源队列。小型作业可以迅速完成，但没有小型作业时该部分资源会被闲置。
- Fair - 公平调度。所有作业分配到相等的资源。比如在集群资源被大型作业耗尽的情况下，有新作业到达时，YARN会在大型作业释放出一半资源后将其分配给新作业。

![YARN - Scheduling - Overview](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\YARN - Scheduling - Overview.png)

#### 4.6.1 Capacity Scheduler配置

Capacity Scheduler允许用户以队列形式管理集群资源，每个队列可以被分配一部分集群资源。队列可以继续划分子队列。在同一队列内部，按照FIFO策略进行资源调度。
队列弹性。一个Job可能会要求超过当前队列的资源，此时如果集群中有其它空闲资源，队列可以设定的大小，临时占用其他队列资源，这称为队列弹性。
正常情况下，Capacity Scheduler不会强制结束Container。当一个队列没有足够的作业时，它的资源可能会被其他队列占用。这时如果有新的作业加入，队列资源只能恢复其它队列Container执行完毕后释放的资源。因此应该为队列资源量设置一个最大值，避免队列弹性情况下占用其他队列资源过多。

来看一个*capacity-scheduler.xml*配置示例。

```xml
<?xml version="1.0"?>
<configuration>
	<property>
		<name>yarn.scheduler.capacity.root.queues</name>
		<value>prod,dev</value>
	</property>
	<property>
		<name>yarn.scheduler.capacity.root.dev.queues</name>
		<value>eng,science</value>
	</property>
	<property>
		<name>yarn.scheduler.capacity.root.prod.capacity</name>
		<value>40</value>
	</property>
	<property>
		<name>yarn.scheduler.capacity.root.dev.capacity</name>
		<value>60</value>
	</property>
	<property>
		<name>yarn.scheduler.capacity.root.dev.maximum-capacity</name>
		<value>75</value>
	</property>
	<property>
		<name>yarn.scheduler.capacity.root.dev.eng.capacity</name>
		<value>50</value>
	</property>
	<property>
		<name>yarn.scheduler.capacity.root.dev.science.capacity</name>
		<value>50</value>
	</property>
</configuration>
```

上面的XML代码配置下面结构的队列。其中的数字代表队列占用资源的百分比。`maximum-capacity`表示队列最大资源占比，为队列弹性增加限制。

```xml
root
├── prod
└── dev
 ├── eng
 └── science
```

##### 4.6.1.1 Application队列选择

Application加入哪个队列在Application中配置。比如对于MapReduce Job，`mapreduce.job.queue`配置队列名称。未配置或配置队列不存在，会加入`default`队列。
注意，队列名只写队列最后一段，全路径不起作用。比如`root.dev.eng`是无效名称。

#### 4.6.2 Fair Scheduler配置

公平调度同样使用用户-队列模型实现。假设有A、B两个用户分别使用一般集群资源在运行作业，此时B有新作业加入。新作业会占用B已有资源的一半，即整个集群资源的一半。
![YARN - Scheduling - Fair](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\YARN - Scheduling - Fair.png)

##### 4.6.2.1 开启Fair Scheduler

 *yarn-site.xml* 中`yarn.resourcemanager.scheduler.class`项指定了Scheduler类，默认是Capacity Scheduler，修改为`org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler`。

##### 4.6.2.2 队列配置

`yarn.scheduler.fair.allocation.file`指定Fair Scheduler配置文件名称，默认 `fair-scheduler.xml`。YARN会从classpath查找对应文件。

配置文件示例。

```xml
<?xml version="1.0"?>
<allocations>
	<defaultQueueSchedulingPolicy>fair</defaultQueueSchedulingPolicy>
	<queue name="prod">
		<weight>2</weight>
		<schedulingPolicy>fifo</schedulingPolicy>
	</queue>
	<queue name="dev">
		<weight>3</weight>
		<queue name="eng" />
		<queue name="science" />
	</queue>
	<queuePlacementPolicy>
		<rule name="specified" create="false" />
		<rule name="primaryGroup" create="false" />
		<rule name="default" queue="dev.eng" />
	</queuePlacementPolicy>
</allocations>
```

所有队列都是 *root* 队列的子队列，即使不嵌套在 *root* 中。
队列都有权重，可以手动配置，*default* 队列和自动创建的队列权重为 1。
`defaultQueueSchedulingPolicy`顶级元素配置了队列内的默认调度策略。可以使用FIFO（fifo）、Dominant Resource Fairness（drf）等。默认使用公平调度。
`schedulingPolicy`队列内部元素覆盖默认`defaultQueueSchedulingPolicy`。
示例中没有展示，但每个队列可以设置最小与最大资源数，和最多运行Application数。其中最小资源数不是强制要求队列达到的资源数量，而是作为调度器分配资源的优先考虑依据。如果两个队列都未达到公平分配，已分配资源与最小资源数差距大的会被优先分配。

4.6.2.3 Application队列选择

Fair Scheduler基于规则进行队列选择。顶级元素`queuePlacementPolicy`包含一系列选择规则。
示例代码中，优先Application指定的队列；未指定不根据用户的Primary Unix Group创建；进入 *default* 队列。

`queuePlacementPolicy`未配置时，默认规则如下。

```xml
<queuePlacementPolicy>
 <rule name="specified" />
 <rule name="user" />
</queuePlacementPolicy>
```

也可以不使用资源调度配置文件，将`yarn.scheduler.fair.user-as-default-queue`设为false将Job放入*default* 队列。同时设置`yarn.scheduler.fair.allow-undeclared-pools`为false阻止用户即时创建队列。

##### 4.6.2.3 抢占

抢占策略允许Scheduler杀死Container以释放资源。被杀死的Container需要重新执行。
将 `yarn.scheduler.fair.preemption` 设置为true，启用抢占策略。抢占策略基于超时时间触发，有两种超时条件：未达到最小资源数超时；未达到公平调度超时。两者单位都是秒，且没有默认值。
未达到最小资源超时，通过`defaultMinSharePreemptionTimeout`顶级元素设置超时时间， `minSharePreemptionTimeout`设置所在队列时间值。
未达到公平调度超时有两个值。`defaultFairSharePreemptionTimeout`顶级元素和`fairSharePreemptionTimeout`设置默认和队列超时时间。`defaultFairSharePreemptionThreshold`和`fairSharePreemptionThreshold`设置未达到公平调度的阈值，默认0.5。即Application分配到的资源未达到其公平调度资源值的一半，并超时时，触发抢占。

#### 4.6.3 延迟调度

很多时候Application请求使用某个节点时，节点可能正在被其他Container占用。大多时候，等待一小段时间（几秒钟）等节点上的Container结束比立即寻找同机架更有效率。这就是延迟调度的作用。
NodeManager会定时向ResourceManager发送心跳（默认每秒一次）。心跳中包含NM当前正在运行的Container、剩余可用资源等信息。因此一次心跳对在等待的Application，可以认为是一次运行Container的**调度机会**。
对于Capacity Scheduler，将`yarn.scheduler.capacity.node-locality-delay`设置为大于 0 的正数，表示Application在等待到多少个集群中任何节点发来的调度机会后，就会切换到同机架其他节点。
对于Fair Scheduler，设置`yarn.scheduler.fair.locality.threshold.node`为小于 1 的小数，表示Application在等待集群中多少比例的节点都发送调度机会后，就会切换到同机架其他节点。还有`yarn.scheduler.fair.locality.threshold.rack`表示切换到其他机架。

#### 4.6.4 主导资源调度

默认配置下，YARN在执行资源调度时，只根据Application对内存的需求分配Container。当需要将CPU也考虑进来时，会变得复杂起来。
YARN引入了Dominant Resource Fairness（drf）算法来进行调度。例如，集群总资源（100 CPU，10 TB Memory）。Application *A* 每个Container请求（2 CPU，300 GB），Application *B* 每个Container请求（6 CPU，100 GB）。A 的请求占总资源比为（2 %，3 %），主导资源是内存；B 为（6 %，1 %），主导资源是CPU。由于B的主导资源占比（6%）是A的（3%）的两倍，因此在公平调度策略下，B会被分到A一半数量的Container。

DRF默认是关闭的。Capacity Scheduler需要在 *capacity-scheduler.xml* 中将 `yarn.scheduler.capacity.resource-calculator` 配置为 `org.apache.hadoop.yarn.util.resource.DominantResourceCalculator` ，Fair Scheduler需要将分配配置文件中的顶级元素 `defaultQueueSchedulingPolicy` 设置为 `drf`。