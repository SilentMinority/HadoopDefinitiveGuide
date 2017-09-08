## 五、MapReduce开发

MapReduce开发的大概流程：

- 编写Map函数和Reduce函数
- 在本地使用小型数据集进行单元测试
- Unit Test通过后放入集群中
- 任务剖析（Task Profiling），使用钩子（hook）优化调整

### 5.1 Configuration API

Hadoop使用`org.apache.hadoop.conf.Configuration`读取配置信息。来看个例子

```xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>color</name>
        <value>yellow</value>
        <description>Color</description>
    </property>

    <property>
        <name>size</name>
        <value>10</value>
        <description>Size</description>
    </property>

    <property>
        <name>weight</name>
        <value>heavy</value>
        <final>true</final>
        <description>Weight</description>
    </property>

    <property>
        <name>size-weight</name>
        <value>${size},${weight}</value>
        <description>Size and weight</description>
    </property>
</configuration>
```

读取代码

```java
Configuration conf = new Configuration();
conf.addResource("configuration-1.xml");
assertThat(conf.get("color"), is("yellow"));
// 指定类型和默认值
assertThat(conf.getInt("size", 0), is(10));
// 指定默认值
assertThat(conf.get("breadth", "wide"), is("wide"));
```

#### 5.1.1 资源合并

加载多个配置文件，较后加载的配置会覆盖前面的。将属性标记为final可以防止被覆盖。`core-default.xml`和`core-site.xml`使用了这种特性。

```java
 Configuration conf = new Configuration();
 conf.addResource("configuration-1.xml");
 conf.addResource("configuration-2.xml");
```

#### 5.1.2 可变扩展

可以用直接定义系统属性的方式，定义Hadoop属性。系统属性的优先级高于配置文件的定义，不会被覆盖。
如果系统定义了配置文件中没有的配置项，无法使用Configuration读取这些配置项。

```java
assertThat(conf.get("size-weight"), is("12,heavy"));
System.setProperty("size", "14");
assertThat(conf.get("size-weight"), is("14,heavy"));
System.setProperty("length", "2");
assertThat(conf.get("length"), is((String) null));
```

### 5.2 配置开发环境

一些基础Maven依赖。

```xml
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.hadoopbook</groupId>
    <artifactId>hadoop-book-mr-dev</artifactId>
    <version>4.0</version>
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <hadoop.version>2.5.1</hadoop.version>
    </properties>
    <dependencies>
        <!-- Hadoop main client artifact -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
        </dependency>
        <!-- Unit test artifacts -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.11</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.mrunit</groupId>
            <artifactId>mrunit</artifactId>
            <version>1.1.0</version>
            <classifier>hadoop2</classifier>
            <scope>test</scope>
        </dependency>
        <!-- Hadoop test artifact for running mini clusters -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-minicluster</artifactId>
            <version>${hadoop.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    <build>
        <finalName>hadoop-examples</finalName>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.1</version>
                <configuration>
                    <source>1.6</source>
                    <target>1.6</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <version>2.5</version>
                <configuration>
                    <outputDirectory>${basedir}</outputDirectory>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

#### 5.2.1 配置管理

三个配置文件，分别指向本地、本地伪分布式、分布式集群。

```xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>file:///</value>
    </property>

    <property>
        <name>mapreduce.framework.name</name>
        <value>local</value>
    </property>

</configuration>
```

```xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost/</value>
    </property>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>localhost:8032</value>
    </property>

</configuration>
```

```xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode/</value>
    </property>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>resourcemanager:8032</value>
    </property>
</configuration>
```

在Hadoop命令中，可以使用`-conf`使用配置文件。如果省略`-conf`，默认从`$HADOOP_INSTALL`路径下查找配置文件。

```shell
% hadoop fs -conf conf/hadoop-localhost.xml -ls .
```

#### 5.2.2 命令行辅助类

GenericOptionsParser, Tool 和 ToolRunner是Hadoop提供的简化命令行使用的辅助类。下面的代码会打印出所有的配置项。

```java
public class ConfigurationPrinter extends Configured implements Tool {

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("yarn-default.xml");
        Configuration.addDefaultResource("yarn-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        for (Entry<String, String> entry: conf) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
        System.exit(exitCode);
    }
}
```

这样使用示例代码。

```shell
% mvn compile
% export HADOOP_CLASSPATH=target/classes/
% hadoop ConfigurationPrinter -conf conf/hadoop-localhost.xml | grep yarn.resourcemanager.address=
yarn.resourcemanager.address=localhost:8032
```

**GenericOptionsParser 和 ToolRunner支持的选项**-D fs.defaultFS=uri

| Option                               | Description                              |
| ------------------------------------ | ---------------------------------------- |
| -D *property=value*                  | 为配置项赋值，会覆盖配置文件的配置                        |
| -conf *filename* ... ...             | 添加配置文件                                   |
| -fs *uri*                            | *-D fs.defaultFS=uri* 的简写                |
| -jt *host:port*                      | *-D yarn.resourcemanager.address=host:port* 的简写 |
| -files *file1,file2... ...*          | 将本地文件拷贝至分布式文件系统                          |
| -archives *archive1,archive2... ...* | 拷贝存档至分布式文件系统并打开，确保对MapReduce程序可用         |
| -libjars *jar1,jar2... ...*          | 将本地JAR拷贝至分布式文件系统，并加入MapReduce的类路径        |

### 5.3 MRUnit单元测试

MRUnit是Apache提供的测试库，方便对Map函数和Reduce函数进行测试。

#### 5.3.1 Mapper

```java
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

public class MaxTemperatureMapperTest {
    @Test
    public void processesValidRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                // Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00111+99999999999");
        // Temperature ^^^^^
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .runTest();
    }
}
```

```java
public class MaxTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String year = line.substring(15, 19);
        int airTemperature = Integer.parseInt(line.substring(87, 92));
        context.write(new Text(year), new IntWritable(airTemperature));
    }
}
```

```java
    @Test
    public void ignoresMissingTemperatureRecord() throws IOException,
            InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                // Year ^^^^
                "99999V0203201N00261220001CN9999999N9+99991+99999999999");
        // Temperature ^^^^^
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .runTest();
    }
```

#### 5.3.2 Reducer

```java
    @Test
    public void returnsMaximumIntegerInValues() throws IOException,
            InterruptedException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(new Text("1950"),
                        Arrays.asList(new IntWritable(10), new IntWritable(5)))
                .withOutput(new Text("1950"), new IntWritable(10))
                .runTest();
    }
```

```java
public class MaxTemperatureReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int maxValue = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
        }
        context.write(key, new IntWritable(maxValue));
    }
}
```

### 5.4 本地运行

#### 5.4.1 本地JobRunner

```java
public class MaxTemperatureDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job(getConf(), "Max temperature");
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
        System.exit(exitCode);
    }
}
```

Hadoop有一个精简版的JobRunner，专门为运行在单个JVM上设计，非常方便于本地测试，可以在其上进行Debug。 
`mapreduce.framework.name`设置为`local`时，就会启动本地JobRunner，默认就是local。
上面的代码可以用下面两种方式运行。

```shell
% mvn compile
% export HADOOP_CLASSPATH=target/classes/
% hadoop v2.MaxTemperatureDriver -conf conf/hadoop-local.xml \
 input/ncdc/micro output
```

```shell
% hadoop v2.MaxTemperatureDriver -fs file:/// -jt local input/ncdc/micro output
```

查看运行结果。

```shell
% cat output/part-r-00000
1949 111
1950 22
```

#### 5.4.2 测试Driver

除了本地运行测试外，还可以使用MiniCluster进行测试。

```java
// 使用正在运行的本地JobRunner进行测试
    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("input/ncdc/micro");
        Path output = new Path("output");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);

        int exitCode = driver.run(new String[] {
                input.toString(), output.toString() });
        assertThat(exitCode, is(0));

        checkOutput(conf, output);
    }
```

Hadoop提供了一组测试类，MiniDFSCluster, MiniMRCluster, 和 MiniYARNCluster。可以使用这些类以程序方式创建正在运行的集群。
可以使用命令行运行一个Mini Cluster。

```shell
% hadoop jar \
 $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-*-tests.jar \
 minicluster
```

### 5.5 集群运行

在集群上运行时，必须将Job程序打包，发送到集群上去运行。

#### 5.5.1 打包Job

使用Maven或Ant等创建JAR文件，Hadoop会自动在Classpath中查找包含JobConf.setJarByClass()设置类的JAR，从而找到Job程序的JAR。
如果每个JAR包都有一个Job，需要在manifest文件中指定MainClass，否则必须在命令行制定。

1. 客户端classpath

   可以使用`hadoop jar \<JAR File>`命令指定客户端classpath。其中包含了

   - Job的JAR文件
   - Job Jar文件的*lib*路径下的所有JAR文件，和类目录（如果定义）
   - HADOOP_CLASSPATH指定的类路径（如果定义）

2. Task的classpath

   集群中，Mapper和Reducer在各自的JVM中运行，classpath不受HADOOP_CLASSPATH控制，H_C只是对Driver所在JVM的classpath进行了设置。
   Task的Classpath包含以下

   - Job的JAR文件
   - Job Jar文件的*lib*路径下的所有JAR文件，和类目录（如果定义）
   - `-libjars`选项指定的，或者Job.addFileToClassPath()方法添加的JAR文件

3. 打包Dependencies

   对于Dependency有以下方式进行打包

   - 将依赖库解包后，加入Job JAR重新打包
   - 对Job JAR的lib目录进行打包
   - 将依赖JAR和Job JAR分开，将依赖JAR加入到HADOOP_CLASSPATH中，并用`-libjars`选项添加到Task的Classpath中。

4. Task的Classpath优先级

   用户代码使用的库版本和Hadoop的可能会有冲突。在Client端，将 HADOOP_USER_CLASSPATH_FIRST 设为 true，强制Hadoop优先搜索用户类库。在Task端， `mapreduce.job.user.classpath.first`设为 true。
   注意这可能导致Hadoop任务提交、运行异常。

#### 5.5.2 启动Job

```shell
% unset HADOOP_CLASSPATH
% hadoop jar hadoop-examples.jar v2.MaxTemperatureDriver -conf conf/hadoop-cluster.xml input/ncdc/all max-temp
```

Job.waitForCompletion()方法会启动作业，并检查进展、输出日志。

```
14/09/12 06:38:11 INFO input.FileInputFormat: Total input paths to process : 101
14/09/12 06:38:11 INFO impl.YarnClientImpl: Submitted application
application_1410450250506_0003
14/09/12 06:38:12 INFO mapreduce.Job: Running job: job_1410450250506_0003
14/09/12 06:38:26 INFO mapreduce.Job: map 0% reduce 0%
...
14/09/12 06:45:24 INFO mapreduce.Job: map 100% reduce 100%
14/09/12 06:45:24 INFO mapreduce.Job: Job job_1410450250506_0003 completed
successfully
14/09/12 06:45:24 INFO mapreduce.Job: Counters: 49
   File System Counters
   FILE: Number of bytes read=93995
   FILE: Number of bytes written=10273563
   FILE: Number of read operations=0
   FILE: Number of large read operations=0
   FILE: Number of write operations=0
   HDFS: Number of bytes read=33485855415
   HDFS: Number of bytes written=904
   HDFS: Number of read operations=327
   HDFS: Number of large read operations=0
   HDFS: Number of write operations=16
   Job Counters
   Launched map tasks=101
   Launched reduce tasks=8
   Data-local map tasks=101
   Total time spent by all maps in occupied slots (ms)=5954495
   Total time spent by all reduces in occupied slots (ms)=74934
   Total time spent by all map tasks (ms)=5954495
   Total time spent by all reduce tasks (ms)=74934
   Total vcore-seconds taken by all map tasks=5954495
   Total vcore-seconds taken by all reduce tasks=74934
   Total megabyte-seconds taken by all map tasks=6097402880
   Total megabyte-seconds taken by all reduce tasks=76732416
   Map-Reduce Framework
   Map input records=1209901509
   Map output records=1143764653
   Map output bytes=10293881877
   Map output materialized bytes=14193
   Input split bytes=14140
   Combine input records=1143764772
   Combine output records=234
   Reduce input groups=100
   Reduce shuffle bytes=14193
   Reduce input records=115
   Reduce output records=100
   Spilled Records=379
   Shuffled Maps =808
   Failed Shuffles=0
   Merged Map outputs=808
   GC time elapsed (ms)=101080
   CPU time spent (ms)=5113180
   Physical memory (bytes) snapshot=60509106176
   Virtual memory (bytes) snapshot=167657209856
   Total committed heap usage (bytes)=68220878848
   Shuffle Errors
   BAD_ID=0
   CONNECTION=0
   IO_ERROR=0
   WRONG_LENGTH=0
   WRONG_MAP=0
   WRONG_REDUCE=0
   File Input Format Counters
   Bytes Read=33485841275
   File Output Format Counters
   Bytes Written=90
```

##### 关于Job ID、Task ID格式

在Hadoop 2中，Map Reduce Job ID由YARN Resource Manager生成。格式为[prefix]\_[timestamp]_[number]。Prefix固定为字符串“application”，timestamp是Resource Manager启动的时间戳，number代表当前Job是该Resource Manager接受的第几个Job（从 1 开始）。number长度小于四位时，会在前面补0，保持排序。超过四位，即大于9999时，会打乱排序。

```
application_1410450250506_0003
```

Task ID前半部分是将Job ID的Prefix换为task，后面根据Task的类型（M/R），追加 m/r。再之后是当前Task是该Job的第几个该类型Task（从 0 开始）。下面例子即是当前Job的第四个Map Task。

```
task_1410450250506_0003_m_000003
```

Task会有失败重新执行、推测执行的情况，此时会将前缀修改为“attempt”，并在Task ID后追加一个编号，从 0 开始。

```
attempt_1410450250506_0003_m_000003_0
```
#### 5.5.3 MapReduce Web UI

YARN Resource Manager的Web页面地址，http://resource-manager-host:8088/。

![MapReduce - WebUI - YARN](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\MapReduce - WebUI - YARN.png)

HDFS的NameNode的Web页面地址，http://name-node-host:50070/。

![MapReduce - WebUI - HDFS](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\MapReduce - WebUI - HDFS.png)

不使用YARN时，Job Tracker的Web页面地址，http://resource-manager-host:50030/。没截图。

##### Resource Manager页面

显示集群概览、集群中正在运行Application的一些信息、资源概览、Node Manager的一些信息。
Resource Manager会再内存中保存10000条Application的信息，超出部分会被持久化到HDFS，再Job History页面可以看到。

###### Job History

Job History保存所有完成Map Reduce Job的信息，不论执行成功还是失败。历史文件保存在 `mapreduce.jobhistory.done-dir`配置的HDFS路径下，默认保存一周。
History文件用JSON格式保存Job、Task、Attempt的信息。可以从Web UI查看，或使用 `mapred job -history`命令查看。

#### 5.5.4 获取结果

#### 5.5.5 Debug Job

#### 5.5.6 Hadoop日志

| 日志                        | 主要对象          | 描述                                       |
| ------------------------- | ------------- | ---------------------------------------- |
| System daemon log         | Administrator | 每个Hadoop守护进程产生一个日志文件（log4j），和另一个合并标准输出和错误的文件。保存在HADOOP_LOG_DIR定义的目录下 |
| HDFS 审计日志                 | Administrator | HDFS访问日志，默认关闭。一般写入Namenode日志目录下          |
| MapReduce Job History Log | Users         | Job运行期间发生事件的日志，保存在HDFS上                  |
| MapReduce Task Log        | Users         | 每个Task子进程产生一个*syslog*（使用log4j），一个标准输出文件*stdout*，一个错误日志*stderr*。保存在YARN_LOG_DIR定义目录的*userlogs*子目录下 |

YARN有一个日志聚合服务，默认关闭，配置`yarn.log-aggregation-enable`为true开启。该服务将运行完毕Application的Task日志转移到HDFS上。
在Java代码中，可以使用Apache Common API 写日志到 Task Log。

```java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

public class LoggingIdentityMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);

    @Override
    @SuppressWarnings("unchecked")
    public void map(KEYIN key, VALUEIN value, Context context)
            throws IOException, InterruptedException {
        // Log to stdout file
        System.out.println("Map key: " + key);

        // Log to syslog file
        LOG.info("Map key: " + key);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Map value: " + value);
        }
        context.write((KEYOUT) key, (VALUEOUT) value);
    }
}
```

默认日志级别是INFO，设置 `mapreduce.map.log.level` 或者 `mapreduce.reduce.log.level`来改变级别。或者使用命令行。

```shell
% hadoop jar hadoop-examples.jar LoggingDriver -conf conf/hadoop-cluster.xml -D mapreduce.map.log.level=DEBUG input/ncdc/sample.txt logging-out
```

Task Log 默认保存三小时，`yarn.nodemanager.log.retain-seconds`修改时长。日志聚合服务开启时该属性被忽略。

#### 5.5.7 远程调试

可以用一下手段进行调试。

- 在本地重新产生错误
- 使用JVM调试选项 - 可以在 `mapred.child.java.opts`中包含`-XX:-HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/path/to/dumps`。这会产生一个堆转储，之后可以使用 jhat 等工具检查
- 使用Task分析 - Java提供了Profiler来检查JVM，Hadoop提供了Task分析机制。

设置`mapreduce.task.files.preserve.failedtasks` 为 true，保存MapTask中间文件，用来分析失败MapTask。`mapreduce.task.files.preserve.filepattern`设置匹配Task ID的正则表达式，保存指定MapTask的中间文件。
设置`yarn.nodemanager.delete.debug-delaysec`时长，将Task Attempt 文件保存一段时间，进行查看分析。
登陆到出错节点，到`mapreduce.cluster.local.dir`定义的目录下检查失败Task中间文件。可能定义了一系列逗号分隔的目录（磁盘负载均衡）。Task Attempt的文件在`mapreduce.cluster.local.dir/usercache/user/appcache/application-ID/output/task-attempt-ID`目录下。

### 5.6 Job调优

一些调优建议。

| 调优项       | 建议                                       |
| --------- | ---------------------------------------- |
| Mapper数量  | 通常单个MapTask运行时间在一分钟左右比较合适                |
| Reducer数量 | Reducer数量应略少于ReduceTask数量，单个ReduceTask运行时间在5分钟左右 |
| Combiner  | 充分利用Combiner减少Shuffle传输数据量               |
| 中间值压缩     | 减少Shuffle开销                              |
| 自定义序列化    | 使用自定义Writable或Comparator，必须实现RawComparator |
| 调整Shuffle | 可以对一些内存参数进行调整，弥补Shuffle性能不足              |

#### 5.6.1 Task分析

1. HPROF分析工具

### 5.7 MapReduce工作流

#### 5.7.1 MapReduce作业分解

尽量将复杂处理分解为多个MapReduce Job，而不是采用更加复杂的Map、Reduce函数。

#### 5.7.2 JobControl

使用 `org.apache.hadoop.mapreduce.jobcontrol.JobControl`类，将多个MapReduce Job连接到一起，并且可以加入更复杂的流程。

#### 5.7.3 Ozzie

不同于JobControl在客户端提交任务，Ozzie作为服务器运行。客户端提交工作流到Ozzie服务器。Ozzie中，工作流是一个由动作节点和控制流节点组成的DAG。
动作节点执行任务，比如HDFS文件移动、MapReduce/Hive/Pig 作业、Sqoop导入、运行Shell脚本、Java程序等。控制流节点通过构建条件逻辑或并行执行来管理工作流执行情况。
在工作流完成、进入工作流、退出工作节点时，Ozzie可以向客户端发送HTTP回调。

1. 定义Ozzie工作流

   Ozzie使用XML格式文件定义工作流

   ```xml
   <workflow-app xmlns="uri:oozie:workflow:0.1" name="max-temp-workflow">
       <start to="max-temp-mr"/>
       <action name="max-temp-mr">
           <map-reduce>
               <job-tracker>${resourceManager}</job-tracker>
               <name-node>${nameNode}</name-node>
               <prepare>
                   <delete path="${nameNode}/user/${wf:user()}/output"/>
               </prepare>
               <configuration>
                   <property>
                       <name>mapred.mapper.new-api</name>
                       <value>true</value>
                   </property>
                   <property>
                       <name>mapred.reducer.new-api</name>
                       <value>true</value>
                   </property>
                   <property>
                       <name>mapreduce.job.map.class</name>
                       <value>MaxTemperatureMapper</value>
                   </property>
                   <property>
                       <name>mapreduce.job.combine.class</name>
                       <value>MaxTemperatureReducer</value>
                   </property>
                   <property>
                       <name>mapreduce.job.reduce.class</name>
                       <value>MaxTemperatureReducer</value>
                   </property>
                   <property>
                       <name>mapreduce.job.output.key.class</name>
                       <value>org.apache.hadoop.io.Text</value>
                   </property>
                   <property>
                       <name>mapreduce.job.output.value.class</name>
                       <value>org.apache.hadoop.io.IntWritable</value>
                   </property>
                   <property>
                       <name>mapreduce.input.fileinputformat.inputdir</name>
                       <value>/user/${wf:user()}/input/ncdc/micro</value>
                   </property>
                   <property>
                       <name>mapreduce.output.fileoutputformat.outputdir</name>
                       <value>/user/${wf:user()}/output</value>
                   </property>
               </configuration>
           </map-reduce>
           <ok to="end"/>
           <error to="fail"/>
       </action>
       <kill name="fail">
           <message>MapReduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]
           </message>
       </kill>
       <end name="end"/>
   </workflow-app>
   ```

   上面的示例定义了如下工作流

   ![MapReduce - WorkFlow - Ozzie](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\MapReduce - WorkFlow - Ozzie.png)

   一个start控制节点、一个map-reduce动作节点、一个kill控制节点、一个end控制节点。一个可选prepare元素。另外使用了JSP 表达式（EL）语法。

2. 打包Ozzie工作流应用

   打包目录结构如下

   ```
   max-temp-workflow/
   ├── lib/
   │ └── hadoop-examples.jar
   └── workflow.xml
   ```

   工作流定义文件workflow.xml必须放在顶级目录下，MapReduce作业的JAR文件放在lib目录下。打包完毕后将其拷贝到HDFS上。

   ```shell
   % hadoop fs -put hadoop-examples/target/max-temp-workflow max-temp-workflow
   ```

3. 运行Ozzie作业

   我们使用Ozzie命令行作为与Ozzie服务器通信的客户端程序。执行以下命令。

   ```shell
   // 定义Ozzie服务器的地址
   % export OOZIE_URL="http://localhost:11000/oozie"
   % oozie job -config ch06-mr-dev/src/main/resources/max-temp-workflow.properties -run job: 0000001-140911033236814-oozie-oozi-W
   ```

   `-config`选项定义本地属性文件，包含XML文件属性，以及配置 `oozie.wf.application.path`，也就是Ozzie作业在HDFS的位置。本地属性文件内容如下。

   ```t
   nameNode=hdfs://localhost:8020
   resourceManager=localhost:8032
   oozie.wf.application.path=${nameNode}/user/${user.name}/max-temp-workflow
   ```

   使用`-info [job id]`查看Job信息，`ozzie job`命令可以得到所有作业列表。

   ```shell
   % oozie job -info 0000001-140911033236814-oozie-oozi-W
   ```

   输出RUNNING、KILLED或SUCCECED。也可以通过Ozzie Web 页面查看，地址 http://localhost:11000/oozie。