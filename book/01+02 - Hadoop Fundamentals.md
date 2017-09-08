## 一、关于Hadoop

### 一些Hadoop历史的关键词

Doug Cutting，Hadoop创始人
Hadoop名字来源，Doug女儿的大象毛绒玩具
Nutch，Hadoop最初是Doug在雅虎时做的Nutch系统一部分，2006年移出单做
GFS，Hadoop灵感来源于GFS

### Hadoop一部分生态

- Common - 一系列组件和接口，用于分布式文件系统和通用 I/O
- Avro - 一个序列化系统
- MapReduce - 分布式计算模型和执行环境
- HDFS - Hadoop Distribute File System
- Pig - 数据流语言和运行环境
- Hive - 分布式的、列式存储的数据仓库
- HBase - 分布式的、列式存储的数据库
- Zookeeper - 分布式的、高可用的协调服务
- Oozie - 用于运行和调度Hadoop作业的服务

## 二、关于MapReduce

### 2.3 JavaMapReduce

#### 示例代码

##### Map函数

```java
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        ... ...
        ... ...
                
        context.write(new Text(year), new IntWritable(airTemperature));
        
    }
}
```

##### Reduce函数

```java
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MaxTemperatureReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        ... ...
        ... ...
        
        context.write(key, new IntWritable(maxValue));
    }
}
```

##### 运行函数

```java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class MaxTemperature {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max temperature");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

#### 代码解析

##### Map函数

这个Mapper类继承`org.apache.hadoop.mapreduce.Mapper`类，是一个泛型类。四个形参分别指定 Map函数的输入键、输入值、输出键、输出值的类型。
Hadoop提供了一套用于**网络传输**的基础类型，不直接使用Java的内嵌类型。这些类型都在`org.apache.hadoop.io`包中。
在这里的代码中，输入键表示行数据相对于文件开始的偏移量。

Map函数提供Context进行结果输出。

##### Reduce函数

Reduce函数同样有四个形参指定输入键、输入值、输出键、输出值的类型。

##### 运行函数

- Job对象制定整个作业的运行规范
- 在Hadoop集群上运行作业时，需要把代码打包成JAR。可以不必设置JAR文件的名称，通过 Job的`setJarByClass()`方法传递一个类，Hadoop会根据这个类查找包含它的JAR文件。
- `FileInputFormat.addInputPath()` 静态方法指定作业输入数据的位置。输入数据可以是一个文件或一个目录。也可以多次调用，指定多个输入目录/文件。
- `FileOutputFormat.setOutputPath()`  静态方法指定作业的输出路径。输出路径只能有一个，并且在作业运行前不应该存在，否则会报错。
- 通过Job对象的`setMapperClass()`和`setReducerClass()` 方法指定Map函数和Reduce函数的实现类
- 通过Job对象的`setOutputKeyClass()`和`setOutputValueClass()`方法指定作业输出的键/值类型。
  这两个方法会同时设置Map和Reduce函数的输出类型。如果M/R两者不同，使用`setMapOutputKeyClass()`和`setMapOutputValueClass()`函数设置Map函数的输出类型。
- 输入类型使用`InputFormat`控制，这里没有设置，使用默认的`TextInputFormat`。
- Job的`waitForCompletion()`方法会提交作业并等待执行结束。它的boolean参数控制是否输出作业详细进度到控制台。返回结果表示执行是否成功。

#### 运行命令

```shell
export HADOOP_CLASSPATH=hadoop-examples.jar
hadoop MaxTemperature input/ncdc/sample.txt output
```

第一行设置Hadoop的Classpath，指定应用程序的类路径。第二行执行作业，由Hadoop脚本执行相关操作。

### 2.4 横向扩展

#### 2.4.1 数据流

Hadoop Map Reduce中存在两类节点，一个JobTracker和若干个TaskTracker。JobTracker负责在所有TaskTracker上进行任务调度，TaskTracker负责具体的Task执行，以及将执行结果报告JobTracker。JobTracker会对失败的Task重新分配TaskTracker执行。

Hadoop会将MapReduce的输入数据还分为等长的小数据块（Split），并为每个Split创建一个MapTask。一般来说，更小的分片大小会带来更好的负载均衡。但过小的Split，会导致分片管理和创建MapTask的时间增加。因此Split大小一般趋向于HDFS中Block的大小，默认64M，不过可以修改集群默认值或针对新建文件具体指定。

MapTask具有数据本地性，MapTask会优先被分配到与其要计算的数据最近的位置。同一节点 > 同一机架 > 不同机架。数据本地性减少数据的网络传输带来的资源消耗。而ReduceTask由于要从多个MapTask接收数据，所以不具备数据本地性。需要注意，MapTask的输出结果会写到其所在节点的本地文件系统而不是HDFS中。

MapTask的数量与输入文件的Split相同，而ReduceTask的数量是单独指定的（7.1.1）。
MapTask会为每个ReduceTask创建一个Partition，每个Partition中有多个Key/Val形式数据，相同Key的数据会在同一个Partition。分区由用户定义的partition方法（`Partitioner`类）指定，默认使用哈希进行分区。
一般情况下，ReduceTask需要接收多个MapTask的数据并将他们结合到一起，这就是Shuffle。

![DataFlow - MultiReduce](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\DataFlow - MultiReduce.png)

#### 2.4.2 Combiner函数

Combiner，实际上是在MapTask所在的节点进行“半”Reduce，即在MapTask结束后，数据传输到ReduceTask节点之前，对相同Key的数据进行计算。
Combiner存在的意义就是减少MapTask到ReduceTask需要传输的数据量。Combiner没有默认的实现，需要用户自行实现，使用`job.setCombinerClass()`方法指定。Combiner与Reducer需要实现相同的父类。

### 2.5 Hadoop Streaming

Hadoop streaming是Hadoop的一个工具， 它帮助用户创建和运行一类特殊的map/reduce作业， 这些特殊的map/reduce作业是由一些可执行文件或脚本文件充当mapper或者reducer。主要用来使用非Java语言编写MapReduce程序，略过不谈。

### 2.6 Hadoop Pipes

Hadoop的C++接口，略过。