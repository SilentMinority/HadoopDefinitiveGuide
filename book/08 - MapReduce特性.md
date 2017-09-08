## 八、MapReduce特性

### 8.1 Counter计数器

#### 8.1.1 内置计数器

Hadoop有许多内置计数器，用以统计Job、Task（Map/Reduce）等等的状态、进度等等信息。
这些计数器被分为几个组。

| Group                      | Name/Enum                                |
| -------------------------- | ---------------------------------------- |
| MapReduce Task Counter     | org.apache.hadoop.mapreduce.TaskCounter  |
| File System Counter        | org.apache.hadoop.mapreduce.FileSystemCounter |
| File Input Format Counter  | org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter |
| File Output Format Counter | org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter |
| Job Counter                | org.apache.hadoop.mapreduce.JobCounter   |

##### TaskCounter

TaskCounter记录Task执行期间的信息，由TaskAttempt维护，定期发送给ApplicationMaster，由AM做累加。TaskAttempt都会发送Counter的完整值，而不是距上次发送的新增值，以防发送失败导致丢失。TaskAttempt失败可能导致Counter数值减少。
挑一些典型的，描述简化，具体参考原书。

| Counter                | 说明                                       |
| ---------------------- | ---------------------------------------- |
| MAP_INPUT_RECORDS      | Job种所有Mapper已消费的记录数，RecordReader每读取一条即增加 |
| SPLIT_RAW_BYTES        | 所有Mapper读取的输入Split**对象**的字节数             |
| MAP_OUTPUT_RECORDS     | Mapper输出Record数，OutputCollector.collect()被调用即增加 |
| COMBINE_INPUT_RECORDS  | Combiner处理的Record数                       |
| COMBINE_OUTPUT_RECORDS | Combiner输出的Record数，Combiner的OutputCollector的collect()方法调用即增加 |
| REDUCE_INPUT_GROUPS    | Reducer输入Key的数量，reduce()方法被调用即增加         |
| REDUCE_INPUT_RECORDS   | Reducer处理的Record数                        |
| REDUCE_OUTPUT_RECORDS  | Reducer输出的Record数                        |
| REDUCE_SHUFFLE_BYTES   | Reducer从Mapper复制数据的字节数                   |
| SPILLED_RECORDS        | 溢写到磁盘的Record数，包括Map和Reduce               |
| CPU_MILLISECONDS       | 总的CPU时间，毫秒，/proc/cpuinfo获取               |
| PHYSICAL_MEMORY_BYTES  | 一个Task用的物理内存字节数，/proc/meminfo获取          |
| VIRTUAL_MEMORY_BYTES   | 一个Task用的物理虚拟字节数，/proc/meminfo获取          |
| SHUFFLED_MAPS          | Mapper传输到Reducer的Map输出文件数                |
| FAILED_SHUFFLE         | 传输失败的Shuffle文件数                          |
| MERGED_MAP_OUTPUTS     | 在Reduce端被合并的Map文件数                       |

##### JobCounter

JobCounter由ApplicationMaster维护，不需要网络传输。

| Counter                                  | 说明                                       |
| ---------------------------------------- | ---------------------------------------- |
| TOTAL_LAUNCHED_MAPS/REDUCES/UBERTASKS    | 已加载Map/Reduce/UberTask数量，包括推测执行          |
| NUM_UBER_SUBMAPS/SUBREDUCES              | UserTask种MapTask/ReduceTask的数量           |
| NUM_FAILED_MAPS/REDUCES/UBERTASKS        | 失败任务数                                    |
| NUM_KILLED_MAPS/REDUCES/UBERTASKS        | 被终止任务数                                   |
| DATA_LOCAL_MAPS/RACK_LOCAL_MAPS/OTHER_LOCAL_MAPS | 与输入数据同节点/同机架/不同机架MapTask数                |
| MILLIS_MAPS/REDUCES                      | MapTask/ReduceTask总耗时，毫秒，包括推测执行。可以参考VCORES_MILLIS_MAPS/REDUCES 和MB_MILLIS_MAPS/REDUCES |

#### 8.1.2 用户定义Java计数器

参考下面代码，一般使用枚举类型来定义计数器。在Map函数、Reduce函数中使用，在Job结束后产生最终结果。

```java
public class MaxTemperatureWithCounters extends Configured implements Tool {

    enum Temperature {
        MISSING,
        MALFORMED
    }

    static class MaxTemperatureMapperWithCounters
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            parser.parse(value);
            if (parser.isValidTemperature()) {
                int airTemperature = parser.getAirTemperature();
                context.write(new Text(parser.getYear()),
                        new IntWritable(airTemperature));
            } else if (parser.isMalformedTemperature()) {
                System.err.println("Ignoring possibly corrupt input: " + value);
                context.getCounter(Temperature.MALFORMED).increment(1);
            } else if (parser.isMissingTemperature()) {
                context.getCounter(Temperature.MISSING).increment(1);
            }

            // dynamic counter
            context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MaxTemperatureMapperWithCounters.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
        System.exit(exitCode);
    }
}
```

执行与输出：

```shell
% hadoop jar hadoop-examples.jar MaxTemperatureWithCounters input/ncdc/all output-counters
```

```
Air Temperature Records
 Malformed=3
 Missing=66136856
TemperatureQuality
 0=1
 1=973422173
 2=1246032
 4=10764500
 5=158291879
 6=40066
 9=66136858
```

##### 动态计数器

可以在代码中动态定义Counter，调用Context对象的getCounter()方法实现。

```java
public Counter getCounter(String groupName, String counterName)
```

##### 获取计数器值

Web UI界面查看、使用  `mapred job -counter` 命令，也可以使用Java API获取。

```java
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MissingTemperatureFields extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            JobBuilder.printUsage(this, "<job ID>");
            return -1;
        }
        String jobID = args[0];
        Cluster cluster = new Cluster(getConf());
        Job job = cluster.getJob(JobID.forName(jobID));
        if (job == null) {
            System.err.printf("No job with ID %s found.\n", jobID);
            return -1;
        }
        if (!job.isComplete()) {
            System.err.printf("Job %s is not complete.\n", jobID);
            return -1;
        }
        Counters counters = job.getCounters();
        long missing = counters.findCounter(
                MaxTemperatureWithCounters.Temperature.MISSING).getValue();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        System.out.printf("Records with missing temperature fields: %.2f%%\n",
                100.0 * missing / total);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MissingTemperatureFields(), args);
        System.exit(exitCode);
    }
}
```

首先使用Job ID调用Cluster.getJob()方法获取Job对象，在确认Job已完成后，调用getCounters()方法获取Counters对象，之后使用findCounter()方法通过Counter名称来获取Counter。

#### 8.1.3 用户定义StreamingCounter

使用Streaming的MapReduce可以通过向错误流发送如下格式信息增加计数器值。

```
reporter:counter:group,counter,amount
```

Python示例代码：

```python
sys.stderr.write("reporter:counter:Temperature,Missing,1\n")
```

状态信息也可以用类似方式发出

```
reporter:status:message
```

### 8.2 排序

#### 8.2.1 准备

本节使用气温监测点的历史气温数据作为示例。先对数据进行过滤，移除无效数据。以温度为Key进行输出。

```shell
% hadoop jar hadoop-examples.jar SortDataPreprocessor input/ncdc/all input/ncdc/all-seq
```

```java
// cc SortDataPreprocessor A MapReduce program for transforming the weather data into SequenceFile format
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv SortDataPreprocessor
public class SortDataPreprocessor extends Configured implements Tool {
  
  static class CleanerMapper
    extends Mapper<LongWritable, Text, IntWritable, Text> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        context.write(new IntWritable(parser.getAirTemperature()), value);
      }
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }

    job.setMapperClass(CleanerMapper.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job,
        CompressionType.BLOCK);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
    System.exit(exitCode);
  }
}
// ^^ SortDataPreprocessor
```

#### 8.2.2 局部排序

Hadoop默认使用Key进行排序。下面的代码就使用IntWritable的Key进行排序。并且产生与Reducer个数相同的排序好的文件。

```java
// cc SortByTemperatureUsingHashPartitioner A MapReduce program for sorting a SequenceFile with IntWritable keys using the default HashPartitioner
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv SortByTemperatureUsingHashPartitioner
public class SortByTemperatureUsingHashPartitioner extends Configured
  implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job,
        CompressionType.BLOCK);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(),
        args);
    System.exit(exitCode);
  }
}
// ^^ SortByTemperatureUsingHashPartitioner
```

```java
% hadoop jar hadoop-examples.jar SortByTemperatureUsingHashPartitioner -D mapreduce.job.reduces=30 input/ncdc/all-seq output-hashsort
```

##### 控制排序顺序

Key排序的顺序由RawComparator按照如下规则控制：

- 如果`mapreduce.job.output.key.comparator.class`属性进行了设置，或者使用Job.setSortComparatorClass()方法进行了设置，则使用设置类的实例
- 否则，Key必须是WritableComparable的子类，并且使用其注册的comparator进行比较
- 如果没有注册的comparator，就使用RawComparator将字节流反序列化为对象，在调用WritableComparable.compareTo()方法比较

#### 8.3.3 全局排序

要将所有数据排序，最简单粗暴的方法是只设置一个Reducer。当然不可行。

代替方案是创建一系列排序好的文件，再进行串联。主要思路是使用Partitioner，将数据进行分组排序。比如 0 - 10 度数据分到一个Reducer，11 - 20分到一个Reducer，以此类推。
这样做的难点在于如何进行分区，保证各个区间的数据量大致相同，避免数据倾斜。这里引入采样器Sampler。

```java
public interface Sampler<K, V> {
    K[] getSample(InputFormat<K, V> inf, Job job)
            throws IOException, InterruptedException;
}
```

该接口通常不由客户端调用，而是由InputSampler的writePartitionFile()静态方法调用。创建一个SequenceFile来存储定义分区的Key。

```java
public static <K, V> void writePartitionFile(Job job, Sampler<K, V> sampler)
   throws IOException, ClassNotFoundException, InterruptedException
```

对示例进行全局排序。
使用RandomSampler进行采样，采样率 0.1。最大样本数 1000，最大分区 10，任一条件满足时即停止采样。为了和集群上的其他Task共享分区文件，InputSampler需要将其所写的分区文件加到分布式缓存中。

```java
// cc SortByTemperatureUsingTotalOrderPartitioner A MapReduce program for sorting a SequenceFile with IntWritable keys using the TotalOrderPartitioner to globally sort the data
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.*;

// vv SortByTemperatureUsingTotalOrderPartitioner
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured
  implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job,
        CompressionType.BLOCK);

    job.setPartitionerClass(TotalOrderPartitioner.class);

    InputSampler.Sampler<IntWritable, Text> sampler =
      new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
    
    InputSampler.writePartitionFile(job, sampler);

    // Add to DistributedCache
    Configuration conf = job.getConfiguration();
    String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
    URI partitionUri = new URI(partitionFile);
    job.addCacheFile(partitionUri);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(
        new SortByTemperatureUsingTotalOrderPartitioner(), args);
    System.exit(exitCode);
  }
}
// ^^ SortByTemperatureUsingTotalOrderPartitioner
```

运行

```shell
% hadoop jar hadoop-examples.jar SortByTemperatureUsingTotalOrderPartitioner -D mapreduce.job.reduces=30 input/ncdc/all-seq output-totalsort
```

#### 8.3.4 二次排序

Mapper只会对数据按Key进行排序，而不会按Value进行排序。多次运行得到的结果的顺序可能是不同的。
假设我们要取得各年份的最高温度记录，先将年份和温度组合成Key。这些Key进行倒序排序。

```
1900 35°C
1900 34°C
1900 34°C
...
1901 36°C
1901 35°C
```

然后自定义Partitioner，将相同年份的数据分组到相同的Reducer。
然而Reducer会将相同年份、气温不同的多条记录全部输出，我们没各年份只需要最高温度的那条就够了。这时就需要再次进行分组。
示例代码。

```java
// cc MaxTemperatureUsingSecondarySort Application to find the maximum temperature by sorting temperatures in the key
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MaxTemperatureUsingSecondarySort
public class MaxTemperatureUsingSecondarySort
  extends Configured implements Tool {
  
  static class MaxTemperatureMapper
    extends Mapper<LongWritable, Text, IntPair, NullWritable> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value,
        Context context) throws IOException, InterruptedException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        /*[*/context.write(new IntPair(parser.getYearInt(),
            parser.getAirTemperature()), NullWritable.get());/*]*/
      }
    }
  }
  
  static class MaxTemperatureReducer
    extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {
  
    @Override
    protected void reduce(IntPair key, Iterable<NullWritable> values,
        Context context) throws IOException, InterruptedException {
      
      /*[*/context.write(key, NullWritable.get());/*]*/
    }
  }
  
  public static class FirstPartitioner
    extends Partitioner<IntPair, NullWritable> {

    @Override
    public int getPartition(IntPair key, NullWritable value, int numPartitions) {
      // multiply by 127 to perform some mixing
      return Math.abs(key.getFirst() * 127) % numPartitions;
    }
  }
  
  public static class KeyComparator extends WritableComparator {
    protected KeyComparator() {
      super(IntPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      IntPair ip1 = (IntPair) w1;
      IntPair ip2 = (IntPair) w2;
      int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
      if (cmp != 0) {
        return cmp;
      }
      return -IntPair.compare(ip1.getSecond(), ip2.getSecond()); //reverse
    }
  }
  
  public static class GroupComparator extends WritableComparator {
    protected GroupComparator() {
      super(IntPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      IntPair ip1 = (IntPair) w1;
      IntPair ip2 = (IntPair) w2;
      return IntPair.compare(ip1.getFirst(), ip2.getFirst());
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setMapperClass(MaxTemperatureMapper.class);
    /*[*/job.setPartitionerClass(FirstPartitioner.class);/*]*/
    /*[*/job.setSortComparatorClass(KeyComparator.class);/*]*/
    /*[*/job.setGroupingComparatorClass(GroupComparator.class);/*]*/
    job.setReducerClass(MaxTemperatureReducer.class);
    job.setOutputKeyClass(IntPair.class);
    job.setOutputValueClass(NullWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
    System.exit(exitCode);
  }
}
// ^^ MaxTemperatureUsingSecondarySort
```

##### 8.3.4.2 Streaming

略

### 8.3 连接（Join）

Join，即将来自不同数据源的数据合并到一起输出。比如用户信息和访问记录、气象站信息和气温记录等等。可以考虑使用Pig、Hive、Cascading、Spark等等。
也可以编写MapReduce程序进行Join操作。当一个数据源的数据量较少，另一个数据量较大时，可以考虑将将较少数据量的数据分发到所有节点上。当两个数据源都有大量数据时，就需要编写MapReduce程序了。
分为两种情况，在Map端Join和在Reduce端Join。

#### 8.3.1 Map端Join

Map端Join对输入数据有一些严格要求。

- 必须预先分区（Partition），并且以相同Partitioner进行分区。
- 以相同方式排序。
- 两个输入数据集被分为相同数量的分区，一一对应。

可以使用 `org.apache.hadoop.mapreduce.join` 包中的 CompositeInputFormat 类进行Map端Join。输入源和连接类型使用连接表达式进行配置，参考其说明文档。 可以参考`org.apache.hadoop.examples.Join` ，这是一个Map端Join的通用样例。

#### 8.3.2 Reduce端Join

Reduce端Join对数据格式及是否排序没有要求，但要经过Shuffle，效率要低一些。基本原理是通过Map阶段，标记各个数据源的数据，并将需要连接到一起的数据发送到同一个Reducer进行处理。

##### 多输入

可以使用`MultipleInputs`类，参考 7.2.4 节。

##### 二次排序

Reduce端Join的实现有关键的一点，两个数据源的数据应该预先排序，保证一个数据源的数据一定在另一个之前。类似这样，UserA的用户信息UserInfo数据必须比他的操作记录Record数据先到达Reducer。这样Record数据到达Reducer时，马上就可以处理输出。否则需要先将Record数据缓存到内存中，对于大规模数据量来说这是不可接受的。

参考范例

```java
// 气象站信息Mapper
public class JoinStationMapper
        extends Mapper<LongWritable, Text, TextPair, Text> {
    private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (parser.parse(value)) {
            context.write(new TextPair(parser.getStationId(), "0"),
                    new Text(parser.getStationName()));
        }
    }
}
// 气象数据Mapper
public class JoinRecordMapper
        extends Mapper<LongWritable, Text, TextPair, Text> {
    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        parser.parse(value);
        context.write(new TextPair(parser.getStationId(), "1"), value);
    }
}
// JoinReducer
public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text stationName = new Text(iter.next());
        while (iter.hasNext()) {
            Text record = iter.next();
            Text outValue = new Text(stationName.toString() + "\t" + record.toString());
            context.write(key.getFirst(), outValue);
        }
    }
}
```

Job程序

```java
public class JoinRecordWithStationName extends Configured implements Tool {

    public static class KeyPartitioner extends Partitioner<TextPair, Text> {
        @Override
        public int getPartition(/*[*/TextPair key/*]*/, Text value, int numPartitions) {
            return (/*[*/key.getFirst().hashCode()/*]*/ & Integer.MAX_VALUE) % numPartitions;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            JobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
            return -1;
        }

        Job job = new Job(getConf(), "Join weather records with station names");
        job.setJarByClass(getClass());

        Path ncdcInputPath = new Path(args[0]);
        Path stationInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        MultipleInputs.addInputPath(job, ncdcInputPath,
                TextInputFormat.class, JoinRecordMapper.class);
        MultipleInputs.addInputPath(job, stationInputPath,
                TextInputFormat.class, JoinStationMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);
    
    /*[*/
        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);/*]*/

        job.setMapOutputKeyClass(TextPair.class);

        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new JoinRecordWithStationName(), args);
        System.exit(exitCode);
    }
}
```

### 8.4 附加数据分布

附加数据指Job所需的额外的只读数据，用以辅助处理主数据集。

#### 8.4.1 使用Configuration接口

Configuration的各种setter方法可以向Job中，以配置数据的方式添加各种所需的数据。如果附加数据量比较小，适合使用这种方式。
用户可以使用Context.getCofiguration()方法获得设置的配置信息。
一般情况下，基本数据类型可以满足使用。必要时用户需要自己实现附加数据的序列化/反序列化。或者使用Hadoop提供的Stringifier类。DefaultStringifier使用Hadoop的序列化框架。
**注意**，这种方式会带来较大的内存压力。因为每次Client、Application Master 或者 Task JVM读取配置信息时，总是会将所有的配置项全部都去到内存中，不论是否使用。

#### 8.4.2 分布式缓存

##### 用法

可以参考 5.2.2 节，GenericOptionsParser 用法。即使用 -files、-archives、-libjars向集群中添加文件、归档，或向Mapper/Reducer的 Classpath添加JAR。源文件可以存放在本地、HDFS或其它Hadoop支持的FS中。如果没有指定文件系统，默认使用本地文件系统。即时指定了默认文件系统，也会使用本地文件系统。

示例命令及代码

```java
% hadoop jar hadoop-examples.jar MaxTemperatureByStationNameUsingDistributedCacheFile -files input/ncdc/metadata/stations-fixed-width.txt input/ncdc/all output
```

```java
// cc MaxTemperatureByStationNameUsingDistributedCacheFile Application to find the maximum temperature by station, showing station names from a lookup table passed as a distributed cache file

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

// vv MaxTemperatureByStationNameUsingDistributedCacheFile
public class MaxTemperatureByStationNameUsingDistributedCacheFile
        extends Configured implements Tool {

    static class StationTemperatureMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new Text(parser.getStationId()),
                        new IntWritable(parser.getAirTemperature()));
            }
        }
    }

    static class MaxTemperatureReducerWithStationLookup
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /*[*/private NcdcStationMetadata metadata;/*]*/

        /*[*/
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            metadata = new NcdcStationMetadata();
            // 从缓存文件气象站信息
            metadata.initialize(new File("stations-fixed-width.txt"));
        }/*]*/

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {
      
      /*[*/
            String stationName = metadata.getStationName(key.toString());/*]*/

            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(new Text(/*[*/stationName/*]*/), new IntWritable(maxValue));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(StationTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
                new MaxTemperatureByStationNameUsingDistributedCacheFile(), args);
        System.exit(exitCode);
    }
}
// ^^ MaxTemperatureByStationNameUsingDistributedCacheFile
```

##### 工作机制

Job开始运行前，Hadoop会将-files/-archives/-libjars指定的文件复制到HDFS上。接着，在Task开始运行前，NodeManager会将文件复制到其本地磁盘——即分布式缓存。对于Task来说，文件已经在本地了。
NodeManager会为每个文件维护一个引用计数器，记录引用该文件的Task数量。当计数器值为0时，表示无Task引用，NM会将文件移除，节省空间。缓存空间的大小可以通过 `yarn.nodemanager.localizer.cache.target-size-mb`属性设置，默认10G。
计数器机制并不保证同一Job的后续Task一定能找到缓存文件，但成功几率还是可以很大的。因为Task几乎都是同时开始运行的。

##### API

如果没有使用GenericOptionsParser，可以使用Java API方式添加附加文件。API在Job类中。

```java
public void addCacheFile(URI uri)
public void addCacheArchive(URI uri)
public void setCacheFiles(URI[] files)
public void setCacheArchives(URI[] archives)
public void addFileToClassPath(Path file)
public void addArchiveToClassPath(Path archive)
```

addXXX()方法可以反复调用，结果是累加的。setXXX()方法会覆盖之前addXXX()和setXXX()设置的内容。
GenericOptionsParser添加的文件可以是本地文件，本地文件会先被复制到HDFS中，再由NodeManager进行复制。而使用API添加的必须是在HDFS中的文件。

### 8.5 MapReduce类库

Hadoop为Mapper和Reducer提供了一个常用函数库。下面是一些简要描述。

| 类                                        | 描述                                       |
| ---------------------------------------- | ---------------------------------------- |
| ChainMapper, ChainReducer                | 描述不清，须自查                                 |
| FieldSelectionMapReduce (old API):FieldSelectionMapper 和FieldSelectionReducer (New API) | 描述不清，须自查                                 |
| IntSumReducer, LongSumReducer            | 求和Reducer                                |
| InverseMapperj                           | 键值交换Mapper                               |
| MultithreadedMapRunner (old API), MultithreadedMapper (new API) | 可以在一个JVM的多个线程中并发运行多个MapTask的Mapper       |
| TokenCounterMapper                       | 使用Java的StringTokenizer将输入分解成单词，并输出每个单词和计数值 1 的Mapper |
| RegexMapper                              | 检查输入是否匹配正则，输出匹配字符串和计数 1 的Mapper          |