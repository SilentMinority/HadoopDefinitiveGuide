##### 七、MapReduce类型与格式

### 7.1 MapReduce类型

在MapReduce中Map函数、Reduce函数、Combiner函数、Partition函数遵循如下常规格式。K1/K2/K3、V1/V2/V3可以是相同的类型。

```
map: (K1, V1) → list(K2, V2)
// 对Map输出的每条KV数据进行分区，返回分区索引（Partition Index）
partition: (K2, V2) → integer
// 输出类型即Map输出类型
combiner: (K2, list(V2)) → list(K2, V2)
reduce: (K2, list(V2)) → list(K3, V3)
```

Context类用于输出键值对，使用write()方法。

```java
public void write(KEYOUT key, VALUEOUT value)
 throws IOException, InterruptedException
```

| Property                                 | Job setter method            | Input types | Intermediate types | Output types |
| ---------------------------------------- | ---------------------------- | ----------- | ------------------ | ------------ |
|                                          |                              | K1    V1    | K2    V2           | K3    V3     |
| Properties for configuring types :       |                              |             |                    |              |
| mapreduce.job.inputformat.class          | setInputFormatClass()        | √       √   |                    |              |
| mapreduce.map.output.key.class           | setMapOutputKeyClass()       |             | √                  |              |
| mapreduce.map.output.value.class         | setMapOutputValueClass()     |             | √                  |              |
| mapreduce.job.output.key.class           | setOutputKeyClass()          |             |                    | √            |
| mapreduce.job.output.value.class         | setOutputValueClass()        |             |                    | √            |
| Properties that must be consistent with the types : |                              |             |                    |              |
| mapreduce.job.map.class                  | setMapperClass()             | √     √     | √       √          |              |
| mapreduce.job.combine.class              | setCombinerClass()           |             | √       √          |              |
| mapreduce.job.partitioner.class          | setPartitionerClass()        |             | √       √          |              |
| mapreduce.job.output.key.comparator.class | setSortComparatorClass()     |             | √                  |              |
| mapreduce.job.output.group.comparator.class | setGroupingComparatorClass() |             | √                  |              |
| mapreduce.job.reduce.class               | setReducerClass()            |             | √       √          | √       √    |
| mapreduce.job.outputformat.class         | setOutputFormatClass()       |             |                    | √       √    |

#### 7.1.1 默认MapReduce Job

不指定Mapper和Reducer时，MapReduce会执行默认设置。

```java
public class MinimalMapReduce extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job(getConf());
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MinimalMapReduce(), args);
        System.exit(exitCode);
    }
}
```

等效于下面代码。即下面代码中各项设置就是MapReduce使用的默认设置。

```java
public class MinimalMapReduceWithDefaults extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(Mapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(HashPartitioner.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
        System.exit(exitCode);
    }
}
```

默认设置如下

- TextInputFormat - 默认输入格式
- Mapper - 默认Mapper类，将输入原样写到输出。
- HashPartitioner - 默认Partitioner。它对每条记录的Key计算哈希值，将哈希值转为一个非负数，之后与Integer.MAX_VALUE进行按位与操作，得出的结果和Partition的数量取模，获得Partition Index。实际默认情况下只有一个Reducer，因此也只有一个Partition。
- Reducer - 默认Reducer类。也是将输入原样写到输出。
- TextOutputFormat - 默认输出格式。将Key和Value用制表符分隔，拼成字符串输出。

### 7.2 输入格式

#### 7.2.1 输入分片与记录

分片（Split）和记录（Record）都是逻辑概念，不能直接对等于文件及内容。比如在输入源是数据库时（DBInputFormat），Split对应一个表上的若干行数据，Record对应一行数据。
Java中，Split的代表接口是InputSplit。Split本身不包含数据，其包含一个以字节为单位的长度，和一组存储位置。MapReduce程序根据存储位置分配任务，达到数据本地性。

开发人员不直接处理InputSplit，而是通过InputFormat处理。先看InputFormat接口。

```java
public abstract class InputFormat<K, V> {
    public abstract List<InputSplit> getSplits(JobContext context)
            throws IOException, InterruptedException;

    public abstract RecordReader<K, V>
    createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException;
}
```

Job的Client端程序调用`getSplits()`计算分片，然后将分片发给Application Master，AM通过存储位置规划Task。MapTask通过将Split发给InputFormat的createRecordReader()方法来为Split创建一个RecordReader实例。RecordReader可以认为是Record的迭代器，用来获取键值对。

```java
public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}
```

Context.nextKeyValue()方法调用RecordReader的同名函数，改变其`getCurrentKey()`、`getCurrentValue()`方法指向KV对象的内容。注意，`getCurrentKey()`、`getCurrentValue()`返回的一直都是同一个KV对象，只是其内容在被改变。当读到Split末尾后，`nextKeyValue()`方法返回false，Map Task运行其`cleanup()`方法结束运行。

Mapper的`run()`方法是*public*的，用户可以自由定制。比如MultithreadedMapper，允许配置个数的线程在同一个JVM内来并发运行多个Mapper，对于需要连接外部服务器导致处理单条记录时间过长的Mapper来说，这很有用。

##### 7.2.1.1 FileInputFormat类

FileInputFormat是所有基于文件输入源的InputFormat的基类。实现两个功能：指出文件位置；将文件切分为Split（由子类具体实现）。

![MapReduce - Type & Format - TextInputFormat](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\MapReduce - Type & Format - TextInputFormat.png)

##### 7.2.1.2 FileInputFormat输入路径

```java
public static void addInputPath(Job job, Path path)
public static void addInputPaths(Job job, String commaSeparatedPaths)
public static void setInputPaths(Job job, Path... inputPaths)
public static void setInputPaths(Job job, String commaSeparatedPaths)
```

可以添加多个文件或目录作为Job的输入路径，目录下所有文件（除隐藏文件）都会被认为是输入文件。addInputPath()、addInputPaths()可以多次调用添加多个路径，setInputPaths()调用一次直接设置，并将覆盖之前设置的路径。
注意目录不会被递归处理，即输入路径中的问加减会被当作文件处理，导致报错。
使用setInputPathFilter()方法设置自定义过滤器。默认过滤器只过滤隐藏文件（. 和 _ 开头的文件），自定义过滤器对默认过滤器是继承关系。
也可以通过属性来设置输入路径和过滤器。

| 属性                                       | 类型     | 默认值  | 描述   |
| ---------------------------------------- | ------ | ---- | ---- |
| mapreduce.input.fileinputformat.inputdir | 逗号分隔路径 | 无    | 输入路径 |
| mapreduce.input.pathFilter.class         | Java类  | 无    | 过滤器类 |

##### 7.2.1.3 FileInputFormat输入分片

| 属性                                       | 类型   | 默认值            | 描述                |
| ---------------------------------------- | ---- | -------------- | ----------------- |
| mapreduce.input.fileinputformat.split.minsize | int  | 1              | 分片大小下限，单位字节       |
| mapreduce.input.fileinputformat.split.maxsize | long | Long.MAX_VALUE | 分片大小上限，单位字节       |
| dfs.blocksize                            | long | 128M           | HDFS的Block大小，单位字节 |

分片大小下线一般都是 1字节，某些情况下可能出现小于1 字节。（不写了）
Application可以设置分片大小比HDFS Block更大，这会导致MapTask有更多的非本地Block，增加计算开销。
分片大小计算方法如下。默认配置下，`minimumSize < blockSize < maximumSize`，即Split大小等于Block大小

```java
max(minimumSize, min(maximumSize, blockSize))
```

##### 7.2.1.4 小文件与CombineFileInputFormat

CombineFileInputFormat可以将多个小文件打包进一个Split，但仍应避免大量小文件，这会导致寻址时间大量增加，以及对HDFS造成资源浪费。CombineFileInputFormat也可以将多个大文件合并成一个Split，这样将多个文件交给一个MapTask处理，进而减少Mapper数量、Task启动初始化时间。

##### 7.2.1.5 避免切分

有时希望一个文件被一个MapTask处理，不被切分。有两种方式：一种是增大Split大小到文件大小，不推荐；第二种是自定义实现FileInputFormat类，重写isSplitable()方法，返回false。

```java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
public class NonSplittableTextInputFormat extends TextInputFormat {
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}
```

##### 7.2.1.6 Mapper中的文件信息

在Map函数中获取当前处理Split所属文件的信息。调用Context.getInputSplit()方法获取。输入格式是FileInputFormat,时，返回结果会被强制转为FileSplit。

| FileSplit方法 | 属性                         | 类型            | 说明             |
| ----------- | -------------------------- | ------------- | -------------- |
| getPath()   | mapreduce.map.input.file   | Path / String | 正在处理的文件路径      |
| getStart()  | mapreduce.map.input.start  | long          | Split开始处的字节偏移量 |
| getLength() | mapreduce.map.input.length | long          | Split长度，单位字节   |

##### 7.2.1.7 整个文件作为一条记录处理

使用WholeFileInputFormat类进行处理。Key为null，Value即文件内容。

#### 7.2.2 文本输入

##### 7.2.2.1 TextInputFormat

TextInputFormat是MapReduce的默认输入InputFormat，它的Key是LongWritable类型，是当前记录在整个文件内的字节偏移量（不是行号！），Value就是对应行的文本内容。因为知道每一个Split的大小，所以可以计算获得每行在整个文件内的偏移量。
可能出现某些行数据跨HDFS Block的情况，此时Mapper处理的数据可能出现非本地化的情况，但开销一般比较小。
为防止内存溢出，需要控制行数据的长度。mapreduce.input.linerecordreader.line.maxlength设置每行数据最大允许字节数，超过的数据会被跳过。

##### 7.2.2.2 KeyValueTextInputFormat

使用KeyValueTextInputFormat，文本数据的Key和Value使用制表符分隔会自动识别。

##### 7.2.2.3 NLineInputFormat

mapreduce.input.lineinputformat.linespermap属性设置每个Split读入多少行，默认为 1。某些情况下，比如仿真计算或某些数据库数据处理时，这很有用。

##### 7.2.2.4 关于XML

可以使用7.2.1.7中所述的，将整个文件做一条记录，来处理XML文件。
另外Hadoop提供了StreamXmlRecordReader类，使用 StreamInputFormat并将`stream.recordreader.class`属性设为`org.apache.hadoop.streaming.mapreduce.StreamXmlRecordReader`可以启用。

#### 7.2.3 二进制输入

##### 7.2.3.1 SequenceFileInputFormat

如果要使用Sequence File作为输入数据，可以使用。要保证Mapper的泛型声明和输入数据的KV类型保持一致。

##### 7.2.3.2 SequenceFileAsTextInputFormat

将输入数据的键值转为Text类型，通过调用其toString()方法完成。

##### 7.2.3.3 SequenceFileAsBinaryInputFormat

将输入数据的键值封装为BytesWritable对象，交与程序处理。

#### 7.2.4 多个输入

当输入文件有种格式、类型，甚至处理方式都不同时，可以使用MultipleInputs。

```java
MultipleInputs.addInputPath(job, ncdcInputPath, TextInputFormat.class, MaxTemperatureMapper.class);
MultipleInputs.addInputPath(job, metOfficeInputPath, TextInputFormat.class, MetOfficeMaxTemperatureMapper.class);
```

为不同的输入路径设置不同的InputFormat、Mapper。或者都是用同相同的Mapper。

```java
public static void addInputPath(Job job, Path path, Class<? extends InputFormat> inputFormatClass)
```

#### 7.2.5 数据库输入（输出）

DBInputFormat可以从数据库读取数据，但可能读取过频导致DB出现问题，不建议使用。对应输出是DBOutputFormat。
可以使用Sqoop从关系型数据库迁移数据到HDFS。
TableInputFormat、TableOutputFormat用来操作HBase的数据。

--------

### 7.3 输出格式

每种InputFormat都有对应的OutputFormat。下图是OutputFormat层次结构。

![MapReduce - Type & Format - OutputFormat](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\MapReduce - Type & Format - OutputFormat.png)

#### 7.3.1 文本输出

默认使用TextOutputFormat，它调用记录键值的`toString()`方法，将每行记录写成文本。KV默认使用制表符分隔，可以通过`mapreduce.output.textoutputformat.separator`属性修改。
可以使用NullOutputFormat省略Key或者Value的输出，也可以两者都省略。

#### 7.3.2 二进制输出

##### SequenceFileOutputFormat

将输出写为Sequence File，如果MapReduce输出要作为后续MR输入，此格式很好，格式紧凑、易于压缩。

##### SequenceFileAsBinaryOutputFormat

以键值对形式写为Sequence File。

##### MapFileOutputFormat

将MapFile作为输出。需要确保Reducer输出的Key已经排序。

#### 7.3.3 多个输出

Reducer的输出放在输出目录下，默认命名规则 part-r-00000, partr-00001等等。每个Reducer输出一个文件。有时需要一个Reducer输出多个文件，可以使用MultipleOutputs类来做到。

如下面的代码，在Reduce函数中没有使用Context实例来写输出文件，而是使用MultipleOutputs的实例来进行输出。MultipleOutputs.write()方法可以指定输出文件名字、使用“/”来制定输出文件子路径。

```java
public class PartitionByStationUsingMultipleOutputs extends Configured
        implements Tool {

    static class StationMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            parser.parse(value);
            context.write(new Text(parser.getStationId()), value);
        }
    }

    static class MultipleOutputsReducer
            extends Reducer<Text, Text, NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                multipleOutputs.write(NullWritable.get(), value, key.toString());
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        job.setMapperClass(StationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(MultipleOutputsReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(),
                args);
        System.exit(exitCode);
    }
}
```

最终输出文件命名格式如下：

```
output/010010-99999-r-00027
output/010050-99999-r-00013
output/010100-99999-r-00015
output/010280-99999-r-00014
output/010550-99999-r-00000
output/010980-99999-r-00011
output/011060-99999-r-00025
output/012030-99999-r-00029
output/012350-99999-r-00018
output/012620-99999-r-00004
```

或者像下面这样指定路径：

```java
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            parser.parse(value);
            String basePath = String.format("%s/%s/part",
                    parser.getStationId(), parser.getYear());
            multipleOutputs.write(NullWritable.get(), value, basePath);
        }
    }
```

#### 7.3.4 延迟输出

FileOutputFormat的子类在输出为空时，也会创建输出文件。 LazyOutputFormat指定只有在出现第一条输出记录时才创建输出文件，使用JobConf.setOutputFormatClass()方法指定。

#### 7.3.5 数据库输出

参见7.2.5的HBase输出。