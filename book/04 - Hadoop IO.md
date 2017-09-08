## 四、Hadoop IO

### 4.1 数据完整性

Hadoop使用CRC-32C来进行数据完整性验证。在数据第一次进入集群时计算校验和（checksum），在数据通过不可靠通道传输时使用校验和校验完整性。

#### 4.1.1 HDFS数据完整性

HDFS为每`dfs.bytes-perchecksum`指定大小的数据块计算校验和，默认512字节。CRC-32C校验和大小是4字节，因此存储校验和额外开销小于 1%。

DataNode负责存储数据及其校验和。用户在写入数据时，会将校验和一同发送给DataNode，管线中的最后一个DataNode负责检查校验和。如果校验失败，返回Client端一个IOException的子类异常。
客户端从DataNode读取数据时，也会检查校验和。如果校验通过会告知DataNode，DataNode保存一份验证校验和的日志，其中有每个数据块最后验证成功的时间，用于损坏检测参考。
每个DataNode都在后台运行一个 DataBlockScanner线程，周期性检查所有存储数据块的校验和。
Client端在检查到损坏数据时，会先向NameNode报告数据块和DataNode信息，再抛出异常。NameNode会将异常Block标为无效，安排重新复制，之后删除损坏Block。

有时需要查看损坏的Block内容以决定如何处理，此时需要禁用校验和读取数据。可以在调用FileSystem的open()方法前，调用 setVerifyChecksum(false) ，可以禁用校验和。Shell命令使用 `-get -ignoreCrc` 或  `-copyToLocal`。

`hadoop fs -checksum` 命令可以查看文件校验和。

### 4.2 压缩

Hadoop常用的常见压缩算法。
| 压缩格式    | 压缩工具  | 算法      | 文件扩展名    | 是否可切割 |
| ------- | ----- | ------- | -------- | ----- |
| DEFLATE | N/A   | DEFLATE | .deflate | No    |
| gzip    | gzip  | DEFLATE | .gz      | No    |
| bzip2   | bzip2 | bzip2   | .bz2     | Yes   |
| LZO     | lzop  | LZO     | .lzo     | No    |
| LZ4     | N/A   | LZ4     | .lz4     | No    |
| Snappy  | N/A   | Snappy  | .snappy  | No    |

#### 4.2.1 Codec

在Hadoop中，一个对CompressionCodec接口的实现就是一个Codec。每种Codec实现了一种研所-解压算法。

Hadoop的Codec。

| Compression | format Hadoop CompressionCodec           |
| ----------- | ---------------------------------------- |
| DEFLATE     | org.apache.hadoop.io.compress.DefaultCodec |
| gzip        | org.apache.hadoop.io.compress.GzipCodec  |
| bzip2       | org.apache.hadoop.io.compress.BZip2Codec |
| LZO         | com.hadoop.compression.lzo.LzopCodec     |
| LZ4         | org.apache.hadoop.io.compress.Lz4Codec   |
| Snappy      | org.apache.hadoop.io.compress.SnappyCodec |

##### 4.2.1.1 使用Codec压缩解压数据

调用`createOutputStream(InputStream)`创建输入流，`createOutputStream(OutputStream)`创建输出流。

```java
public class StreamCompressor {
    public static void main(String[] args) throws Exception {
        String codecClassname = args[0];
        Class<?> codecClass = Class.forName(codecClassname);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec)
                ReflectionUtils.newInstance(codecClass, conf);

        CompressionOutputStream out = codec.createOutputStream(System.out);
        IOUtils.copyBytes(System.in, out, 4096, false);
        out.finish();
    }
}
```

也可以使用命令行测试。

```shell
% echo "Text" | hadoop StreamCompressor org.apache.hadoop.io.compress.GzipCodec | gunzip -
Text
```

##### 4.2.1.2 使用CompressionCodecFactory推断CompressionCodec

CompressionCodecFactory的getCodec()方法可以根据文件后缀名推断Codec。

```java
public class FileDecompressor {
    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);
        if (codec == null) {
            System.err.println("No codec found for " + uri);
            System.exit(1);
        }
        String outputUri =
                CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        InputStream in = null;
        OutputStream out = null;
        try {
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, conf);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
```

4.2.1.3 原生类库
| Compression format | Java implementation? | Native implementation? |
| ------------------ | -------------------- | ---------------------- |
| DEFLATE            | Yes                  | Yes                    |
| gzip               | Yes                  | Yes                    |
| bzip2              | Yes                  | Yes                    |
| LZO                | No                   | Yes                    |
| LZ4                | No                   | Yes                    |
| Snappy             | No                   | Yes                    |

可以使用`etc/hadoop`下的`hadoop`脚本设置Java系统属性`java.library.path`，来指定原生类库位置。或者在Application中手动指定。
Hadoop默认会调用原生类库，无需修改任何配置。也可以将 `io.native.lib.available`属性设为false，禁用原生类库，使用Java内置类库。

4.2.1.3 Codec Pool

Codec资源池。

```java
public class PooledStreamCompressor {
	public static void main(String[] args) throws Exception {
		String codecClassname = args[0];
		Class<?> codecClass = Class.forName(codecClassname);
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec)
				ReflectionUtils.newInstance(codecClass, conf);
		Compressor compressor = null;
		try {
			compressor = CodecPool.getCompressor(codec);
			CompressionOutputStream out =
					codec.createOutputStream(System.out, compressor);
			IOUtils.copyBytes(System.in, out, 4096, false);
			out.finish();
		} finally {
			CodecPool.returnCompressor(compressor);
		}
	}
}
```

#### 4.2.2 压缩与分片

在MapReduce中，对输入数据启用压缩时，必须考虑压缩格式是否支持分片。不支持分片的压缩数据，将整个分给一个MapTask。

一般按照以下顺序选择压缩格式。

- 使用容器格式文件。如顺序文件、RCFile、Avro文件，这些个事都支持压缩和切片。一般与快速压缩工具（LZO、LZ4、Snappy等）结合使用。
- 使用支持切片的压缩工具，如bizp2等。
- 将输入文件切分为合适的大小后再压缩。
- 存储为压缩的文件。

#### 4.2.3 在MapReduce中使用压缩

使用CompressionCodecFactory推断CompressionCodec，Hadoop在读取压缩文件时会自动执行解压。
想要压缩MapReduce的输出，在Job中配置`mapreduce.output.fileoutputformat.compress`为 true， `mapreduce.output.fileoutputformat.compress.codec` 配置为压缩类名。
或者像下面这样，在FileOutputFormat中设置这些属性。

```java
public class MaxTemperatureWithCompression {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
                    "<output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(MaxTemperature.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

使用下面的命令执行。输入压缩格式与输出不必相同，这里使用了相同的。

```shell
% hadoop MaxTemperatureWithCompression input/ncdc/sample.txt.gz output
```

如果要输出顺序文件，使用`mapreduce.output.fileoutputformat.compress.type`属性控制输出格式。默认是RECORD，即针对每条数据进行压缩。建议使用BLOCK，对一组数据进行压缩，效率较高。

下表总结了上述的压缩属性配置。如果MapReduce使用了Tool接口，可以使用命令行传递这些属性。

| Property name                            | Type       | Default value                            |
| ---------------------------------------- | ---------- | ---------------------------------------- |
| mapreduce.output.fileoutputformat.compress | boolean    | false                                    |
| mapreduce.output.fileoutputformat.compress.codec | Class name | org.apache.hadoop.io.compress.DefaultCodec |
| mapreduce.output.fileoutputformat.compress.type | String     | RECORD                                   |

##### 4.2.3.2 Map输出启用压缩

对MapTask输出启用压缩,减少传递到Reducer的网络开销。使用下表中属性配置。

| Property name                       | Type    | Default value                            | Description |
| ----------------------------------- | ------- | ---------------------------------------- | ----------- |
| mapreduce.map.output.compress       | boolean | false                                    | 是否压缩Map输出   |
| mapreduce.map.output.compress.codec | Class   | org.apache.hadoop.io.compress.DefaultCodec | 压缩Codec     |

示例代码。

```java
Configuration conf = new Configuration();
conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);
Job job = new Job(conf);
```

### 4.3 序列化

Hadoop内部节点间通信使用RPC协议。序列化格式使用了Hadoop自身的Writeable接口及其继承接口和实现类。

#### 4.3.1 Writeable接口

Writeable接口定义了两个方法，写入数据到DataOutput二进制流和从DataInput二进制流读取数据。

```java
public interface Writable {
  /** 
   * Serialize the fields of this object to <code>out</code>.
   * 
   * @param out <code>DataOuput</code> to serialize this object into.
   * @throws IOException
   */
  void write(DataOutput out) throws IOException;

  /** 
   * Deserialize the fields of this object from <code>in</code>.  
   * 
   * <p>For efficiency, implementations should attempt to re-use storage in the 
   * existing object where possible.</p>
   * 
   * @param in <code>DataInput</code> to deseriablize this object from.
   * @throws IOException
   */
  void readFields(DataInput in) throws IOException;
}
```

##### WritableComparable 和 comparator

IntWritable、Text、NullWritable等类，都是实现了WritableComparable\<T>接口，WritableComparable继承了Writable和java.langComparable接口。
这些类中，都使用了Hadoop的一个优化类WritableComparator，它实现了RawComparable接口，作为RawComparable的实例工厂使用。RawComparable定义了`compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)`方法，允许直接比较数据流中的记录，无需先将数据流反序列化。

```java
RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);

IntWritable w1 = new IntWritable(163);
IntWritable w2 = new IntWritable(67);
assertThat(comparator.compare(w1, w2), greaterThan(0));

byte[] b1 = serialize(w1);
byte[] b2 = serialize(w2);
assertThat(comparator.compare(b1, 0, b1.length, b2, 0, b2.length), greaterThan(0));
```

#### 4.3.2 Writable类

Writable类结构图。

![HadoopIO - Writable - Class Architecture](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\HadoopIO - Writable - Class Architecture.png)

##### 1. Writable类封装器

除char外（可以用IntWritable），所有Java基础类型都有对应的Writable封装类。

| Java primitive | Writable implementation | Serialized size (bytes) |
| -------------- | ----------------------- | ----------------------- |
| boolean        | BooleanWritable         | 1                       |
| byte           | ByteWritable            | 1                       |
| short          | ShortWritable           | 2                       |
| int            | IntWritable             | 4                       |
|                | VIntWritable            | 1–5                     |
| float          | FloatWritable           | 4                       |
| long           | LongWritable            | 8                       |
|                | VLongWritable           | 1–9                     |
| double         | DoubleWritable          | 8                       |

VIntWritable和VLongWritable是变长封装器，如果封装的数值在-127~127之间，边长封装器只用一个字节存储数据，否则第一个字节表示正负和后面有多少个字节。

```java
byte[] data = serialize(new VIntWritable(163));
assertThat(StringUtils.byteToHexString(data), is("8fa3"));
```

关于定长/变长的选择，倾向于数值在整个空间分布比较均匀时选择定长，否则选择后者比较节省空间。

##### 2. Text

Text是针对UTF-8序列的封装类，一般可以认为是java.lang.String的封装类。最大存储为2GB。
由于Text本质是UTF-8序列的封装，所以在有关Unicode、索引、迭代操作时，可能出现预期外结果，比如`charat()`方法返回的不是char类型值，而是一个表示Unicode编码位置的Int值。因此强烈建议调用Text的`toString()`方法转为String后再操作。
关于与String的具体不同参考原书。

##### 3. BytesWritable

BytesWritable是对二进制数组的封装。其序列化格式以一个4字节整数开头，指定其包含数据的字节数。后面跟数据内容。

```java
BytesWritable b = new BytesWritable(new byte[] { 3, 5 });
byte[] bytes = serialize(b);
assertThat(StringUtils.byteToHexString(bytes), is("000000020305"));
```

##### 4. NullWritable

序列化长度为零，作为占位符使用。可以将键或值声明为NullWritable。

##### 5. ObjectWritable和GenericWritable

对Java基本类型（String、enum、Writable、null或这些类型组成的数组）的封装。如果一个SequenceFile中包含多个类型，适合使用ObjectWritable。如果包含的类型数量较少且都是已知的，可以使用GenericWritable的继承类，并声明支持什么类型。

##### 6. 集合类

 `org.apache.hadoop.io` 包含6个集合类封装： ArrayWritable,、ArrayPrimitiveWritable,、TwoDArrayWritable,、MapWritable、SortedMapWritable、EnumSetWritable。

ArrayWritable和TwoDArrayWritable是对Writable数组和二维数组的实现。声明时必须指定类型。

```java
ArrayWritable writable = new ArrayWritable(Text.class);
```

ArrayPrimitiveWritable是对Java基本数组类型的封装。
MapWritable实现了 java.util.Map\<Writable, Writable>，SortedMapWritable实现了 java.util.SortedMap\<WritableComparable, Writable>。

```java
 MapWritable src = new MapWritable();
 src.put(new IntWritable(1), new Text("cat"));
 src.put(new VIntWritable(2), new LongWritable(163));

 MapWritable dest = new MapWritable();
 WritableUtils.cloneInto(dest, src);
 assertThat((Text) dest.get(new IntWritable(1)), is(new Text("cat")));
 assertThat((LongWritable) dest.get(new VIntWritable(2)),
 is(new LongWritable(163)));
```

#### 4.3.3 自定义Writable

相对于实现自定义Writable，更推荐使用诸如Avro等序列化框架。

```java
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public TextPair(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    @Override
    public int compareTo(TextPair tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }
}
```

##### 1. 实现自定义RawComparator进行优化

```java
public static class Comparator extends WritableComparator {

    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    public Comparator() {
        super(TextPair.class);
    }
    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {

        try {
            int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
            int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
            int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            if (cmp != 0) {
                return cmp;
            }
            return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                    b2, s2 + firstL2, l2 - firstL2);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
static {
        WritableComparator.define(TextPair.class, new Comparator());
        }
```

##### 2. 自定义Comparator

```java
public static class FirstComparator extends WritableComparator {

    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    public FirstComparator() {
        super(TextPair.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {

        try {
            int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
            int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
            return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof TextPair && b instanceof TextPair) {
            return ((TextPair) a).first.compareTo(((TextPair) b).first);
        }
        return super.compare(a, b);
    }
}
```

### 4.4 序列化框架

出于效率考虑，Hadoop没有使用Java的序列化机制，而是自己实现了一套API。外挂的序列化框架通过使用这些API加入到Hadoop中。
序列化框架需要一个`org.apache.hadoop.io.serializer` 包中Serialization的实现类，这个实现类定义了对象/二进制流之间的转换映射方式。
Hadoop的 `io.serializations`属性定义了一串逗号分隔的序列化类。默认只有org.apache.hadoop.io.serializer.WritableSerialization 和 Avro的实现类。
Hadoop也有一个Java序列化机制的实现类，JavaSerialization，不建议使用。

##### IDL

 *interface description language* ，接口定义语言，脱离具体编程语言的方式进行接口声明。

### 4.5 Avro

略过，参考[官网](https://avro.apache.org/)。

*Avro*是一种远程过程调用和数据序列化框架，是在Apache的Hadoop项目之内开发的。它使用JSON来定义数据类型和通讯协议，使用压缩二进制格式来序列化数据。

### 4.6 基于文件的数据结构

某些Application需要特定的数据结构，而将每个大对象都放到单独的文件里不太现实，Hadoop提供了更高层次的解决方案，基于文件的数据结构。

#### 4.6.1 SequenceFile

SequenceFile文件中可以存储包括Writable、KV在内的任何类型数据，也可以作为小文件的容器，将多个小文件包含到大文件中，适应HDFS。

##### 1. 写操作

```java
public class SequenceFileWriteDemo {

    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            // 创建SequenceFile对象
            writer = SequenceFile.createWriter(fs, conf, path,
                    key.getClass(), value.getClass());

            for (int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                // getLength()获取文件当前位置
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                // 追加键值对内容
                writer.append(key, value);
            }
        } finally {
            // 关闭
            IOUtils.closeStream(writer);
        }
    }
}
```

输出结果

```shell
% hadoop SequenceFileWriteDemo numbers.seq
[128] 100 One, two, buckle my shoe
[173] 99 Three, four, shut the door
[220] 98 Five, six, pick up sticks
[264] 97 Seven, eight, lay them straight
[314] 96 Nine, ten, a big fat hen
[359] 95 One, two, buckle my shoe
[404] 94 Three, four, shut the door
[451] 93 Five, six, pick up sticks
[495] 92 Seven, eight, lay them straight
[545] 91 Nine, ten, a big fat hen
...
[1976] 60 One, two, buckle my shoe
[2021] 59 Three, four, shut the door
[2088] 58 Five, six, pick up sticks
[2132] 57 Seven, eight, lay them straight
[2182] 56 Nine, ten, a big fat hen
...
[4557] 5 One, two, buckle my shoe
[4602] 4 Three, four, shut the door
[4649] 3 Five, six, pick up sticks
[4693] 2 Seven, eight, lay them straight
[4743] 1 Nine, ten, a big fat hen
```

##### 2. 读操作

```java
public class SequenceFileReadDemo {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            // getKeyClass()、getValueClass()推断SequenceFile内容类型
            Writable key = (Writable)
                    ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable)
                    ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            // next()读取下一条，文件末尾时返回false
            /** 
                对于非Writable类型框架（Avro等），使用下面两个方法
                  public Object next(Object key) throws IOException
                  public Object getCurrentValue(Object val) throws IOException
            */
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition(); // beginning of next record
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
```

SequenceFile会记录同步点，以在读取的实例出错后，可以再次同步。SequenceFile.Writer会在文件中每个几项就写入一个特殊标识作为同步点，可以调用SequenceFile.Writer的sync()方法手动插入同步点。下面的输出中，星号位置就是同步点位置。

```shell
% hadoop SequenceFileReadDemo numbers.seq
[128] 100 One, two, buckle my shoe
[173] 99 Three, four, shut the door
[220] 98 Five, six, pick up sticks
[264] 97 Seven, eight, lay them straight
[314] 96 Nine, ten, a big fat hen
[359] 95 One, two, buckle my shoe
[404] 94 Three, four, shut the door
[451] 93 Five, six, pick up sticks
[495] 92 Seven, eight, lay them straight
[545] 91 Nine, ten, a big fat hen
[590] 90 One, two, buckle my shoe
...
[1976] 60 One, two, buckle my shoe
[2021*] 59 Three, four, shut the door
[2088] 58 Five, six, pick up sticks
[2132] 57 Seven, eight, lay them straight
[2182] 56 Nine, ten, a big fat hen
...
[4557] 5 One, two, buckle my shoe
[4602] 4 Three, four, shut the door
[4649] 3 Five, six, pick up sticks
[4693] 2 Seven, eight, lay them straight
[4743] 1 Nine, ten, a big fat hen
```

SequenceFile读取指定位置有两个方法，seek()和sync()。

```java
 // 跳至指定位置，如果该位置不是记录边界，抛出IOException
 reader.seek(359);
 assertThat(reader.next(key, value), is(true));
 assertThat(((IntWritable) key).get(), is(95));

```

```java
 // 自动跳至指定位置后一个同步点，没有同步点的话跳至文件末尾
 reader.sync(360);
 assertThat(reader.getPosition(), is(2021L));
 assertThat(reader.next(key, value), is(true));
 assertThat(((IntWritable) key).get(), is(59));
```

##### 3. 命令行显示SequenceFile

`hadoop fs -text`可以展示SequenceFile文件内容，支持gzip格式。

```shell
% hadoop fs -text numbers.seq | head
100 One, two, buckle my shoe
99 Three, four, shut the door
98 Five, six, pick up sticks
97 Seven, eight, lay them straight
96 Nine, ten, a big fat hen
95 One, two, buckle my shoe
94 Three, four, shut the door
93 Five, six, pick up sticks
92 Seven, eight, lay them straight
91 Nine, ten, a big fat hen
```

##### 4. 合并排序

SequenceFile.Sorter可以合并排序SequenceFile，但很旧不建议使用。推荐使用MapReduce。

```shell
% hadoop jar \
 $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
 sort -r 1 \
 -inFormat org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat \
 -outFormat org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat \
 -outKey org.apache.hadoop.io.IntWritable \
 -outValue org.apache.hadoop.io.Text \
 numbers.seq sorted
% hadoop fs -text sorted/part-r-00000 | head
1 Nine, ten, a big fat hen
2 Seven, eight, lay them straight
3 Five, six, pick up sticks
4 Three, four, shut the door
5 One, two, buckle my shoe
6 Nine, ten, a big fat hen
7 Seven, eight, lay them straight
8 Five, six, pick up sticks
9 Three, four, shut the door
10 One, two, buckle my shoe
```

##### 5. SequenceFile文件格式

SequenceFile由文件头和记录组成。文件前三字节为“SEQ”（SequenceFile文件代码）。第四字节是SequenceFile的版本号。文件头还包含诸如键/值类名、数据压缩细节、用户定义元数据、同步标识等。每个文件都会随机生成一个同步标识，位于记录之间。

记录的格式分为三种，无压缩、记录压缩、块压缩。如下面两张图。

![HadoopIO - Serialization - SequenceFileFormat](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\HadoopIO - Serialization - SequenceFileFormat.png)

![HadoopIO - Serialization - SequenceFileFormat - BlockCompress](D:\CloudStorage\Dropbox\Note\_NotePicture\Subject\Hadoop\Hadoop权威指南\HadoopIO - Serialization - SequenceFileFormat - BlockCompress.png)

块压缩中，一个Block可以存储多条记录，直到其大小达到`io.seqfile.compress.blocksize`属性指定的大小，默认 1MB。

#### 4.6.2 MapFile

MapFile是已经排过序的SequenceFile，有索引，可以按键查找。
MapFile的读写API与SequenceFile非常类似，使用 MapFile.Reader/Writer。不过MapFile文件存储记录的键值类型必须是WritableComparable和Writable，并且写入时必须按序写入，否则会抛出IOException。
MapFile文件实际包含两个文件，index（索引）和data（数据）文件。索引记录一部分Key，以及Key对应记录在data文件中的偏移量。
`io.map.index.interval`属性设置索引Key的频率，默认128，即每128条数据记录一个Key。
当调用 get(key)时，会将index加载到内存中，然后二分查找找到最接近的Key，然后到data中顺序读取。即每次按Key获取数据，要进行一次二分查找并读取最多128条数据。
大型文件的索引也很大，可以通过`io.map.index.skip`设置跳过一部分索引。默认为0，即不跳过。

#### 4.6.3 MapFIle变种

- SetFile - 存储Writable键的集合，必须按序排列
- ArrayFile - 键是一个整形，值是Writable类型
- BloomFile - 针对稀疏文件特别优化的格式，使用一个[布隆过滤器](http://www.cnblogs.com/allensun/archive/2011/02/16/1956532.html)来查找给定Key是否存在，但可能出现误判

#### 4.6.4 SequenceFile转MapFile

将SequenceFile排序后保存，使用MapFile.fix()方法重建索引。

##### 4.6.5 其他格式与列式存储

现在更建议使用Avro。SequenceFile、MapFile、Avro都是列式存储。