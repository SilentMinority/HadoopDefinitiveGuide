# 二、MapReduce





####一、二	概述

- Mapper读入文本，key是当前行相对于文件起始位置的偏移量，value是文本数据。

- Mapper的输入数据被分为等长数据块InputSplit。Hadoop为每个InputSplit建立一个MapTask。

- MapTask输出保存在本地，不做多节点备份。ReduceTask输出保存到HDFS。​


#### 三、HDFS

- 文件分块

  - HDFS系统中，对大文件会分块（Block）进行存储
  - 块大小一般为64M或128M
  - 小于一个块大小的文件不会占用一个块大小的实际磁盘空间
  - 文件分块优点
    - 文件可以是任意大小，甚至超过一个磁盘的大小
    - 简化存储管理、元数据管理等等的设计。
    - 对块进行复制、备份，保证数据可靠性。
  - 块大小一设置为64/128MB优点
    - Block设置较大，相应的，Block数量就会减少。NameNode需要存储的元数据也会减少，节省内存。
    - 减少硬盘寻址开销
  - Block不宜设置的过大。因为MapReduce任务通常只处理一个Block中的数据，Task数量太少，计算速度会变慢。

- NameNode&DataNode

  - NameNode保存HDFS所有元数据
    - 文件与block关联关系、目录相关等信息，持久保存在image和editslog中。
    - Block位置信息，不持久保存，在DataNode启动时上报给NameNode。
  - Client提供访问HDFS的接口。通过与NameNode和DataNode交互检索数据。
  - DataNode管理数据块block，为NameNode和客户端提供检索服务，定期向NameNode提供所存储的Block列表。

- 文件权限

  - HDFS默认通过进程的用户名和组进行权限验证。但应注意，这种情况下对远程访问用户等于不设防，因此对集群运行环境安全性有较高要求
  - Hadoop已支持Kerberos验证，[参考一](http://dongxicheng.org/mapreduce/hadoop-kerberos-introduction/)|[参考二](http://www.cloudera.com/content/www/zh-CN/documentation/enterprise/5-3-x/topics/cdh_sg_cdh5_hadoop_security.html)
  - HDFS权限检查开关：dfs.permission

- HDFS集群

  - [Federation（联邦）](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-hdfs/Federation.html)
    - **超大**规模HDFS系统中，随着文件数的增长，NameNode内存会成为系统瓶颈。
    - 在Federation集群中，存在多个NameNode，每个NameNode维护一个NamespaceVolume下的文件元数据。
    - 各个节点相互独立，互不通信、互不影响。
    - DataNode需要注册到每一个NameNode。
  - [HA高可用](./Hadoop HA.md)

- 文件系统

  - Hadoop一般通过URI选择合适的文件系统实例进行交互。例如`hdfs dfs -ls file:///`列出物理机本地根目录文件

- 接口

  - HTTP

    - HDFS提供两种HTTP访问方式。
      - 直接访问NameNode/DataNode内嵌的Web服务器（端口分别是50070、50075），此时NameNode以XML或JSON形式存储目录列表，DataNode以数据流形式提供数据传输
      - 通过一台或多台代理服务器访问HDFS服务器，这种方式可以采用更严格的防火墙、带宽策略。一般使用代理服务器实现不同数据中心Hadoop集群的数据交换

  - C语言

    - Hadoop提供了libhdfs类库，开发进度落后于Java

  - Fuse

    - 可以使用Fuse-DFS将HDFS作为标准文件系统进行挂载，之后可使用任何语言调用POSIX库访问

  - Java

    - **FsUrlStreamHandlerFactory**读取数据 最简单直接方法，使用java.net.URL读取HDFS文件。但是每个虚拟机只能有一个FsUrlStreamHandlerFactory实例，可能因为冲突导致失败。

      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

      ```
      InputStream inputStream = inputStream = new URL("hdfs://host/path").openStream();
      IOUtils.copyBytes(inputStream, System.out, 4096, true);
      ```

    - **FileSystem**读取数据 标准文件系统API

      FileSystem fileSystem = FileSystem.get(new Configuration());

      ```
      InputStream inputStream = fileSystem.open(new Path("/path"));
      IOUtils.copyBytes(inputStream, System.out, 4096, true);
      ```

    - **FileSystem**写入数据 使用了Progressable接口的构造方法构造的实例，会回调Progressable报告进度表

      InputStream inputStream = new BufferedInputStream(new FileInputStream("local"));

      ```
      FileSystem fileSystem = FileSystem.get(new Configuration());
      OutputStream outputStream = fileSystem.create(new Path("/path"), new Progressable() {
      	public void progress() {
      		System.out.println(".");
      	}
      });
      IOUtils.copyBytes(inputStream, outputStream, 4096, true);
      ```

    - **FileSystem**操作目录

      Path path = new Path("/path");

      ```
      new FileSystem(new Configuration()).mkdirs(path);
      ```

    - **FileStatus**文件元数据

      Path path = new Path("/user");

      ```
      FileStatus[] fss = fs.listStatus(path);
      for (FileStatus fs : fss) {
      	System.out.println(fs.getPath() + "  " + fs.getAccessTime() + "  " + fs.getLen());
      }
      ```

    - **FileSystem**删除文件

      FileSystem fileSystem = FileSystem.get(new Configuration());

      ```
      fileSystem.delete(new Path("/path"), false);  // false 如果目标是目录，抛出异常。true直接删除
      ```

- 数据流