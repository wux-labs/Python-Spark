# Spark On Hive

## 原理

对于Hive来说，包含两部分的内容：

* SQL执行引擎，将SQL翻译成MapReduce并提交到Yarn执行
* MetaStore，元数据管理中心

对于Spark来说：

* **自身就是一个执行引擎，自身就可以执行SQL**
* 但是Spark没有元数据管理功能

我们手动指定数据源的时候，比如：指定读取HDFS上的文件、指定JDBC数据源，Spark完全有能力从这些地方加载数据并进行执行。因为此时的表是来自DataFrame注册的，DataFrame中有数据、有字段、有字段类型，有这些信息Spark就可以工作。

但是当我们没有手动指定数据源就直接编写SQL代码，比如：

```
df = spark.sql("select * from hive_table_test")
```

Spark虽然有能力将上面的SQL变成RDD/DataFrame，但会出现问题：hive_table_test的数据在哪里？hive_table_test有哪些字段？字段类型是啥？Spark就完全不知道了。也就没办法正常工作了，这就是因为没有元数据。

## 解决方案

将Spark与Hive结合起来：

* Spark提供执行引擎
* Hive提供元数据管理

于是就有了Spark On Hive。

> **Spark On Hive：**
>
> 将Spark构建在Hive之上，Spark需要用到Hive的元数据管理功能。
>
> * 在Spark的配置文件中配置Hive相关的信息
> * 将Hive的依赖（MySQL驱动）拷贝到Spark的jars下
> * 用户通过编写Spark代码来进行数据处理
>
> **Hive On Spark：**
>
> 将Hive构建到Spark之上，Hive需要用到Spark的执行引擎功能。
>
> * 在Hive的配置文件中配置执行引擎为Spark
> * 将Spark的依赖拷贝到Hive的lib下
> * 用户通过编写Hive代码（Hive SQL）来进行数据处理

## 配置

根据原理，只需要告诉Spark元数据在哪里就可以了，也就是只需要让Spark能够连接上Hive的MetaStore服务就可以了。

在Spark的conf目录中，创建并配置`hive-site.xml`文件：

```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
  <property>
    <name>hive.metastore.local</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://node1:9083</value>
  </property>
</configuration>
```

并且需要确认：

* 数据库驱动包拷贝到Spark的jars目录下
* Hive配置了元数据服务
* Hive的元数据服务正常启动



