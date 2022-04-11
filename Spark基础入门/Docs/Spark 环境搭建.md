# Spark 环境搭建

## Spark 安装程序下载

首先通过Spark的官方网站[https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)下载Spark的安装程序。

![image-20220409232626566](images/image-20220409232626566.png)

或者直接通过下载链接[https://www.apache.org/dyn/closer.lua/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz](https://www.apache.org/dyn/closer.lua/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz)进行下载。

将下载好的安装程序放置到softs目录下。

![image-20220409233328618](images/image-20220409233328618.png)

## Local 模式搭建

### 基本原理

本质：**启动一个JVM Process进程(一个进程里面有多个线程)，执行任务Task**。

* Local模式可以限制模拟Spark集群环境的线程数量，即Local[N] 或 Local[\*]。
* 其中N代表可以使用N个线程，每个线程拥有一个CPU Core。如果不指定N，则默认是1个线程（该线程有1个core）。 通常CPU有几个Core，就指定几个线程，最大化利用计算能力。
* 如果是local[*]，则代表 Run Spark locally with as many worker threads as logical cores on your machine。按照CPU最多的Cores设置线程数。

Local 下的角色分布：

资源管理：

* Master：Local进程本身
* Worker：Local进程本身

任务执行：

* Driver：Local进程本身
* Executor：不存在，没有独立的Executor角色，而是由Local进程(也就是Driver)内的线程提供计算能力

> Driver也算一种特殊的Executor，只不过多数时候，我们将Executor当做纯Worker对待，这样和Driver好区分  
> 注意：Local模式只能运行一个Spark程序，如果执行多个Spark程序，那就是由多个相互独立的Local进程在执行

### 环境搭建

开箱即用：**解压安装包之后，直接运行bin目录下的相关命令即可**。

创建apps目录，并将Spark安装包解压到apps目录下。

```
mkdir -p apps
tar -xzf softs/spark-3.2.1-bin-hadoop3.2.tgz -C apps/
ls -al apps/
```

![image-20220409233711398](images/image-20220409233711398.png)

切换到Spark的安装路径下。

```
cd ~/apps/spark-3.2.1-bin-hadoop3.2
ls -al
```

![image-20220409234023251](images/image-20220409234023251.png)

### 环境验证

#### 测试spark-shell

spark-shell 程序，可以提供一个交互式的 Scala 解释器环境，在这里面可以用 Scala 语言调用 Spark API 进行计算。

```
bin/spark-shell
```

![image-20220409235345935](images/image-20220409235345935.png)

* Spark Web UI：http://10.114.14.168:4040
* Spark 版本：version 3.2.1
* Scala 版本：Scala version 2.12.15
* sc：SparkContext实例对象
* spark：SparkSession实例对象
* 4040：Web监控页面端口号

```
scala> sc.parallelize(List(1,2,3,4,5)).map(x => x * 2).collect()
```

![image-20220410000130855](images/image-20220410000130855.png)

#### 测试pyspark

pyspark程序，可以提供一个交互式的 Python 解释器环境，在这里面可以用 Python 语言调用 Spark API 进行计算。

```
bin/pyspark
```

![image-20220410000558432](images/image-20220410000558432.png)

* Spark Web UI：http://10.114.14.168:4040
* Spark 版本：version 3.2.1
* Python 版本：Python version 3.6.8
* sc：SparkContext实例对象
* spark：SparkSession实例对象
* 4040：Web监控页面端口号

```
>>> sc.parallelize([1,2,3,4,5]).map(lambda x: x + 1).collect()
```

![image-20220410000752583](images/image-20220410000752583.png)

#### 测试Spark Web UI

访问http://10.114.14.168:4040。

![image-20220410000957475](images/image-20220410000957475.png)

通过上面的界面，我们可以看到提交的job。

![image-20220410001230714](images/image-20220410001230714.png)

在Executors页面，我们可以发现仅有一个Driver程序。因为我们是Local模式，Driver即管理又干活。

同时，输入jps可以看到local模式下的唯一进程存在，这个进程既是master也是worker。

![image-20220410001446123](images/image-20220410001446123.png)

#### 测试spark-submit

spark-submit程序，作用：提交指定的Spark代码到Spark环境中运行。

语法：

```
bin/spark-submit [可选的一些选项] jar包或者Python代码的路径 [代码的参数]
```

示例：

```
bin/spark-submit --class org.apache.spark.examples.SparkPi examples/jars/spark-examples_2.12-3.2.1.jar 10
```

```
bin/spark-submit examples/src/main/python/pi.py 10
```

#### spark-shell/pyspark/spark-submit 对比

| 程序     | spark-shell                                                  | pyspark                                                      | spark-submit                               |
| -------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------ |
| 功能     | 提供一个交互式的 Scala 解释器环境，在这里面可以用 Scala 语言调用 Spark API 进行计算。 | 提供一个交互式的 Python 解释器环境，在这里面可以用 Python 语言调用 Spark API 进行计算。 | 提交指定的Spark代码到Spark环境中运行。     |
| 特点     | 交互式环境，写一行执行一行                                   | 交互式环境，写一行执行一行                                   | 提交代码用                                 |
| 使用场景 | 学习/测试/交互式验证等                                       | 学习/测试/交互式验证等                                       | 生产场景/正式场合，正式提交Spark程序运行。 |

## Standalone 模式搭建

### 基本架构

Standalone模式是Spark自带的一种集群模式，不同于本地模式启动多个进程来模拟集群的环境，Standalone模式是真实地在多个机器之间搭建Spark集群的环境，完全可以利用该模式搭建多机器集群，用于实际的大数据处理。

Standalone 是完整的Spark运行环境，其中：

* Master角色以Master进程存在
* Worker角色以Worker进程存在
* Driver和Executor运行于Worker进程内，由Worker提供资源供给它们运行

Spark架构设计将资源管理单独抽象出一层Cluster Manager，通过这种抽象能够构建一种适合企业当前技术栈的插件式资源管理模块，从而为不同的计算场景提供不同的资源分配与调度策略。

![image-20220411222446909](images/image-20220411222446909.png)

Cluster Manager支持：Standalone、Yarn、Mesos……

Standalone集群在进程上主要有3类进程：

* 主节点Master进程：Master角色，管理整个集群资源，并托管运行各个任务的Driver
* 从节点Workers：Worker角色，管理每个机器的资源，分配对应的资源来运行Executor(Task)；每个从节点分配资源信息给Worker管理，资源信息包含内存Memory和CPU Cores核数
* 历史服务器HistoryServer(可选)：Spark Application运行完成以后，保存事件日志数据至HDFS，启动HistoryServer可以查看应用运行相关信息

### 环境搭建

由于Standalone需要多台服务器，一般至少3台服务器，但是我们并没有多台服务器可用，所以环境搭建我们采用Docker容器进行搭建。具体步骤参照[实验2 Spark Standalone模式搭建](../Labs/实验2 Spark Standalone模式搭建.md)。
