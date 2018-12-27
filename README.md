  **MyCAT-SF是在开源社区版本基础上对易用性，稳定性等方面进行大量优化的分布式数据库中间件产品。其核心功能是分库分表功能，通过MyCAT-SF对外提供MySQL数据库统一访问接口，使业务系统数据操作具有可扩展性，支持高并发，海量存储的能力。**



#### 快速开始

####  1.1 环境准备

##### 1.1.1 install JDK

​	由于MyCAT-SF是通过JAVA语言开发实现的，所以需要JAVA的运行环境，推荐下载JDK8或更高版本。
（下载地址：http://www.oracle.com/technetwork/java/javase/downloads）

##### 1.1.2 MySQL

MyCAT-SF支持多种数据库的接入，这里主要推荐使用MySQL 5.6、MySQL 5.7。
（下载地址：http://www.mysql.com/downloads）



#### 1.2 源码编译和打包

​	MyCAT-SF项目采用maven进行管理，可以使用maven对源码进行编译打包。

##### 1.2.1 Maven的下载和安装

​	从官网http://maven.apache.org/download.cgi 可以下载maven的压缩包。解压压缩包，将bin的路径添加到PATH环境变量中。以windows为例，在cmd中输入mvn -v测试，若返回maven的版本信息，则说明配置成功。

##### 1.2.2 使用Maven打包源码

​	在项目源码的根目录下输入命令：mvn clean package。mvn会对项目源码进行编译、测试和打包，打包成功后会在源码的根目录下生成一个target文件夹。打开target文件夹可以找到多个压缩包，不同的压缩包对应不同的操作系统。

##### 1.3 运行MyCAT-SF服务端

​	这里的例子主要是在window环境下启动一个本地的MyCAT-SF服务端。

##### 1.3.1 编辑配置文件

​	以windows为例，解压压缩包，可以在目录conf下找到几个主要的配置文件。

在schema.xml中配置逻辑库逻辑表

```
<schema name="logicdb" checkSQLschema="false" sqlMaxLimit="10000">
	<table name="testtable" primaryKey="id" dataNode="node1, node2" ></table>
</schema>
```

​	这里的逻辑库的名字为logicdb，逻辑表的名字为testtable，分片字段为id，逻辑表有两个分片，分别位于node1和node2这两dataNode上。

在database.xml中配置分片信息

```
<dataNode name="node1" dataHost="localhost" database="db1" />
<dataNode name="node2" dataHost="localhost" database="db2" />

<dataHost name="localhost" maxCon="200" minCon="10" balance="0" dbType="mysql" dbDriver="native" switchType="-1">
	<heartbeat>select user()</heartbeat>
	<writeHost host="hostM1" url="127.0.0.1:3306" user="user" password="password"></writeHost>
</dataHost>
```

​	其中，node1和node2对应schema.xml中配置的dataNode，其中dataHost代表dataNode所在的主机，database是实际的数据库名。在dataHost的配置中，url是主机地址及端口号，user和password是访问实际数据库的用户名和密码，这里配置的是本地的数据库。

在user.xml中配置访问服务端的用户名和密码

```
<user name="test">
	<property name="password">test</property>
	<property name="schemas">logicdb</property>
</user>
```

​	这里将用户名和密码都配置成test，另外还要配置逻辑库的名字logicdb。

##### 1.3.2 创建分片数据库

​	完成配置后，按照database.xml中配置的分片信息，分别在本地新建数据库

```
mysql> create database db1;
Query OK, 1 row affected (0.01 sec)

mysql> create database db2;
Query OK, 1 row affected (0.01 sec)
```

##### 1.3.3 运行服务端

​	执行bin目录下的mycat脚本即可启动MyCAT-SF服务端

##### 

##### 1.4 使用MyCAT-SF服务

​	完成MyCAT-SF服务端的配置和启动之后，就可以使用MyCAT-SF提供的服务。由于服务端实现了mysql协议，所以访问MyCAT-SF服务端的方式与访问mysql服务端相同。根据server.xml中配置的用户名和密码，在命令行窗口输入

```
>mysql.exe -utest -ptest -P8066
Warning: Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1
Server version: 5.5.8-mycat-1.5.3-RELEASE-20161025135147 MyCat Server (OpenCloundDB)

Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.
```

成功登录本地的MyCAT-SF服务端。

此时逻辑库中没有任何表，可以按照database.xml定义的逻辑表信息新建一个表：

```
mysql> create table testtable (
    -> id int primary key,
    -> name varchar(20)
    -> ) engine = innodb default character set = 'utf8';
Query OK, 0 rows affected (0.21 sec)
```

创建成功后就可以用正常的MySQL语句来操作MyCAT-SF了。
