[上一页](operations.md)
[回目录](../../README.md)
[下一页](db.md)

# 移植

作为开源产品，我们更希望的是大家一起贡献；比如发现代码bug，或者有新的优化，我们更希望大家直接发PR到开源仓库上，所以我们更希望的是大家直接采用qmq的开源分支，而不是fork自己的分支改动，因为一旦做出这样的改动之后，不仅仅是你修的bug很难合并到开源分支，而且开源分支修的bug也很难合并到内部的fork分支。
但是每个公司的情况和运维环境千差万别，比如监控平台不同，配置中心也可能不同，DB也可能不一样，为此qmq采用了SPI的机制，将一些可能存在定制的地方抽取接口，然后只需要提供实现即可，不用对qmq的源码进行任何修改。
下面对哪些SPI做一些说明，如果你觉得有哪些地方还可以提取为SPI可以向我们发送PR：

## 配置
对于配置来讲，现在很多公司已经采用了集中式的配置中心，但是也有一些公司仍然基于本地的配置文件。qmq默认提供了基于本地配置文件的方式获取配置，也提供了SPI的方式，你可以很容易通过扩展(而不是修改)的方式接入自己公司的配置中心:
```java
package qunar.tc.qmq.configuration;

public interface DynamicConfigFactory {
    DynamicConfig create(String name, boolean failOnNotExist);
}
```
你只需要在另外一个独立的模块里实现上面这个接口，然后在该模块的resources/META-INF/services下建立一个qunar.tc.qmq.configuration.DynamicConfigFactory的纯文本文件，然后内容是你的实现类即可。

## DB
qmq的metaserver和backup都有对db的依赖，但是不同的公司对db的管理方式不一样，比如有的公司直接用ip，端口号和用户名密码这种原生的方式直接连接数据库，而有的公司扩展了数据源，需要用内部定制的DataSource才可以。那么qmq提供了DataSource构建的SPI:
```java
package qunar.tc.qmq.jdbc;

import javax.sql.DataSource;

import qunar.tc.qmq.configuration.DynamicConfig;

public interface DataSourceFactory {
	DataSource createDataSource(DynamicConfig config);
}
```
qmq默认提供了基于ip，端口，用户名和密码的方式，只需要在datasource.properties的配置里提供下面的信息即可:
```java
package qunar.tc.qmq.jdbc;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import qunar.tc.qmq.configuration.DynamicConfig;

public class DefaultDataSourceFactory implements DataSourceFactory {

	@Override
	public DataSource createDataSource(DynamicConfig config) {
		final HikariConfig cpConfig = new HikariConfig();
		cpConfig.setDriverClassName(config.getString("jdbc.driverClassName", "com.mysql.jdbc.Driver"));
		cpConfig.setJdbcUrl(config.getString("jdbc.url"));
		cpConfig.setUsername(config.getString("jdbc.username"));
		cpConfig.setPassword(config.getString("jdbc.password"));
		cpConfig.setMaximumPoolSize(config.getInt("pool.size.max", 10));

		return new HikariDataSource(cpConfig);
	}
}
```
但如果你们采用的是自定义数据源，也能很方便的进行扩展:
```java
package com.company.mq.jdbc;

public class XxxDataSourceFactory implements DataSourceFactory {

	@Override
	public DataSource createDataSource(DynamicConfig config) {
		return new XxxDataSource();
	}
}
```
然后在模块的resources/META-INF/services下放置qunar.tc.qmq.jdbc.DataSourceFactory的纯文本文件，内容是你的实现类，比如: 
```
com.company.mq.jdbc.XxxDataSourceFactory
```

## 监控
每个公司都有自己的监控平台，监控的接入方式也不相同，对于监控如何接入请参照: [监控](monitor.md)


[上一页](operations.md)
[回目录](../../README.md)
[下一页](db.md)