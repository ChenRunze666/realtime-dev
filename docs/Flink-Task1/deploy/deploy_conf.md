#### 在/etc/profile.d/my_env.sh中配置
	export HADOOP_CLASSPATH=`hadoop classpath`

#### 修改yarn-site.xml配置  container最小和最大值以及nm分配的内存
	<property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>1024</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>2048</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>4096</value>
    </property>

#### 修改hadoop下的capacity-scheduler.xml配置  
	这里意思是集群中可用于运行应用程序主机的最大资源百分比 - 控制并发活动应用程序的数量。每个队列的限制与其队列容量和用户限制成正比。官方文档默认是每个队列最大使用10%的资源，把这里的0.1按需更改最大使用的资源数就行了
	<property>
	    <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
	    <value>1</value>
	</property>

#### 修改flink客户端配置
    jobmanager.memory.process.size: 1024m
    taskmanager.memory.process.size: 1024m
    taskmanager.numberOfTaskSlots: 2