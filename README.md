# 项目介绍 #
这是2022字节青训营大数据方向-项目实践三-简易分布式存储系统的简单实现
# 项目架构 #
![image](https://user-images.githubusercontent.com/46276651/187206005-49d8c2e9-5982-4fa3-85f0-80f43f18d76b.png)
# 编译&运行 #

## 初始化 ##

## 启动NameNode ##
NameNode启动类为`hdfs-bytetenns\hdfs-namenode\src\main\java\com\bytetenns\namenode\NameNode.java`
直接点击运行即可
## 启动BackupNode ##
BackupNode启动类为：`hdfs-backupnode/src/main/java/com/bytetenns/backupnode/BackupNode.java`    

BackupNode启动前需要修改配置文件，设置本地文件存储路径和backupNode的启动地址端口，添加nameNode的地址端口，启动参数配置如下：
`hdfs-backupnode/src/main/java/com/bytetenns/backupnode/config/BackupNodeConfig.java`  
```text
{
private String baseDir = "/bytetenns/hdfs/backupnode"; // 本地文件存放路径
private long fetchEditLogInterval = 5000; // 每次拉取editslog的间隔
private int fetchEditLogSize = 10; // 每次拉取editslog的数量
private long checkpointInterval = 3600000; // checkpoint操作的时间间隔, 默认60分钟
private String nameNodeServer = "localhost:2345"; // namenode地址
private String backupNodeServer = "localhost:12341"; // backupNode地址
}
```

## 启动DataNode ##
在`conf/datanode.properties`中配置相应的参数，然后配置输入参数，比如在`.vscode/launch.json`中配置如下：
```json
{
    "type": "java",
    "name": "Launch DataNode",
    "request": "launch",
    "mainClass": "com.bytetenns.datanode.DataNode",
    "projectName": "hdfs-datanode",
    "vmArgs": "-Dlogback.configurationFile=conf/logback-datanode.xml",
    "args": "conf/datanode.properties"
}
```

## 运行客户端和单元测试 ##
Client启动需要配置输入参数，比如`.vscode/launch.json`中配置如下：

```json
{
    "type": "java",
    "name": "Launch DfsCommand",
    "request": "launch",
    "mainClass": "com.bytetenns.client.tools.DfsCommand",
    "projectName": "hdfs-client",
    "args": ["-port=2345", "-server=localhost"]
}
```
