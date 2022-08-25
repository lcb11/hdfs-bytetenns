# 项目介绍 #

# 项目架构 #

# 编译&运行 #

## 初始化 ##

## 启动NameNode ##

## 启动BackupNode ##
BackupNode启动类为：`hdfs-backupnode/src/main/java/com/bytetenns/backupnode/BackupNode.java`    

BackupNode启动前需要修改配置文件，设置本地文件存储路径和backupNode的启动地址端口，添加nameNode的地址端口，启动参数配置如下：  
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
    "args": ["-port=2345", "-secret=123456", "-server=localhost", "-username=root"]
}
```
