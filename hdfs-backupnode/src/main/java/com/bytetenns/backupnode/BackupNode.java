package com.bytetenns.backupnode;

import com.bytetenns.backupnode.config.BackupNodeConfig;
import lombok.extern.slf4j.Slf4j;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * @Author jiaoyuliang
 * @Description 负责同步edits log线程|namenode 备份节点
 * @Date 2022/8/17
 */
@Slf4j
public class BackupNode {
    public static void main(String[] args) {
    // 1 读取配置文件 并进行解析
        // 1.1 判断args是否不为空
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("BackupNode配置文件不能为空");
        }
        // 1.2 读取配置文件
        BackupNodeConfig backupNodeConfig = null;
        try {
            Path path = Paths.get(args[0]); //获取配置文件路径
            try (InputStream inputStream = Files.newInputStream(path)) { //Input流读文件
                // 1.3 Properties类封装
                Properties properties = new Properties();
                properties.load(inputStream);
                // 1.4 解析到BacnkupNodeConfig文件中
                backupNodeConfig = BackupNodeConfig.parse(properties);
            }
            log.info("BackupNode启动配置文件：{}", path.toAbsolutePath());  //配置文件启动成功打印log日志
        } catch (Exception e) {
            log.error("配置类加载失败：{}", e);
            System.exit(1);  //配置文件读取失败退出系统
        }

        // 2 启动或关闭程序
        try {
            // 2.1 初始化backupNode启动类
            BackupNode backupNode = new BackupNode(backupNodeConfig);
            // 2.2 shutdown关闭
            Runtime.getRuntime().addShutdownHook(new Thread(backupNode::shutdown));
            // 2.3 start启动
            backupNode.start();
        } catch (Exception e) {
            log.error("BackupNode启动失败：", e);
            System.exit(1); //启动失败
        }
    }

    /**
     * 初始化BackupNode  有参构造
     * @param backupNodeConfig
     */
    public BackupNode(BackupNodeConfig backupNodeConfig) {

    }

    /**
     * 启动BackupNode
     * @throws Exception
     */
    private void start() {

    }

    /**
     * 关闭BackupNode
     */
    public void shutdown() {

    }

}
