package com.ruyuan.dfs.client.tools;

import com.ruyuan.dfs.client.FileSystemImpl;
import com.ruyuan.dfs.client.config.FsClientConfig;
import com.ruyuan.dfs.client.tools.command.Command;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 控制台工具类
 *
 * @author Sun Dasheng
 */
public class DfsCommand {

    public static void main(String[] args) throws Exception {
        ClientCommandOption option = new ClientCommandOption(args);
        option.checkArgs();
        FsClientConfig fsClientConfig = FsClientConfig.builder()
                .username(option.getOptionSet().valueOf(option.getUsernameOpt()))
                .secret(option.getOptionSet().valueOf(option.getSecretOpt()))
                .server(option.getOptionSet().valueOf(option.getServerOpt()))
                .port(option.getOptionSet().valueOf(option.getPortOpt()))
                .connectRetryTime(1)
                .build();
        FileSystemImpl fileSystem = new FileSystemImpl(fsClientConfig);
        CountDownLatch latch = new CountDownLatch(1);  // 计数器，到达某个点后才能继续往下走
        AtomicBoolean auth = new AtomicBoolean(false);
        fileSystem.setCommandLineListener(new CommandLineListener() {
            @Override
            public void onConnectFailed() {
                System.out.println("连接NameNode失败.");
                System.exit(1);
            }

            @Override
            public void onAuthResult(boolean result) {
                auth.set(result);
                latch.countDown();
            }
        });
        fileSystem.start();
        latch.await();
        if (!auth.get()) {  // auth==false
            System.out.println("认证失败.");
            System.exit(1);
        } else {
            System.out.println("连接成功");
            Command command;
            String host = option.getOptionSet().valueOf(option.getServerOpt()) + ":" +
                    option.getOptionSet().valueOf(option.getPortOpt());
            CommandReader commandReader = new CommandReader(host);
            while ((command = commandReader.readCommand()) != null) {
                try {
                    command.execute(fileSystem, commandReader.getLineReader());
                } catch (Exception e) {
                    System.out.println("Error while execute command: " + e);
                }
            }
        }
    }
}
