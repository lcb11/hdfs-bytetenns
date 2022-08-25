package com.bytetenns.client.tools;


import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.bytetenns.client.FileSystemImpl;
import com.bytetenns.client.config.FsClientConfig;
import com.bytetenns.client.tools.command.Command;

/**
 * 控制台工具类
 *
 * @author LiZhirun
 */
public class DfsCommand {

    public static void main(String[] args) throws Exception {
        ClientCommandOption option = new ClientCommandOption(args);
        option.checkArgs();
        FsClientConfig fsClientConfig = FsClientConfig.builder()
                .server(option.getOptionSet().valueOf(option.getServerOpt()))
                .port(option.getOptionSet().valueOf(option.getPortOpt()))
                .connectRetryTime(1)
                .build();
        FileSystemImpl fileSystem = new FileSystemImpl(fsClientConfig);
        fileSystem.setCommandLineListener(new CommandLineListener() {
            @Override
            public void onConnectFailed() {
                System.out.println("连接NameNode失败.");
                System.exit(1);
            }
        });
        fileSystem.start();
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
