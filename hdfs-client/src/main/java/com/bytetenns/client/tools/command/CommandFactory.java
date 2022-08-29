package com.bytetenns.client.tools.command;

import com.bytetenns.client.tools.CommandReader;

/**
 * 命令工厂
 *
 * @author LiZhirun
 */
public class CommandFactory {

    public static Command getCommand(String command, String currentPath, CommandReader commandReader) {
        if (command == null) {
            return new NoOpCommand();
        }
        command = command.trim();
        if ("pwd".equalsIgnoreCase(command)) {
            return new PwdCommand(currentPath, command);
        } else if ("ls".equalsIgnoreCase(command)) {
            return new ListCommand(currentPath, command);
        } else if (command.startsWith("cd")) {
            return new CdCommand(currentPath, command, commandReader);
        } else if (command.startsWith("get")) {
            return new GetCommand(currentPath, command);
        } else if (command.startsWith("put")) {
            return new PutCommand(currentPath, command);
        } else if (command.startsWith("attr")) {
            return new AttrCommand(currentPath, command);
        } else if (command.startsWith("node")) {
            return new NodeCommand(currentPath, command);
        } else if (command.startsWith("stat")) {
            return new StatCommand(currentPath, command);
        }
        
        if (command.length() > 0) {
            System.out.println("无法识别命令：" + command);
        }
        return new NoOpCommand();
    }


}
