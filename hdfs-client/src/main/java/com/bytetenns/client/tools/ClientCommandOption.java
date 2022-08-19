package com.ruyuan.dfs.client.tools;

import com.ruyuan.dfs.client.utils.CommandLineUtils;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import lombok.Data;

/**
 * 客户端命令行工具
 *
 * @author Sun Dasheng
 */
@Data
public class ClientCommandOption {

    private OptionParser parser;
    private OptionSpec<String> usernameOpt;
    private OptionSpec<String> secretOpt;
    private OptionSpec<String> serverOpt;
    private OptionSpec<Integer> portOpt;
    private OptionSet optionSet;

    public ClientCommandOption(String[] args) {
        this.parser = new OptionParser(false);
        this.usernameOpt = parser.accepts("username", "用户名")
                .withRequiredArg()
                .ofType(String.class);
        this.secretOpt = parser.accepts("secret", "秘钥")
                .withRequiredArg()
                .ofType(String.class);
        this.serverOpt = parser.accepts("server", "NameNode节点地址")
                .withRequiredArg()
                .ofType(String.class);
        this.portOpt = parser.accepts("port", "NameNode节点端口")
                .withRequiredArg()
                .ofType(Integer.class);
        this.optionSet = parser.parse(args);
    }

    /**
     * 校验参数
     */
    public void checkArgs() {
        CommandLineUtils.checkRequiredArgs(parser, optionSet, usernameOpt, secretOpt, secretOpt, portOpt);
    }
}
