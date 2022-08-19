package com.ruyuan.dfs.client.utils;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import lombok.extern.slf4j.Slf4j;

/**
 * 命令行工具
 *
 * @author Sun Dasheng
 */
@Slf4j
public class CommandLineUtils {

    private CommandLineUtils(){}
    /**
     * 检查列表中参数是否存在
     */
    public static void checkRequiredArgs(OptionParser parser, OptionSet optionSet, OptionSpec... optionSpecs) {
        for (OptionSpec arg : optionSpecs) {
            if (!optionSet.has(arg)) {
                printUsageAndDie(parser, "Missing required argument \"" + arg + "\"");
            }
        }
    }

    /**
     * 打印使用说明并退出
     */
    public static void printUsageAndDie(OptionParser parser, String message) {
        try {
            System.err.println(message);
            parser.printHelpOn(System.err);
            System.exit(1);
        } catch (Exception e) {
            log.error("printUsageAndDie error.", e);
        }
    }
}
