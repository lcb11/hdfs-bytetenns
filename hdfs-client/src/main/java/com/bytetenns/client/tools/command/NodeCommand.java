package com.ruyuan.dfs.client.tools.command;

import com.ruyuan.dfs.client.FileSystem;
import com.ruyuan.dfs.client.utils.ConsoleTable;
import com.ruyuan.dfs.common.utils.FileUtil;
import com.ruyuan.dfs.model.namenode.ClientDataNode;
import com.ruyuan.dfs.model.namenode.ClientDataNodeInfo;
import com.ruyuan.dfs.model.namenode.ClientNameNodeInfo;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.jline.reader.LineReader;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 读取配置命令
 *
 * <p>node --role=namenode 获取namenode基本信息：配置、backup信息、slot信息</p>
 * <p>node --role=datanode 获取datanode的基本信息</p>
 *
 * @author Sun Dasheng
 */
public class NodeCommand extends AbstractCommand {

    public NodeCommand(String currentPath, String command) {
        super(currentPath, command);
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) throws Exception {
        String roleInfo = getArgs(0);
        if (roleInfo == null) {
            System.out.println("无法识别命令：" + command);
            return;
        }
        OptionParser optionParser = new OptionParser(false);
        OptionSpec<String> role = optionParser.accepts("role")
                .withRequiredArg()
                .ofType(String.class);
        OptionSet optionSet = optionParser.parse(roleInfo);
        if (!optionSet.has(role)) {
            System.out.println("缺少参数 --node");
            return;
        }
        String nodeRole = optionSet.valueOf(role);
        if ("namenode".equals(nodeRole)) {
            ClientNameNodeInfo clientNameNodeInfo = fileSystem.nameNodeInfo();
            System.out.println("NameNode 配置信息: ");
            Map<String, String> configMap = clientNameNodeInfo.getConfigMap();
            printMap(configMap);
            System.out.println();
            System.out.println("Backup信息：" + clientNameNodeInfo.getBackup());
            System.out.println();
            System.out.println("Slot基本信息：");
            Map<Integer, Set<Integer>> nodeSlotsMap = new HashMap<>(2);
            for (Map.Entry<Integer, Integer> entry : clientNameNodeInfo.getSlotsMap().entrySet()) {
                Set<Integer> slots = nodeSlotsMap.computeIfAbsent(entry.getValue(), k -> new HashSet<>());
                slots.add(entry.getKey());
            }
            for (Map.Entry<Integer, Set<Integer>> entry : nodeSlotsMap.entrySet()) {
                List<String> collect = entry.getValue().stream()
                        .sorted()
                        .map(String::valueOf)
                        .limit(10)
                        .collect(Collectors.toList());
                System.out.println(entry.getKey() + " => " + String.join(",", collect) + "...共(" + entry.getValue().size() + ")");
            }
        } else if ("datanode".equals(nodeRole)) {
            ClientDataNodeInfo clientDataNodeInfo = fileSystem.dataNodeInfo();
            List<ClientDataNode> clientDataNodesList = clientDataNodeInfo.getClientDataNodesList();
            List<ConsoleTable.Cell> header = new ArrayList<ConsoleTable.Cell>() {{
                add(new ConsoleTable.Cell("nodeId"));
                add(new ConsoleTable.Cell("hostname"));
                add(new ConsoleTable.Cell("status"));
                add(new ConsoleTable.Cell("useSpace"));
                add(new ConsoleTable.Cell("freeSpace"));
            }};
            for (ClientDataNode clientDataNode : clientDataNodesList) {
                List<List<ConsoleTable.Cell>> body = new ArrayList<>();
                List<ConsoleTable.Cell> rows = new ArrayList<>(3);
                rows.add(new ConsoleTable.Cell(String.valueOf(clientDataNode.getNodeId())));
                rows.add(new ConsoleTable.Cell(clientDataNode.getHostname()));
                rows.add(new ConsoleTable.Cell(clientDataNode.getStatus() == 2 ? "Ready" : "Starting"));
                rows.add(new ConsoleTable.Cell(FileUtil.formatSize(clientDataNode.getStoredDataSize())));
                rows.add(new ConsoleTable.Cell(FileUtil.formatSize(clientDataNode.getFreeSpace())));
                body.add(rows);
                new ConsoleTable.ConsoleTableBuilder().addHeaders(header).addRows(body).build().print();
            }
        }
    }
}
