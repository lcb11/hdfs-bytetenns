package com.bytetenns.backupnode.fs;

import com.bytetenns.dfs.model.backup.INode;
import com.bytetenns.common.enums.NodeType;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 代表文件目录，包含一堆子文件或者子文件目录
 */
@Data
@Slf4j
public class Node {

    private String path;
    private int type;
    private final TreeMap<String, Node> children;
    //节点属性
    private Map<String, String> attr;
    private Node parent;

    public Node() {
        this.children = new TreeMap<>();
        this.attr = new HashMap<>();
        this.parent = null;
    }

    public Node(String path, int type) {
        this();
        this.path = path;
        this.type = type;
    }

    /**
     * 是否是一个文件
     * @return 是否是一个文件
     */
    public boolean isFile() {
        return type == NodeType.FILE.getValue();
    }

    /**
     * 获取当前节点的全名路径
     * @return 当前节点的全路径
     */
    public String getFullPath() {
        return getFullPathInternal(this);
    }

    private String getFullPathInternal(Node parent) {
        if (parent == null) {
            return null;
        }
        String parentPath = getFullPathInternal(parent.getParent());
        if (parentPath == null) {
            return "";
        }
        return parentPath + "/" + parent.path;
    }

    public static INode toINode(Node node) {
        INode.Builder builder = INode.newBuilder();
        String path = node.getPath();
        int type = node.getType();
        builder.setPath(path);
        builder.setType(type);
        builder.putAllAttr(node.getAttr());
        Collection<Node> children = node.getChildren().values();
        if (children.isEmpty()) {
            return builder.build();
        }
        List<INode> tmpNode = new ArrayList<>(children.size());
        for (Node child : children) {
            INode iNode = toINode(child);
            tmpNode.add(iNode);
        }
        builder.addAllChildren(tmpNode);
        return builder.build();
    }

    public static Node parseINode(INode iNode) {
        return parseINode(iNode, null);
    }

    public static Node parseINode(INode iNode, String parent) {
        Node node = new Node();
        if (parent != null && log.isDebugEnabled()) {
            log.debug("parseINode executing :[path={},  type={}]", parent, node.getType());
        }
        String path = iNode.getPath();
        int type = iNode.getType();
        node.setPath(path);
        node.setType(type);
        node.putAllAttr(iNode.getAttrMap());
        List<INode> children = iNode.getChildrenList();
        if (children.isEmpty()) {
            return node;
        }
        for (INode child : children) {
            node.addChildren(parseINode(child, parent == null ? null : parent + "/" + child.getPath()));
        }
        return node;
    }


    /**
     * 深度拷贝节点
     * @param node  节点
     * @param level 拷贝多少个孩子层级
     * @return 拷贝节点
     */
    public static Node deepCopy(Node node, int level) {
        if (node == null) {
            return null;
        }
        Node ret = new Node();
        String path = node.getPath();
        int type = node.getType();
        ret.setPath(path);
        ret.setType(type);
        ret.putAllAttr(node.getAttr());
        if (level > 0) {
            TreeMap<String, Node> children = node.children;
            if (!children.isEmpty()) {
                for (String key : children.keySet()) {
                    ret.addChildren(deepCopy(children.get(key), level - 1));
                }
            }
        }
        return ret;
    }

    /**
     * 添加一个孩子节点
     * @param child 孩子节点
     */
    public void addChildren(Node child) {
        synchronized (children) {
            child.setParent(this);
            this.children.put(child.getPath(), child);
        }
    }

    /**
     * 获取孩子节点
     * @param child 孩子节点
     */
    public Node getChildren(String child) {
        synchronized (children) {
            return children.get(child);
        }
    }

    public void putAllAttr(Map<String, String> attr) {
        this.attr.putAll(attr);
    }

    @Override
    public String toString() {
        return "Node{" +
                "path='" + path + '\'' +
                ", type=" + type +
                ", children=" + children +
                ", attr=" + attr +
                '}';
    }

}
