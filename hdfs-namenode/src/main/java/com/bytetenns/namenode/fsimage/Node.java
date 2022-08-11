package com.bytetenns.namenode.fsimage;

import java.util.List;

/**
 * @Author lcb
 * @Description 代表文件目录树当中的一个目录或者一个文件
 * @Date 2022/8/10
 * @Param
 * @return
 **/
public class Node {

    //节点路径
    private String path;
    //孩子节点集合
    private List<Node> children;

    public Node() {
    }

    public Node(String path, List<Node> children) {
        this.path = path;
        this.children = children;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }
}
