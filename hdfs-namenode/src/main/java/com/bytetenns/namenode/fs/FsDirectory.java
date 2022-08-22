package com.bytetenns.namenode.fs;

import com.bytetenns.dfs.model.backup.INode;
import com.bytetenns.common.enums.NodeType;
import com.bytetenns.common.netty.Constants;
import com.bytetenns.common.utils.StringUtils;
import com.bytetenns.dfs.model.namenode.Metadata;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
   * @Author lcb
   * @Description 负责管理内存目录树的组件
   * @Date 2022/8/12
   * @Param
   * @return
   **/
@Slf4j
public class FsDirectory {
    private Node root;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public FsDirectory() {
        this.root = new Node("/", NodeType.DIRECTORY.getValue());
    }

    /**
     * 创建文件目录
     *
     * @param path 文件目录
     */
    public void mkdir(String path, Map<String, String> attr) {
        try {
            //获取写锁
            lock.writeLock().lock();
            //将路径划转换为String[]数组
            String[] paths = StringUtils.split(path, '/');
            Node current = root;
            for (String p : paths) {
                if ("".equals(p)) {
                    continue;
                }
                current = findDirectory(current, p);
            }
            //给当前节点设置属性信息
            current.putAllAttr(attr);
        } finally {
            //解锁
            lock.writeLock().unlock();
        }
    }

    /**
     * 创建文件
     *
     * @param filename 文件名
     * @return 是否创建成功
     */
    public boolean createFile(String filename, Map<String, String> attr) {
        try {
            //获取写锁
            lock.writeLock().lock();
            //将文件名划分
            String[] paths = StringUtils.split(filename, '/');
            //paths数组的最后一个元素代表文件名
            String fileNode = paths[paths.length - 1];
            Node fileParentNode = getFileParent(paths);
            Node childrenNode = fileParentNode.getChildren(fileNode);
            if (childrenNode != null) {
                log.warn("文件已存在，创建失败 : {}", filename);
                return false;
            }
            Node child = new Node(fileNode, NodeType.FILE.getValue());//FILE文件节点类型
            child.putAllAttr(attr);
            fileParentNode.addChildren(child);
            return true;
        } finally {
            //释放写锁
            lock.writeLock().unlock();
        }
    }

    private Node getFileParent(String[] paths) {
        Node current = root;
        //因为没有遍历了数组的的最后一个元素，最后一个元素的值代表文件名
        for (int i = 0; i < paths.length - 1; i++) {
            String p = paths[i];
            if ("".equals(p)) {
                continue;
            }
            current = findDirectory(current, p);
        }
        return current;
    }

    /**
     * 删除文件
     *
     * @param filename 文件名
     */
    public Node delete(String filename) {
        //获取写锁
        lock.writeLock().lock();
        try {
            String[] paths = StringUtils.split(filename, '/');
            String name = paths[paths.length - 1];
            Node current = getFileParent(paths);
            Node childrenNode;
            //如果文件名为null
            if ("".equals(name)) {
                //将当前节点的位置前移到上一级
                childrenNode = current;
            } else {
                //如果文件名不为空，就定位到该文件
                childrenNode = current.getChildren(name);
            }
            if (childrenNode == null) {
                log.warn("文件不存在, 删除失败：[filename={}]", filename);
                return null;
            }
            if (childrenNode.getType() == NodeType.DIRECTORY.getValue()) {
                if (!childrenNode.getChildren().isEmpty()) {
                    log.warn("文件夹存在子文件，删除失败：[filename={}]", filename);
                    return null;
                }
            }
            //删除父节点中保存的要删除节点的信息
            Node remove = current.getChildren().remove(name);

            // 删除空文件夹
            Node parent = remove.getParent();
            Node child = remove;
            //逐级删除文件，从下往上删除
            while (parent != null) {
                if (child.getChildren().isEmpty()) {
                    //将要删除节点中保存的父节点的信息置为null
                    child.setParent(null);
                    //将父节点的保存的孩子节点的信息删除
                    parent.getChildren().remove(child.getPath());
                }
                child = parent;
                parent = parent.getParent();
            }
            return Node.deepCopy(remove, Integer.MAX_VALUE);
        } finally {
            //释放锁
            lock.writeLock().unlock();
        }
    }

    private Node findDirectory(Node current, String p) {
        Node childrenNode = current.getChildren(p);
        if (childrenNode == null) {
            //如果当前节点的孩子节点不存在孩子节点，将给节点的类型设置为文件夹
            childrenNode = new Node(p, NodeType.DIRECTORY.getValue());
            current.addChildren(childrenNode);
        }
        //如果有childrenNode，返回该childrenNode
        current = childrenNode;
        return current;
    }

    /**
     * 根据内存目录树生成FsImage
     *
     * @return FsImage
     */
    public FsImage getFsImage() {
        try {
            lock.readLock().lock();
            INode iNode = Node.toINode(root);
            return new FsImage(0L, iNode);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 根据FSImage初始化内存目录树
     *
     * @param fsImage FSImage
     */
    public void applyFsImage(FsImage fsImage) {
        try {
            //获取写锁
            lock.writeLock().lock();
            //构造文件目录树，调用parseINode解析INode文件，转换为Node
            this.root = Node.parseINode(fsImage.getINode(), "");
        } finally {
            //不管有没有调用成功，都要解锁
            lock.writeLock().unlock();
        }
    }

    /**
     * 查看某个目录文件
     *
     * @param parent 目录路径
     * @return 文件路径
     */
    public Node listFiles(String parent, int level) {
        return Node.deepCopy(unsafeListFiles(parent), level);
    }


    /**
     * 查看某个目录文件
     *
     * @param parent 目录路径
     * @return 文件路径
     */
    public Node listFiles(String parent) {
        return listFiles(parent, Integer.MAX_VALUE);
    }

    /**
     * 查看某个目录文件
     *
     * @param parent 目录路径
     * @return 文件路径
     */
    public Node unsafeListFiles(String parent) {
        if (root.getPath().equals(parent)) {
            return root;
        }
        lock.readLock().lock();
        try {
            String[] paths = StringUtils.split(parent, '/');
            String name = paths[paths.length - 1];
            Node current = getFileParent(paths);
            return current.getChildren(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * <pre>
     *     假设存在文件：
     *
     *     /aaa/bbb/c1.png
     *     /aaa/bbb/c2.png
     *     /bbb/ccc/c3.png
     *
     * 传入：/aaa，则返回：[/bbb/c1.png, /bbb/c2.png]
     *
     * </pre>
     * <p>
     * 返回文件名
     */
    public List<String> findAllFiles(String path) {
        Node node = listFiles(path);
        if (node == null) {
            return new ArrayList<>();
        }
        return findAllFiles(node);
    }

    private List<String> findAllFiles(Node node) {
        List<String> ret = new ArrayList<>();
        if (node.isFile()) {
            ret.add(node.getFullPath());
        } else {
            for (String key : node.getChildren().keySet()) {
                Node child = node.getChildren().get(key);
                ret.addAll(findAllFiles(child));
            }
        }
        return ret;
    }


    public Set<Metadata> findAllFileBySlot(int slot) {
        return findAllFilesFilterBySlot(root, slot);
    }


    public Set<Metadata> findAllFilesFilterBySlot(Node node, int slot) {
        Set<Metadata> ret = new HashSet<>();
        if (node.isFile()) {
            String fullPath = node.getFullPath();
            int slotIndex = StringUtils.hash(fullPath, Constants.SLOTS_COUNT);
            if (slotIndex == slot) {
                ret.add(Metadata.newBuilder()
                        .setFileName(fullPath)
                        .setType(NodeType.FILE.getValue())
                        .putAllAttr(node.getAttr())
                        .build());
            }
        } else {
            for (String key : node.getChildren().keySet()) {
                Node child = node.getChildren().get(key);
                ret.addAll(findAllFilesFilterBySlot(child, slot));
            }
        }
        return ret;
    }
}
