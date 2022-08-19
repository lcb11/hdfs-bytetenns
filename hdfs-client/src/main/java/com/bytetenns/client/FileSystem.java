package com.ruyuan.dfs.client;

import com.ruyuan.dfs.common.network.file.OnProgressListener;
import com.ruyuan.dfs.model.client.GetAllFilenameResponse;
import com.ruyuan.dfs.model.client.PreCalculateResponse;
import com.ruyuan.dfs.model.client.ReadStorageInfoResponse;
import com.ruyuan.dfs.model.namenode.ClientDataNodeInfo;
import com.ruyuan.dfs.model.namenode.ClientNameNodeInfo;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * 作为文件系统的接口
 *
 * @author Sun Dasheng
 */
public interface FileSystem {

    /**
     * 创建目录
     *
     * @param path 目录对应的路径
     * @throws Exception 文件不存在
     */
    void mkdir(String path) throws Exception;

    /**
     * 创建目录
     *
     * @param path 目录对应的路径
     * @param attr 文件属性
     * @throws Exception 文件不存在
     */
    void mkdir(String path, Map<String, String> attr) throws Exception;

    /**
     * 上传文件
     *
     * @param filename 服务器文件路径
     * @param file     本地文件
     * @throws Exception 文件不存在
     */
    void put(String filename, File file) throws Exception;

    /**
     * 上传文件
     *
     * @param filename     服务器文件路径
     * @param file         本地文件
     * @param numOfReplica 文件副本数量
     * @throws Exception 文件不存在
     */
    void put(String filename, File file, int numOfReplica) throws Exception;


    /**
     * 上传文件
     *
     * @param filename     服务器文件路径
     * @param file         本地文件
     * @param attr         文件属性
     * @param numOfReplica 文件副本数量
     * @throws Exception 文件不存在
     */
    void put(String filename, File file, int numOfReplica, Map<String, String> attr) throws Exception;

    /**
     * 上传文件
     *
     * @param filename     服务器文件路径
     * @param file         本地文件
     * @param attr         文件属性
     * @param numOfReplica 文件副本数量
     * @param listener     进度监听
     * @throws Exception 文件不存在
     */
    void put(String filename, File file, int numOfReplica, Map<String, String> attr, OnProgressListener listener) throws Exception;


    /**
     * 下载文件
     *
     * @param filename     存储的文件名
     * @param absolutePath 本地路径
     * @throws Exception 文件不存在
     */
    void get(String filename, String absolutePath) throws Exception;

    /**
     * 下载文件
     *
     * @param filename     存储的文件名
     * @param absolutePath 本地路径
     * @param listener     进度监听器
     * @throws Exception 文件不存在
     */
    void get(String filename, String absolutePath, OnProgressListener listener) throws Exception;

    /**
     * 删除文件
     *
     * @param filename 文件名
     * @throws Exception 文件不存在
     */
    void remove(String filename) throws Exception;

    /**
     * 读取文件属性
     *
     * @param filename 文件名
     * @return 文件属性
     * @throws Exception 文件不存在
     */
    Map<String, String> readAttr(String filename) throws Exception;

    /**
     * 关闭
     */
    void close();

    /**
     * 列出某个目录的文件列表
     *
     * @param path 文件目录
     * @return 文件信息
     * @throws Exception 异常
     */
    List<FsFile> listFile(String path) throws Exception;

    /**
     * 获取NameNode的基本信息
     *
     * @return NameNode基本信息
     * @throws Exception 异常
     */
    ClientNameNodeInfo nameNodeInfo() throws Exception;

    /**
     * 获取DataNode基本信息
     *
     * @return 基本信息
     * @throws Exception 异常
     */
    ClientDataNodeInfo dataNodeInfo() throws Exception;

    /**
     * 读取存储信息
     *
     * @param filename 文件名
     * @return 存储信息
     * @throws Exception 异常
     */
    ReadStorageInfoResponse readStorageInfo(String filename) throws Exception;

    /**
     * 计算某个路径下所有文件数量和存储大小
     *
     * @param path 文件路径
     * @return 结果
     * @throws Exception 异常
     */
    PreCalculateResponse preCalculatePath(String path) throws Exception;

    /**
     * 获取目录下所有的文件路径
     *
     * @param path 目录
     * @return
     */
    GetAllFilenameResponse getAllFilenameByPath(String path) throws Exception;
}
