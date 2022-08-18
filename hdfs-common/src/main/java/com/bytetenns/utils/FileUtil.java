package com.bytetenns.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * 文件IO工具集
 */
@Slf4j
public class FileUtil {
    /**
     * The number of bytes in a kilobyte.
     */
    public static final long ONE_KB = 1024;
    /**
     * The number of bytes in a kilobyte.
     *
     * @since 2.4
     */
    public static final BigDecimal ONE_KB_BI = BigDecimal.valueOf(ONE_KB);

    /**
     * The number of bytes in a megabyte.
     *
     * @since 2.4
     */
    public static final BigDecimal ONE_MB_BI = ONE_KB_BI.multiply(ONE_KB_BI);

    /**
     * The number of bytes in a gigabyte.
     *
     * @since 2.4
     */
    public static final BigDecimal ONE_GB_BI = ONE_KB_BI.multiply(ONE_MB_BI);

    /**
     * The number of bytes in a terabyte.
     *
     * @since 2.4
     */
    public static final BigDecimal ONE_TB_BI = ONE_KB_BI.multiply(ONE_GB_BI);


    /**
     * The number of bytes in a petabyte.
     *
     * @since 2.4
     */
    public static final BigDecimal ONE_PB_BI = ONE_KB_BI.multiply(ONE_TB_BI);

    /**
     * The number of bytes in an exabyte.
     *
     * @since 2.4
     */
    public static final BigDecimal ONE_EB_BI = ONE_KB_BI.multiply(ONE_PB_BI);


    /**
     * NIO方式写文件
     *
     * @param path       文件路径
     * @param delOldFile 是否删除旧文件
     * @param buffer     内存缓冲区
     */
    public static void saveFile(String path, boolean delOldFile, ByteBuffer buffer) throws IOException {
        mkdirParent(path);
        File file = new File(path);
        if (delOldFile) {
            if (file.exists()) {
                delete(file);
            }
        }
        try (RandomAccessFile raf = new RandomAccessFile(path, "rw"); FileOutputStream fos =
                new FileOutputStream(raf.getFD()); FileChannel channel = fos.getChannel()) {
            channel.write(buffer);
            channel.force(true);
        }
    }

    /**
     * 读取文件
     *
     * @param path 文件路径
     */
    public static ByteBuffer readBuffer(String path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path, "r"); FileInputStream fis =
                new FileInputStream(raf.getFD()); FileChannel channel = fis.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate((int) raf.length());
            channel.read(buffer);
            buffer.flip();
            return buffer;
        }
    }

    /**
     * 读取文件
     *
     * @param path 文件路径
     */
    public static String readString(String path) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get(path));
        return new String(bytes, StandardCharsets.UTF_8);
    }


    /**
     * 删除文件或目录
     *
     * @param pathName 目录名
     * @return 删除结果
     */
    public static boolean delete(String pathName) {
        return delete(new File(pathName));
    }

    /**
     * 删除文件或目录
     *
     * @param file 文件
     * @return 删除结果
     */
    public static boolean delete(File file) {
        return FileUtils.deleteQuietly(file);
    }

    /**
     * 删除目录
     *
     * @param dir 目录
     */
    public static void deleteDirectory(File dir) throws IOException {
        FileUtils.deleteDirectory(dir);
    }

    /**
     * 创建文件的父目录存在
     *
     * @param fileName 文件名
     */
    public static void mkdirParent(String fileName) {
        File file = new File(fileName);
        mkdirs(file.getParent());
    }

    /**
     * 递归创建目录
     *
     * @param pathName 目录名
     */
    public static void mkdirs(String pathName) {
        File file = new File(pathName);
        file.mkdirs();
    }

    public static String fileMd5(String fileName) throws IOException {
        try (FileInputStream fis = new FileInputStream(fileName)) {
            return DigestUtils.md5Hex(fis);
        }
    }

    public static String md5(byte[] data) {
        return DigestUtils.md5Hex(data);
    }

    public static String formatSize(long length) {
        BigDecimal size = new BigDecimal(String.valueOf(length));
        String displaySize;

        if (size.divide(ONE_EB_BI, 2, RoundingMode.HALF_UP).compareTo(BigDecimal.ONE) > 0) {
            displaySize = size.divide(ONE_EB_BI, 2, RoundingMode.HALF_UP) + " EB";
        } else if (size.divide(ONE_PB_BI, 2, RoundingMode.HALF_UP).compareTo(BigDecimal.ONE) > 0) {
            displaySize = size.divide(ONE_PB_BI, 2, RoundingMode.HALF_UP) + " PB";
        } else if (size.divide(ONE_TB_BI, 2, RoundingMode.HALF_UP).compareTo(BigDecimal.ONE) > 0) {
            displaySize = size.divide(ONE_TB_BI, 2, RoundingMode.HALF_UP) + " TB";
        } else if (size.divide(ONE_GB_BI, 2, RoundingMode.HALF_UP).compareTo(BigDecimal.ONE) > 0) {
            displaySize = size.divide(ONE_GB_BI, 2, RoundingMode.HALF_UP) + " GB";
        } else if (size.divide(ONE_MB_BI, 2, RoundingMode.HALF_UP).compareTo(BigDecimal.ONE) > 0) {
            displaySize = size.divide(ONE_MB_BI, 2, RoundingMode.HALF_UP) + " MB";
        } else if (size.divide(ONE_KB_BI, 2, RoundingMode.HALF_UP).compareTo(BigDecimal.ONE) > 0) {
            displaySize = size.divide(ONE_KB_BI, 2, RoundingMode.HALF_UP) + " KB";
        } else {
            displaySize = size + " bytes";
        }
        return displaySize;
    }

    /**
     * 将存放在src目录下的源文件，打包成zipFile文件
     *
     * @param src         待压缩的文件路径
     * @param zipFilename zip文件
     */
    public static void zip(String src, String zipFilename, OnZipProgressListener listener) throws IOException {
        File sourceFile = new File(src);
        File[] sourceFiles = sourceFile.listFiles();
        File zipFile = new File(zipFilename);
        if (zipFile.exists()) {
            delete(zipFile);
        }
        try (FileOutputStream fos = new FileOutputStream(zipFile);
             ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(fos))) {
            zip(sourceFiles, "/", zos, listener);
        }
    }

    /**
     * 压缩ZIP
     *
     * @param files      多个文件
     * @param baseFolder 压缩到ZIP的父级目录(目录后面跟上File.separator)
     */
    private static void zip(File[] files, String baseFolder, ZipOutputStream zos,
                            OnZipProgressListener listener) throws IOException {
        if (files == null || files.length == 0) {
            return;
        }
        ZipEntry entry;
        byte[] buffer = new byte[1024 * 1024];
        for (File file : files) {
            if (file.isDirectory()) {
                zip(file.listFiles(), baseFolder + file.getName() + File.separator, zos, listener);
            } else {
                entry = new ZipEntry(baseFolder + file.getName());
                zos.putNextEntry(entry);
                try (FileInputStream fis = new FileInputStream(file)) {
                    int count;
                    while ((count = fis.read(buffer, 0, buffer.length)) != -1) {
                        zos.write(buffer, 0, count);
                        if (listener != null) {
                            listener.onWriteBytes(count);
                        }
                    }
                    zos.closeEntry();
                }
            }
        }
    }

    public static void unzip(String zipFilePath, String destDir, OnZipProgressListener listener) throws Exception {
        File srcFile = new File(zipFilePath);
        if (!srcFile.exists()) {
            return;
        }
        ZipFile zipFile = new ZipFile(srcFile);
        Enumeration<?> entries = zipFile.entries();
        byte[] buffer = new byte[1024 * 1024];
        while (entries.hasMoreElements()) {
            ZipEntry entry = (ZipEntry) entries.nextElement();
            if (entry.isDirectory()) {
                srcFile.mkdirs();
            } else {
                File targetFile = new File(destDir + File.separator + entry.getName());
                mkdirParent(targetFile.getAbsolutePath());
                try (InputStream is = zipFile.getInputStream(entry);
                     FileOutputStream fos = new FileOutputStream(targetFile)) {
                    int len;
                    while ((len = is.read(buffer)) != -1) {
                        fos.write(buffer, 0, len);
                    }
                }
                if (listener != null) {
                    listener.onWriteBytes((int) entry.getCompressedSize());
                }
            }
        }
    }

    public interface OnZipProgressListener {
        /**
         * 写了xx字节
         *
         * @param bytes 字节数
         */
        void onWriteBytes(int bytes);
    }
}
