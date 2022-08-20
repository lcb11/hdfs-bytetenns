package com.bytetenns.client;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import com.bytetenns.client.config.FsClientConfig;
import com.bytetenns.utils.PrettyCodes;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
public class FileSystemTest {

    private static final String DOWNLOAD_PATH = System.getProperty("user.dir") + "/../client-download/";
    private static final String UPLOAD_LOCAL_PATH = System.getProperty("user.dir") + "/../img/architecture.png";

    private FileSystem getFileSystem() throws Exception {
        FsClientConfig fsClientConfig = FsClientConfig.builder()
                .username("admin")
                .secret("admin")
                .server("localhost")
                .port(2341)
                .ack(1)
                .build();
        return FsClient.getFileSystem(fsClientConfig);
    }

    private String putFile(FileSystem fileSystem) throws Exception {
        Random random = new Random();
        Map<String, String> attr = new HashMap<>(PrettyCodes.trimMapSize());
        attr.put("aaa", "1222");
        String filename = "/usr/local/TestPicture-" + random.nextInt(100000000) + ".jpg";
        fileSystem.put(filename, new File(UPLOAD_LOCAL_PATH), -1, attr);
        return filename;
    }

    @Test
    public void testMkdir() throws Exception {
        FileSystem fileSystem = getFileSystem();
        fileSystem.mkdir("/usr/local/kafka");
    }

    @Test
    public void testPut() throws Exception {
        FileSystem fileSystem = getFileSystem();
        putFile(fileSystem);
        fileSystem.close();
    }


    @Test
    public void testMultiPut() throws InterruptedException {
        int multiCount = 50;
        CountDownLatch latch = new CountDownLatch(multiCount);
        for (int i = 0; i < multiCount; i++) {
            new Thread(() -> {
                try {
                    FileSystem fileSystem = getFileSystem();
                    putFile(fileSystem);
                    fileSystem.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
    }


    @Test
    public void testReadAttr() throws Exception {
        FileSystem fileSystem = getFileSystem();
        String filename = putFile(fileSystem);
        Map<String, String> attrFromServer = fileSystem.readAttr(filename);
        assertNotNull(attrFromServer);
        assertEquals(attrFromServer.get("aaa"), "1222");
    }


    @Test
    public void testRemoveFile() throws Exception {
        FileSystem fileSystem = getFileSystem();
        String filename = putFile(fileSystem);
        fileSystem.remove(filename);
    }


    @Test
    public void testGet() throws Exception {
        FileSystem fileSystem = getFileSystem();
        String filename = putFile(fileSystem);
        fileSystem.get(filename, DOWNLOAD_PATH + filename,
                (total, current, progress, currentReadBytes) -> log.info("下载文件进度：{}", progress));
    }

    @Test
    public void testEstablishConnect() throws Exception {
        FileSystem fileSystem = getFileSystem();
    }

}
