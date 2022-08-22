package com.bytetenns.namenode.server;


import com.bytetenns.common.network.file.FileAttribute;
import com.bytetenns.common.network.file.FileTransportCallback;
import com.bytetenns.common.scheduler.DefaultScheduler;
import com.bytetenns.namenode.NameNodeConfig;
import com.bytetenns.namenode.fs.DiskNameSystem;
import com.bytetenns.namenode.fs.FsImageClearTask;

/**
  * @Author lcb
  * @Description FsImage文件接受回调
  * @Date 2022/8/20
  * @Param
  * @return
  **/
public class FsImageFileTransportCallback implements FileTransportCallback {

    private FsImageClearTask fsImageClearTask;
    private DefaultScheduler defaultScheduler;
    private NameNodeConfig nameNodeConfig;

    public FsImageFileTransportCallback(NameNodeConfig nameNodeConfig, DefaultScheduler defaultScheduler, DiskNameSystem diskNameSystem) {
        this.nameNodeConfig = nameNodeConfig;
        this.defaultScheduler = defaultScheduler;
        this.fsImageClearTask = new FsImageClearTask(diskNameSystem, nameNodeConfig.getBaseDir(),
                diskNameSystem.getEditLog());
    }


    @Override
    public String getPath(String filename) {
        // 这里只接收FSImage文件，文件名写死就可以了
        return nameNodeConfig.getFsimageFile(String.valueOf(System.currentTimeMillis()));
    }

    @Override
    public void onCompleted(FileAttribute fileAttribute) {
        defaultScheduler.scheduleOnce("删除FSImage任务", fsImageClearTask, 0);
    }
}
