package com.ruyuan.dfs.client.tools;

import com.ruyuan.dfs.common.network.file.OnProgressListener;

/**
 * 多文件传输进度监听器
 *
 * @author Sun Dasheng
 */
public class OnMultiFileProgressListener implements OnProgressListener {

    private OnProgressListener listener;
    private int fileCount;
    private int currentFile = 0;

    public OnMultiFileProgressListener(OnProgressListener listener, int fileCount) {
        this.listener = listener;
        this.fileCount = fileCount;
    }

    @Override
    public void onProgress(long total, long current, float progress, int currentReadBytes) {
        int base = 100 / fileCount;
        float readProgress = (base * progress / 100.0F) + currentFile * base;
        if (listener != null) {
            listener.onProgress(total * fileCount, currentFile * total + current,
                    readProgress, currentReadBytes);
        }
    }

    @Override
    public void onCompleted() {
        currentFile++;
        if (currentFile == fileCount) {
            if (listener != null) {
                listener.onCompleted();
            }
        }
    }
}
