package com.bytetenns.namenode.fs;

import com.bytetenns.namenode.editlog.EditLogWrapper;

/**
 * 回放
 *
 * @author Sun Dasheng
 */
public interface PlaybackEditLogCallback {

    /**
     * 回放
     *
     * @param editLogWrapper editLogWrapper
     */
    void playback(EditLogWrapper editLogWrapper);
}
