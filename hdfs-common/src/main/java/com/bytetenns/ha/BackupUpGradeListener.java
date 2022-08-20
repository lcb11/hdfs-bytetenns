package com.bytetenns.ha;

/**
 * BackupNode升级监听器
 *
 * @author Sun Dasheng
 */
public interface BackupUpGradeListener {

    /**
     * BackNode 升级
     *
     * @param hostname BackupNode的主机名
     */
    void onBackupUpGrade(String hostname);

}
