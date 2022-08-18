package com.bytetenns.backupnode.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 用户变更事件
 */
@Getter
@AllArgsConstructor
public enum UserChangeEventEnum {

    /**
     * 新增用户
     */
    ADD(1),
    /**
     * 修改用户
     */
    MODIFY(2),
    /**
     * 删除用户
     */
    DELETE(3),
    ;
    private int value;

    public static UserChangeEventEnum getEnum(int value) {
        for (UserChangeEventEnum nameNodeLaunchMode : values()) {
            if (nameNodeLaunchMode.value == value) {
                return nameNodeLaunchMode;
            }
        }
        return ADD;
    }

}
