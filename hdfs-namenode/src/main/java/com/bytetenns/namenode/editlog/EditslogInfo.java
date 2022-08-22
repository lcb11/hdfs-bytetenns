package com.bytetenns.namenode.editlog;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
  * @Author lcb
  * @Description  editslog文件信息
  * @Date 2022/8/19
  * @Param
  * @return
  **/
@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EditslogInfo implements Comparable<EditslogInfo> {

    //todo stat end 代表什么
    private long start;
    private long end;
    private String name;

    @Override
    public int compareTo(EditslogInfo o) {
        return (int) (this.start - o.start);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EditslogInfo that = (EditslogInfo) o;
        return start == that.start &&
                end == that.end &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, name);
    }
}
