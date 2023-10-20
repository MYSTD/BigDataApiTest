package com.atguigu.bigdata.spark.sql;

import java.io.Serializable;

public class AvgAgeBuffer implements Serializable {
    private Long total;
    private Long count;

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public AvgAgeBuffer(Long total, Long count) {
        this.total = total;
        this.count = count;
    }

    public AvgAgeBuffer() {
    }
}