package com.atguigu.bigdata.spark.sql;

import java.io.Serializable;
import java.util.*;

public class CityRemarkBuffer implements Serializable {
    private int totalcnt;
    private Map<String, Integer> cityMap;

    public int getTotalcnt() {
        return totalcnt;
    }

    public void setTotalcnt(int totalcnt) {
        this.totalcnt = totalcnt;
    }

    public Map<String, Integer> getCityMap() {
        return cityMap;
    }

    public void setCityMap(Map<String, Integer> cityMap) {
        this.cityMap = cityMap;
    }

    public CityRemarkBuffer(int totalcnt, Map<String, Integer> cityMap) {
        this.totalcnt = totalcnt;
        this.cityMap = cityMap;
    }

    public CityRemarkBuffer() {
    }
}
