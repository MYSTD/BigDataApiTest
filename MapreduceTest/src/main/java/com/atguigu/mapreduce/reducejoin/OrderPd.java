package com.atguigu.mapreduce.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用于封装order和pd的数据
 */
public class OrderPd implements Writable {

    private Integer orderId;
    private Integer pid;
    private Integer amount;
    private String pname;
    private String title;

    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public Integer getPid() {
        return pid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }


    @Override
    public String toString() {
        return orderId + "\t" + pname + "\t" + amount;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeInt(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(title);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readInt();
        pid = in.readInt();
        amount = in.readInt();
        pname = in.readUTF();
        title = in.readUTF();
    }
}
