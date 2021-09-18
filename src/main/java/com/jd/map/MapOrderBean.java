package com.jd.map;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-27 20:52
 */
public class MapOrderBean implements WritableComparable<MapOrderBean> {

    private String id;

    private String pid;

    private int amount;

    private String pname;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public String toString() {
        return id + "\t" + pid + "\t" + amount + "\t" + pname;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
    }

    @Override
    public int compareTo(MapOrderBean bean) {
        int result = this.pid.compareTo(bean.getPid());
        if(result==0){
            return bean.getPname().compareTo(this.getPname());
        }
        return result;
    }
}
