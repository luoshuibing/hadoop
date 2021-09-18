package com.jd.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-29 20:25
 */
public class TopNFlowBean implements WritableComparable<TopNFlowBean> {

    private long upFlow;

    private long downFlow;

    private long sumFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return this.upFlow+"\t"+this.downFlow+"\t"+this.sumFlow;
    }

    /**
     * 决定Map阶段的排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(TopNFlowBean o) {
        return Long.compare(o.getSumFlow(),this.getSumFlow());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow=in.readLong();
        this.downFlow=in.readLong();
        this.sumFlow=in.readLong();
    }
}
