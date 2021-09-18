package com.jd.mapreduce.flow;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-22 23:26
 */
@Data
public class FlowBean implements Writable {

    private Long upflow;

    private Long downflow;

    private Long sumFLow;

    public void set(Long upflow,Long downflow){
        this.upflow=upflow;
        this.downflow=downflow;
        this.sumFLow=upflow+downflow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upflow);
        out.writeLong(downflow);
        out.writeLong(sumFLow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upflow=in.readLong();
        this.downflow=in.readLong();
        this.sumFLow=in.readLong();
    }

    @Override
    public String toString(){
        return upflow+"\t"+downflow+"\t"+sumFLow;
    }

}
