package com.jd.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author FM
 * @Description
 * @create 2021-05-29 21:04
 */
public class TopNComparator extends WritableComparator {

    public TopNComparator() {
        super(TopNFlowBean.class,true);
    }

    /**
     * 决定reducer阶段的分组概念
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }
}
