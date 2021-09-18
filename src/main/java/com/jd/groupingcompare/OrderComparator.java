package com.jd.groupingcompare;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author FM
 * @Description
 * @create 2021-05-26 21:52
 */
public class OrderComparator extends WritableComparator {

    public OrderComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa=(OrderBean)a;
        OrderBean ob=(OrderBean)b;
        return oa.getOrderId().compareTo(ob.getOrderId());
    }
}
