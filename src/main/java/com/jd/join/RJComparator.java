package com.jd.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author FM
 * @Description
 * @create 2021-05-27 21:11
 */
public class RJComparator extends WritableComparator {

    public RJComparator() {
        super(RJOrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RJOrderBean oa = (RJOrderBean) a;
        RJOrderBean ob = (RJOrderBean) b;
        return oa.getPid().compareTo(ob.getPid());
    }
}
