package com.jd.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author FM
 * @Description
 * @create 2021-08-24 22:44
 */
public class Lower extends UDF {

    public String evaluate(String original){
        if(original==null){
            return null;
        }
        return original.toLowerCase();
    }

}
