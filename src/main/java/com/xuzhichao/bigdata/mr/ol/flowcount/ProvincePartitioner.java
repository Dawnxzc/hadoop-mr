package com.xuzhichao.bigdata.mr.ol.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * MapReduce中会将map输出的kv对，按照相同的key分组，然后分发给不同的reducetask
 * 默认分发规则为：根据key的hashcode%reducetask数 来分
 * 所以如果按照其他的需求来分组，就需要自定义分组组件Partitioner，
 * 需要自定义一个类继承Partitioner，然后设置给job，job.setPartitionerClass(XxxPartitioner);
 * @Date: Create on 2018-2-6 19:47
 * @author xuzhichao
 * @modified By:
 * @version 1.0
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    private static HashMap<String, Integer> provinceMap = new HashMap<String, Integer>(5);

    static {
        provinceMap.put("135", 0);
        provinceMap.put("136", 1);
        provinceMap.put("137", 2);
        provinceMap.put("138", 3);
        provinceMap.put("139", 4);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        Integer provinceCode = provinceMap.get(text.toString().substring(0, 3));
        return provinceCode == null ? 5 : provinceCode;
    }
}
