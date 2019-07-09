package com.xuzhichao.bigdata.mr.ol.flowcount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义bean
 * @Date: Create by xuzhichao on 2018-1-23 21:02
 * @author xuzhichao
 * @modified By:
 * @version 1.0
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private Long upFlow;

    private Long downFlow;

    private Long sumFlow;

    public FlowBean(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public FlowBean(){

    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /**
     * 序列化方法
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化方法
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "upFlow:" + upFlow + "\t downFlow:" + downFlow + "\t sumFlow:" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        return sumFlow > o.getSumFlow() ? 1 : -1;
    }
}
