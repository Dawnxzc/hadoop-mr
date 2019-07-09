package com.xuzhichao.bigdata.mr.ol.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
 * 统计用户的上行流量、下行流量、总流量
 * @Date: Create by xuzhichao on 2018-1-23 21:27
 * @author xuzhichao
 * @modified By:
 * @version 1.0
 */
public class FlowCount {

    /**
     * MapTask
     * @Date: Create by xuzhichao on 2018-1-23 21:33
     * @author xuzhichao
     * @modified By:
     * @version 1.0
     */
    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 将VALUEIN输入的数据转换成文本
            String valuein = value.toString();
            // 将字符串按空格切分
            String[] values = valuein.split("\t");
            // 取出用户手机号
            String phoneNo = values[1];
            // 取出上行流量、下行流量，封装到持久化对象内
            long upFlow = Long.parseLong(values[values.length - 3]);
            long downFlow = Long.parseLong(values[values.length - 2]);
            context.write(new Text(phoneNo), new FlowBean(upFlow, downFlow));
        }
    }

    /**
     * ReduceTask
     * @Date: Create by xuzhichao on 2018-1-23 21:33
     * @author xuzhichao
     * @modified By:
     * @version 1.0
     */
    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

        /**
         * 输入的是同一个号码的对应的不同的FlowBean
         * <1, bean1> <1, bean2> ...
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sumUpFlow = 0;
            long sumDownFlow = 0;
            for(FlowBean flowBean : values){
                sumUpFlow += flowBean.getUpFlow();
                sumDownFlow += flowBean.getDownFlow();
            }
            FlowBean sumFlowBean = new FlowBean(sumUpFlow, sumDownFlow);
            context.write(key, sumFlowBean);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args == null || args.length == 0){
            args = new String[2];
            args[0] = "hdfs://mini1:9000/flowcount/input/flow.log";
            args[1] = "hdfs://mini1:9000/flowcount/output";
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //job指定本程序的jar所在的路径
        job.setJarByClass(FlowCount.class);

        // 设置分组类
        job.setPartitionerClass(ProvincePartitioner.class);

        //jab指定map类/reduce类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指定最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定输入文件所在的目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定输出文件所在的目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
