package com.xuzhichao.bigdata.mr.ol.wcdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于一个yarn集群的客户端
 * 需要在此处指定我们mr程序运行时相关的参数，指定jar包
 * 最后提交给yarn
 * Created by xzc on 2018-1-17.
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args == null || args.length == 0){
            args = new String[2];
            args[0] = "hdfs://mini1:9000/wordcount/input/wordcount.txt";
            args[1] = "hdfs://mini1:9000/wordcount/output9";
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //job指定本程序的jar所在的路径
        job.setJarByClass(WordCountDriver.class);

        //jab指定map类/reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定输入文件所在的目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定输出文件所在的目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
