package com.xuzhichao.bigdata.mr.ol.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN、VALUEIN对应mapper输出的KEYOUT、VALUEOUT
 * KEYOUT、VALUEOUT自定义reducer处理逻辑的输出类型
 * Created by xzc on 2018-1-17.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * <apple,1><apple,1><apple,1><apple,1>
     * <banana,1><banana,1>
     * <car,1><car,1><car,1><car,1>
     * 数据会如上所示，一行一行传入到reduce方法，传入方式为values
     * @param key 是一组相同单词kv对的key
     * @param values <apple,1><apple,1><apple,1><apple,1>.....
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for(IntWritable value : values){
            count += value.get();
        }

        // 将汇总结果写到Context
        context.write(key, new IntWritable(count));
    }
}
