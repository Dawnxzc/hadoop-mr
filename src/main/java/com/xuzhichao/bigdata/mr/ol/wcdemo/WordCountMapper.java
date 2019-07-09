package com.xuzhichao.bigdata.mr.ol.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper 有4个泛型：
 * KEYIN：默认情况下，是mr框架所读到的一行文本的起始偏移量，
 *        默认类型应该为Long，但是Long的序列化规则是默认的序列化规则，有太多的冗余，
 *        所以在这里使用Hadoop自己实现的更精简的序列化接口的类 ： LongWritable
 * VALUEIN：默认情况下是mr读取到的第一行文本内容，同上，原String，现：Text
 * KEYOUT：是用户自己定义的map逻辑处理完之后输出的key，在这里指的是一个单词，Text
 * VALUEOUT：是用户自己定义的map逻辑处理完之后输出的value，在这里指的是单词的数量，原Integer，现：IntWritable
 *
 * Created by xzc on 2018-1-17.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * map阶段的业务逻辑就写在自定义的map() 方法中，
     * maptask会对每一行输入的数据调用一次自定义的map方法
     * @param key
     * @param value maptask传给我们的文本内容
     * @param context 上下文对象，用来写map完成后的输出信息
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 将value转化为String
        String valueString = value.toString();
        // 以空格将文本切成单词数组
        String[] words = valueString.split(" ");

        // 将统计到的单词输出 <word, 1>
        for(String word : words){
            // 将单词作为key，次数1作为value，以便于后续数据的分发，可根据单词分发，以便于相同的单词会到相同的reduce task
            context.write(new Text(word), new IntWritable(1));
        }

    }
}
