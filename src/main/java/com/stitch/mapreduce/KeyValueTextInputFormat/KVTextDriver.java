package com.stitch.mapreduce.KeyValueTextInputFormat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVTextDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/ubuntu/input/KVText/wordcount.txt", "/ubuntu/output/KVText/out"};
        Configuration conf = new Configuration();
        // 设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        // 1 获取 job 对象
        Job job = Job.getInstance(conf);
        // 2 设置 jar 包位置，关联 mapper 和 reducer
        job.setJarByClass(KVTextDriver.class);
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);
        // 3 设置 map 输出 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 4 设置最终输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 5 设置输入输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        // 6 设置输出数据路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 7 提交 job
        job.waitForCompletion(true);
    }
}
