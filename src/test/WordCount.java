/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

/**
 *
 * @author mary
 */
import java.io.IOException;
import java.util.*;
         
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCount {
        
    public static class Map extends Mapper{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map (LongWritable key,Text value,Context context)
                throws IOException, InterruptedException{
            String line = value.toString();
            StringTokenizer tokenizer =  new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
                
    }
    
    public static class Reduce extends Reducer{
        public void reduce(Text key,Iterable values, Context context)
        throws IOException,InterruptedException{
            int sum=0;
            for(Object val : values){
                sum += ((IntWritable)val).get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
    
    public static void main (String args[]) throws Exception{
        Configuration conf =  new Configuration();
        Job job =  new Job(conf,"wordcount");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
    }
}