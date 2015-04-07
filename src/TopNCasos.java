import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author mary
 */
public class TopNCasos {
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
    public static class Map extends Mapper{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map (LongWritable key,Text value,Reducer.Context context)
                throws IOException, InterruptedException{
            Configuration conf =  context.getConfiguration();
            String top = conf.get("TopN");
            
            String array[] = value.toString().split("/");
            int anio = Integer.parseInt(array[2].substring(0,3));
            
            word.set(array[5]);
            context.write(word, new IntWritable(anio));         
        }
    }
   
    public static class Reduce extends Reducer{
        private LongWritable result = new LongWritable();
        private TreeMap<Integer,Text> topTenRecordMap = new TreeMap<Integer,Text>();
        
        public void reduce(Text key,Iterable<IntWritable> values, Reducer.Context context)
        throws IOException,InterruptedException{
            /*
            int sum=0;
            for(IntWritable val : values){
                sum++;
            }
            result.set(sum);
            
            context.write(key,result);*/
            for(IntWritable val : values){
                //Map<String,String> parsed =
            }
        }
    }
    /*
    public static void main (String args[]) throws Exception{
        
        Configuration conf =  new Configuration();
        conf.set("TopN", args[2]);
        Job job = new Job(conf,"topN");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
    }*/
}
