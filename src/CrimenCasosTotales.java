
import java.io.IOException;
import java.util.StringTokenizer;
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
public class CrimenCasosTotales {
    /**
     * ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location 
     * Description, Arrest, Domestic, Beat, District, Ward, Community Area, 
     * FBI Code, X Coordinate, Y Coordinate, Year, Updated On, Latitude, 
     * Longitude, Location
     * 
     * A) El numero total de casos con arresto por Primary Type, y el numero 
     * total de casos sin arresto. Ejemplo: Criminal damage, 1200, 300
     * B) El top N de crimenesporano. El parametroN esdado porel usuarioal 
     * momento de ejecutar el proceso en Hadoop. Ejemplo: 2002, Criminal damage 
     * ¡V4500, Theft ¡V3800, Assault -2596
     */
      
    public static class Map extends Mapper{
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable cero = new IntWritable(0);
        private Text word = new Text();
        
        public void map (LongWritable key,Text value,Reducer.Context context)
                throws IOException, InterruptedException{
            
            String array[] = value.toString().split(",");
            word.set(array[5]);
            //No arrestado
            if(array[8].equals("false")){
                context.write(word, cero);
            }
            else{
                //Primary type
                context.write(word, one);
            }
        }
                
    }
    
    public static class Reduce extends Reducer<Text,IntWritable,Text,Text>{
        public void reduce(Text key,Iterable<IntWritable> values, Reducer.Context context)
        throws IOException,InterruptedException{
            int Arrestados=0;
            int NoArrestados=0;
            for(IntWritable val : values){
                if(val.get()==1)
                    Arrestados++;
                else
                    NoArrestados++;
            }
            String cad = Arrestados + "," + NoArrestados;
            
            context.write(key,new Text(cad));
        }
    }
    
    public static void main (String args[]) throws Exception{
        
        Configuration conf =  new Configuration();
        Job job = new Job(conf,"casostotales");
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
