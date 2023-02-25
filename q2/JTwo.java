import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Arrays;

// 2. Avg age gender wise (key - gender Text, value - age IntWritable)

public class JTwo
{
    public static void main(String[] args) throws Exception
    {
        Configuration c = new Configuration();
        Job j = new Job(c, "jtitanic");

        j.setJar("JTwo.jar");
        j.setJarByClass(JTwo.class);
        j.setMapperClass(MapTwo.class);
        j.setReducerClass(ReduceTwo.class); 
        j.setOutputKeyClass(Text.class); // Output - Keys of type Text
        j.setOutputValueClass(FloatWritable.class); // Output - Values of type IntWritable

        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1]));

        System.exit(j.waitForCompletion(true) ? 0:1);
    }

    public static class MapTwo extends Mapper<LongWritable, Text, Text, FloatWritable>
    {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            
            String line = value.toString();
            String[] columns = line.split(","); // split csv into columns over the comma
            String gender = columns[4];  // Gender is 4th index in the dataset
            float age = Float.parseFloat(columns[5]);

            con.write(new Text(gender), new FloatWritable(age));
        }
    }

    public static class ReduceTwo extends Reducer<Text, FloatWritable, Text, FloatWritable>
    {
        public void reduce(Text gender, Iterable<FloatWritable> ages, Context con) throws IOException, InterruptedException
        {
            try
            {
                float totalAge = 0;
                int number = 0;

                for (FloatWritable values: ages)
                {
                    totalAge += values.get();
                    number++;
                }

                float average = totalAge / number;

                con.write(gender, new FloatWritable(average));
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
}
