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

// 3. Highest fare per class (key - class IntWritable, value - fare FloatWritable)

public class JThree
{
    public static void main(String[] args) throws Exception
    {
        Configuration c = new Configuration();
        Job j = new Job(c, "jtitanic");

        j.setJar("JThree.jar");
        j.setJarByClass(JThree.class);
        j.setMapperClass(MapThree.class);
        j.setReducerClass(ReduceThree.class);
        j.setOutputKeyClass(IntWritable.class); // Output - Keys of type IntWritable
        j.setOutputValueClass(FloatWritable.class); //Output - Values of type FloatWritable

        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1]));

        System.exit(j.waitForCompletion(true) ? 0:1);
    }

    public static class MapThree extends Mapper<LongWritable, Text, IntWritable, FloatWritable>
    {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] columns = line.split(","); // split csv into columns over the comma
            int pclass = Integer.parseInt(columns[2]); // pclass is 2nd index in the dataset  
            float fare = Float.parseFloat(columns[9]); // pclass is 9nd index in the dataset

            con.write(new IntWritable(pclass), new FloatWritable(fare));
        }
    }

    public static class ReduceThree extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>
    {
        public void reduce(IntWritable pclass, Iterable<FloatWritable> fares, Context con) throws IOException, InterruptedException
        {
            try
            {
                float highestFare = fares.iterator().next().get();

                for (FloatWritable fare: fares)
                {
                    if (fare.get() > highestFare)
                    {
                        highestFare = fare.get();
                    }
                }

                con.write(pclass, new FloatWritable(highestFare));
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
}
