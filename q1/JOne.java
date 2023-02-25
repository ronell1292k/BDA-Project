import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// 1. Count survived column, where 1 is survived and 0 is not survived

public class JOne
{
    public static void main(String[] args) throws Exception
    {
        Configuration c = new Configuration();
        Job j = new Job(c, "jtitanic");

        j.setJar("JOne.jar");
        j.setJarByClass(JOne.class);
        j.setMapperClass(MapOne.class);
        j.setReducerClass(ReduceOne.class);
        j.setOutputKeyClass(Text.class); // Output - Keys of type Text
        j.setOutputValueClass(IntWritable.class); // Output - Values of type IntWritable

        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1]));

        System.exit(j.waitForCompletion(true) ? 0:1);
    }

    public static class MapOne extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] columns = line.split(","); // split csv into columns over the comma
            int survival = Integer.parseInt(columns[1]); // survial status is the 1st index in the dataset

            String outputKey = "";

            if (survival == 0)
                outputKey = "Not survived";
            else
                outputKey = "Survived";

            con.write(new Text(outputKey), new IntWritable(1));
        }
    }

    public static class ReduceOne extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text survival, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
        {
            int sum = 0; // initial value of zero, survivors and non survivors will be incremented 

            for (IntWritable value : values)
            {
                sum += value.get();
            }

            con.write(survival, new IntWritable(sum));
        }
    }
}
