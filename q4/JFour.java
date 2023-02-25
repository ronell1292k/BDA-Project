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

// 4. Count embarked: Southampton, Cherbourg, and Queenstown

public class JFour
{
    public static void main(String[] args) throws Exception
    {
        Configuration c = new Configuration();
        Job j = new Job(c, "JFour");

        j.setJar("JFour.jar");
        j.setJarByClass(JFour.class);
        j.setMapperClass(MapFour.class);
        j.setReducerClass(ReduceFour.class); 
        j.setOutputKeyClass(Text.class); // Output of the application (Keys of type Text)
        j.setOutputValueClass(IntWritable.class); // Values of type IntWritable

        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1]));

        System.exit(j.waitForCompletion(true) ? 0:1);
    }

    public static class MapFour extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] columns = line.split(","); // split csv into columns over the comma
            String embarkedCity = columns[columns.length-1]; // embarked city is the last index in the dataset

            String outputKey = "";

            if (embarkedCity.equals("S"))
                outputKey = "Southampton";
            else if (embarkedCity.equals("C"))
                outputKey = "Cherboug";
            else if (embarkedCity.equals("Q"))
                outputKey = "Queenstown";
            else
                outputKey = "Not Available";

            con.write(new Text(outputKey), new IntWritable(1));
        }
    }

    public static class ReduceFour extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text embarkedCity, Iterable<IntWritable> count, Context con) throws IOException, InterruptedException
        {
            int sum = 0;

            for (IntWritable value : count)
            {
                sum += value.get();
            }

            con.write(embarkedCity, new IntWritable(sum));
        }
    }
}
