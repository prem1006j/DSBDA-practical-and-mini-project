package Assignment_13;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class WeatherAnalysis {
 // Mapper Class
 public static class WeatherMapper
 extends Mapper<Object, Text, Text, DoubleWritable> {
 private Text keyOut = new Text();
 private DoubleWritable valueOut = new DoubleWritable();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 String line = value.toString();
 String[] parts = line.split(" ");
 // Format: Date Temp Dew Wind
 if (parts.length == 4) {
 double temp = Double.parseDouble(parts[1]);
 double dew = Double.parseDouble(parts[2]);
 double wind = Double.parseDouble(parts[3]);
 keyOut.set("Temperature");
 valueOut.set(temp);
 context.write(keyOut, valueOut);
 keyOut.set("DewPoint");
 valueOut.set(dew);
 context.write(keyOut, valueOut);
 keyOut.set("WindSpeed");
 valueOut.set(wind);
 context.write(keyOut, valueOut);
 }
 }
 }
 // Reducer Class
 public static class WeatherReducer
 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
 public void reduce(Text key, Iterable<DoubleWritable> values,
 Context context)
 throws IOException, InterruptedException {
 double sum = 0;
 int count = 0;
 for (DoubleWritable val : values) {
 sum += val.get();
 count++;
 }
 double avg = sum / count;
 context.write(key, new DoubleWritable(avg));
 }
 }
 // Driver Class
 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = Job.getInstance(conf, "Weather Analysis");
 job.setJarByClass(WeatherAnalysis.class);
 job.setMapperClass(WeatherMapper.class);
 job.setReducerClass(WeatherReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(DoubleWritable.class);
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}