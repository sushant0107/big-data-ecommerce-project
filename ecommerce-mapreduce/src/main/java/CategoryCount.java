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
import java.io.IOException;

public class CategoryCount {

    // 1. MAPPER: Reads CSV lines, extracts category (index 4), and outputs (Category, 1)
    public static class CategoryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text category = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) return;

            String[] fields = line.split(",");
            if (fields.length >= 10) {
                category.set(fields[4].trim()); // Column 4 is Product_Category
                context.write(category, one);
            }
        }
    }

    // 2. REDUCER: Receives grouped categories and sums the 1s
    public static class CategoryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // 3. DRIVER: Configures and executes the Hadoop Job
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CategoryCount <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count Orders By Category");

        job.setJarByClass(CategoryCount.class);
        job.setMapperClass(CategoryMapper.class);
        job.setCombinerClass(CategoryReducer.class); // Local aggregation optimization
        job.setReducerClass(CategoryReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}