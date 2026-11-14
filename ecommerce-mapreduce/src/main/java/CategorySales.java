import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class CategorySales {
    public static class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text category = new Text();
        private DoubleWritable amount = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) return;
            String[] fields = line.split(",");
            if (fields.length >= 10) {
                try {
                    category.set(fields[4].trim()); // Column 4: Category
                    double sales = Double.parseDouble(fields[5].trim()) * Double.parseDouble(fields[6].trim()); // Quantity x Price
                    amount.set(sales);
                    context.write(category, amount);
                } catch (NumberFormatException e) { }
            }
        }
    }

    public static class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(Math.round(sum * 100.0) / 100.0);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Total Sales By Category");
        job.setJarByClass(CategorySales.class);
        job.setMapperClass(SalesMapper.class);
        job.setCombinerClass(SalesReducer.class);
        job.setReducerClass(SalesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}