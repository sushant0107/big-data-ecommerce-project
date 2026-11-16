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

/**
 * Total gross sales revenue per city.
 * Revenue for each order = Quantity (column 5) x Price (column 6),
 * matching the CategorySales job and the Hive cross-check query.
 */
public class CitySales {

    // Counters make skipped rows visible in the job summary instead of silently dropping them
    public enum RowCounter { VALID, MALFORMED }

    // 1. MAPPER: Emits (City, Quantity * Price) for every valid order
    public static class CityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text city = new Text();
        private DoubleWritable amount = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) return;

            String[] fields = line.split(",");
            if (fields.length < 10) {
                context.getCounter(RowCounter.MALFORMED).increment(1);
                return;
            }
            try {
                double quantity = Double.parseDouble(fields[5].trim());
                double price = Double.parseDouble(fields[6].trim());
                city.set(fields[3].trim()); // Column 3: City
                amount.set(quantity * price);
                context.write(city, amount);
                context.getCounter(RowCounter.VALID).increment(1);
            } catch (NumberFormatException e) {
                context.getCounter(RowCounter.MALFORMED).increment(1);
            }
        }
    }

    // 2. COMBINER: Partial sums on the map side (no rounding, so totals stay exact)
    public static class SumCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // 3. REDUCER: Final sum per city, rounded to 2 decimal places
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

    // 4. DRIVER
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CitySales <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Total Sales By City");

        job.setJarByClass(CitySales.class);
        job.setMapperClass(CityMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(SalesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
