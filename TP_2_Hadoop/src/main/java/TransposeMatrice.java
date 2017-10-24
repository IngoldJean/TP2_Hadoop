import java.io.Console;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransposeMatrice {

    public static class TransposeMapper
            extends Mapper<LongWritable, Text, IntWritable, Text>{


        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] decoupage = value.toString().split(",");
            Integer i;
            int column = 0;
            long somethingLikeRow = key.get();
            for (i=0; i < decoupage.length; i++) {
                context.write(new IntWritable(column), new Text(somethingLikeRow + "\t" + decoupage[i]));
                ++column;
            }
        }

    }

    public static class TransposeReducer
            extends Reducer<IntWritable,Text,Text,NullWritable> {

        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            TreeMap<Long, String> row = new TreeMap<Long, String>(); // storing values sorted by positions in input file
            for (Text text : values) {
                String[] decoupage = text.toString().split("\t"); // somethingLikeRow, value
                row.put(Long.valueOf(decoupage[0]), decoupage[1]);
            }

            String rowString = StringUtils.join(row.values(), ','); // i'm using org.apache.commons library for concatenation
            context.write(new Text(rowString), NullWritable.get());
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "transposee");
        job.setJarByClass(TransposeMatrice.class);
        job.setMapperClass(TransposeMapper.class);
        //job.setCombinerClass(TransposeReducer.class);
        job.setReducerClass(TransposeReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
