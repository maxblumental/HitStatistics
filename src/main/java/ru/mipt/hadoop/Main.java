package ru.mipt.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Calendar;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "hit statistics");
        job.setJarByClass(Main.class);

        job.setMapperClass(HitStatisticsMapper.class);
        job.setCombinerClass(HitStatisticsReducer.class);
        job.setReducerClass(HitStatisticsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    @SuppressWarnings("Since15")
    public static class HitStatisticsMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\\s+");

            String hour = split[1];
            long timestampSeconds = Long.parseLong(hour);
            String timestampString = getTimestampString(timestampSeconds);

            String url = split[2];
            word.set(timestampString + "%" + url);
            context.write(word, one);
        }

        private String getTimestampString(long timestampSeconds) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timestampSeconds * 1000);
            int year = calendar.get(Calendar.YEAR);
            int month = calendar.get(Calendar.MONTH);
            int day = calendar.get(Calendar.DAY_OF_MONTH);
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            return String.format("%d-%d-%d %d", year, month, day, hour);
        }
    }

    public static class HitStatisticsReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private TableManager tableManager;

        public HitStatisticsReducer() {
            try {
                tableManager = new TableManager("hits", new String[]{"hour", "domain", "hits"});
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            String[] split = key.toString().split("%");
            String hour = split[0];
            String domain = split[1];
            String hits = Integer.toString(sum);
            tableManager.put(new String[]{hour, domain, hits});
        }

    }

}