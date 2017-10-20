package it.cnr.isti.pad.mb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {

    private final static String separator = "@";

    public static class WordFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text word = new Text();
        private int length = 0;
        private Map<String,Integer> words  = new HashMap<String, Integer>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                if (words.containsKey(word)) words.put(word,words.get(word)+1);
                else words.put(word,1);
                ++length;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
            IntWritable count = new IntWritable(0);
            Text keyOut = new Text();
            for (String word : words.keySet()) {
                count.set(words.get(word)/length);
                keyOut.set(fileName + separator + word);
                context.write(keyOut, count);
            }
            super.cleanup(context);
        }
    }

    public static class IdfMapper extends Mapper<Object,Text,Text,IntWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fileWordFrequency = value.toString().split("\t");
            String[] fileWord = fileWordFrequency[0].split(separator);

        }
    }


    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "tf-idf");
        job.setJarByClass(Main.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(WordFrequencyMapper.class);
        job.setNumReduceTasks(0);
//        job.setReducerClass(WordCountReducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}