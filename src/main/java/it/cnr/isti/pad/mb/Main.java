package it.cnr.isti.pad.mb;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
    private static final DecimalFormat DF = new DecimalFormat("###.########");
    private static final int numberOfDocumentsInCorpus = 782;

    public static class WordFrequencyMapper extends Mapper<Object, Text, Text, FloatWritable> {
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
            FloatWritable count = new FloatWritable(0.f);
            Text keyOut = new Text();
            for (String word : words.keySet()) {
                count.set((float)words.get(word)/(float)length);
                keyOut.set(fileName + separator + word);
                context.write(keyOut, count);
            }
            super.cleanup(context);
        }
    }

    public static class IdfMapper extends Mapper<Object,Text,Text,Text>{
        private Text word = new Text();
        private Text docFreq = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fileWordFrequency = value.toString().split("\t");
            String[] fileWord = fileWordFrequency[0].split(separator);
            word.set(fileWord[1]);
            docFreq.set(fileWord[0] + separator + fileWordFrequency[2]);
            context.write(word,docFreq);
        }
    }


    public static class IdfMapperReducer  extends Reducer<Text, Text, Text, FloatWritable> {

        private FloatWritable result = new FloatWritable();
        private Map<String,FloatWritable> frequencies = new HashMap<String, FloatWritable>();
        float frequency;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text docFreq : values) {
                //tmp[0] file tmp[1] frequency
                String[] tmp = docFreq.toString().split(separator);
                frequencies.put(tmp[0],new FloatWritable(Float.parseFloat(tmp[1])));
            }
            frequency = (float) numberOfDocumentsInCorpus/ (float) frequencies.size();
            for (String documents : frequencies.keySet()) {
                
            }
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