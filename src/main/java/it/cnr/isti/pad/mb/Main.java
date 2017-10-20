package it.cnr.isti.pad.mb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Main {

    private final static String separator = "@";
    private static final DecimalFormat DF = new DecimalFormat("###.########");
    private static final int numberOfDocumentsInCorpus = 782;

    private static final String tmpDir = "/tmp";

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

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "tf-idf");
        job.setJarByClass(Main.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapperClass(WordFrequencyMapper.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(tmpDir));
        job.waitForCompletion(true);
        conf.clear();
        job.setJarByClass(Main.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(IdfMapper.class);
        job.setReducerClass(IdfReducer.class);
        FileInputFormat.addInputPath(job, new Path(tmpDir));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
//        FileUtils.deleteDirectory(new File(tmpDir));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class WordFrequencyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private int length = 0;
        private Map<String, Integer> words = new HashMap<String, Integer>();
        private static final Pattern p = Pattern.compile("\\w+");
        DoubleWritable count = new DoubleWritable();
        Text keyOut = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Matcher m = p.matcher(value.toString());
            while (m.find()) {
                String word = m.group().toLowerCase();
                if (words.containsKey(word)) words.put(word, words.get(word) + 1);
                else words.put(word, 1);
                ++length;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            for (String word : words.keySet()) {
                count.set((double) words.get(word) / length);
                keyOut.set(fileName + separator + word);
                context.write(keyOut, count);
            }
            super.cleanup(context);
        }
    }

    public static class IdfReducer extends Reducer<Text, Text, Text, Text> {

        double frequency;
        private Map<String, Double> frequencies = new HashMap<String, Double>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] tmp = null;
            for (Text docFreq : values) {
                //tmp[0] file tmp[1] frequency
                tmp = docFreq.toString().split(separator);
                frequencies.put(tmp[0], Double.parseDouble(tmp[1]));
            }
            frequency = Math.log10((double) numberOfDocumentsInCorpus / frequencies.size());
            Text wordDocument = new Text();
            Text TF = new Text();
            for (String word : frequencies.keySet()) {
                TF.set(DF.format(frequencies.get(word) * frequency));
                wordDocument.set(word + separator + tmp[0]);
                context.write(wordDocument, TF);
            }
        }
    }

}