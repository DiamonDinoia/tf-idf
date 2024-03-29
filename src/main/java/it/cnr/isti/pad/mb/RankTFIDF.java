/*
 MIT License

 Copyright (c) 2017 Marco Barbone

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
*/
package it.cnr.isti.pad.mb;

import org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RankTFIDF {

    private final static String separator = "@";
    private static final DecimalFormat DF = new DecimalFormat("###.########");
    private static final int numberOfDocumentsInCorpus = 782;
    private static final String tmpDir = "./tmp";

    public static class IdfMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text docFreq = new Text();
        StringBuilder valueBuilder = new StringBuilder();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] wordFileFrequency = value.toString().split("\t");
            String[] wordFile = wordFileFrequency[0].split(separator);
            word.set(wordFile[0]);
            docFreq.set(
                    valueBuilder.append(wordFile[1])
                            .append(separator)
                            .append(wordFileFrequency[1])
                            .toString()
            );
            context.write(word, docFreq);
            valueBuilder.setLength(0);
        }
    }

    public static class IdfReducer extends Reducer<Text, Text, Text, Text> {

        double frequency;
        private Text wordDocument = new Text();
        private Text TF = new Text();
        private Map<String, Double> frequencies = new HashMap<String, Double>();
        private final StringBuilder keyBuilder = new StringBuilder();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text docFreq : values) {
                String[] tmp = docFreq.toString().split(separator);
                frequencies.put(tmp[0], Double.parseDouble(tmp[1]));
            }
            frequency = Math.log10((double) numberOfDocumentsInCorpus / frequencies.size());
            for (String filename : frequencies.keySet()) {
                TF.set(DF.format(frequencies.get(filename) * frequency));
                wordDocument.set(
                        keyBuilder.append(key.toString())
                                .append(separator)
                                .append(filename)
                                .toString()
                );
                context.write(wordDocument, TF);
                keyBuilder.setLength(0);
            }
            frequencies.clear();
        }
    }

    public static class WordFrequencyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Map<String, Integer> words = new HashMap<String, Integer>();
        private final DoubleWritable count = new DoubleWritable();
        private static final Pattern p = Pattern.compile("\\w+");
        private final Text outKey = new Text();
        private final StringBuilder keyBuilder = new StringBuilder();
        private int length = 0;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Matcher m = p.matcher(value.toString());
            while (m.find()) {
                String word = m.group().toLowerCase();
                if (!Character.isLetter(word.charAt(0)) || Character.isDigit(word.charAt(0)) || word.contains("_"))
                    continue;
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
                outKey.set(
                        keyBuilder.append(word)
                                .append(separator)
                                .append(fileName)
                                .toString()
                );
                context.write(outKey, count);
                keyBuilder.setLength(0);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "tf-idf");
        job.setJarByClass(RankTFIDF.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapperClass(WordFrequencyMapper.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(tmpDir));
        job.waitForCompletion(true);
        job = new Job(conf, "final");
        conf.clear();
        job.setJarByClass(RankTFIDF.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(IdfMapper.class);
        job.setReducerClass(IdfReducer.class);
        FileInputFormat.addInputPath(job, new Path(tmpDir));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        FileUtils.deleteDirectory(new File(tmpDir));
    }
}
