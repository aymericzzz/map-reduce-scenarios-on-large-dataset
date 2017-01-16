/**
 * Created by Aymeric on 30/11/2016.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.google.common.collect.Lists;

public class Q2 {
    public static class FirstMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable paperid = new IntWritable();
        private Text authid = new Text();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Our file contains space-limited values: paperid, authid
            String[] tokens = value.toString().split("\\s+");
            if (tokens.length == 2 && tokens[0].matches("\\d+") && tokens[1].matches("\\d+") && Integer.parseInt(tokens[0]) > 0
                    && Integer.parseInt(tokens[1]) > 0) {
                paperid.set(Integer.parseInt(tokens[0]));
                authid.set(tokens[1]);
                context.write(paperid, authid);
            }
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Text valueToEmit = new Text();
            StringBuilder sb = new StringBuilder();
            for (Text x : values) {
                sb.append(x.toString()).append(",");
            }
            valueToEmit.set(sb.substring(0, sb.length() - 1));
            context.write(key, valueToEmit);
        }
    }

    public static class SecondMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Our file contains space-limited values: int paperid, text list of authors
            String[] tokens = value.toString().split("\t");
            String[] authors;
            Text valueToEmit = new Text();
            if (tokens.length == 2) {
                authors = tokens[1].split(",");
                if (authors.length > 1) {
                    for (String s : authors) {
                        //authid.set(Integer.parseInt(s));
                        for (String s2 : authors) {
                            if (Integer.parseInt(s) != Integer.parseInt(s2)) {
                                /*authid2.set(Integer.parseInt(s2));
                                context.write(authid, authid2);*/
                                StringBuilder sb = new StringBuilder();
                                sb.append(s).append(",").append(s2);
                                valueToEmit.set(sb.toString());
                                context.write(valueToEmit, one);
                            }
                        }
                    }
                }
            }
        }
    }

    public static class SecondReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class thirdMapper extends Mapper<Object, Text, Text, Text> {
        private Text authid = new Text();
        private Text result = new Text();
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if(line.length == 2){
                String auth = line[0].split(",")[0];
                String caid = line[0].split(",")[1];
                //int num_papers = Integer.parseInt(line[1]);
                authid.set(auth.trim());
                StringBuilder sb = new StringBuilder();
                //sb.append("<").append(caid.trim()).append(",").append(line[1]).append(">");
                sb.append(caid.trim()).append(",").append(line[1]);
                result.set(sb.toString());
                context.write(authid, result);
            }
        }
    }

    public static class thirdReducer extends Reducer<Text, Text, IntWritable, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int K = Integer.parseInt(conf.get("K"));
            int cnt = 0;
            StringBuilder sb = new StringBuilder();
            TreeSet<Integer> uniqueNbPp = new TreeSet<>();
            TreeSet<Integer> uniqueNbPp_rev = new TreeSet<>();
            List<String> caPairs = new ArrayList<>();
            // we retrieve (authid, (ca, nb_paper)), we have to sort by nb_paper in decreasing order

            for (Text val : values) {
                String pair = val.toString();
                // add the ca, nbpp in the list
                caPairs.add(pair);
                // add the nbpp in a set then sort the set in dec order, then loop over the caPairs list to retrieve the K top pairs
                int nbPp = Integer.parseInt(pair.split(",")[1]);
                uniqueNbPp.add(nbPp);
            }
            uniqueNbPp_rev = (TreeSet)uniqueNbPp.descendingSet();
            // for each unique number of papers
            for(int i: uniqueNbPp_rev){
                // if the count of top is lesser than K
                if(cnt < K){
                    for(String p: caPairs){
                        String ca = p.split(",")[0].trim();
                        String pp = p.split(",")[1].trim();
                        if(Integer.parseInt(pp) == i){
                            sb.append("<").append(ca).append(",").append(pp).append(">");
                        }
                    }
                    cnt++;
                }
            }

            result.set(sb.toString());
            context.write(new IntWritable(Integer.parseInt(key.toString())), result);
        }
    }


    public static class ChainJobs extends Configured implements Tool {

        private static final String OUTPUT_PATH = "int_op_q2";
        private static final String OUTPUT_PATH2 = "int_op_q2_2";
        private static final String OUTPUT_PATH3 = "int_op_q2_3";

        @Override
        public int run(String[] args) throws Exception {
            /**
             * JOB 1
             */
            Configuration conf = new Configuration();
            conf.set("K", args[3]);
            Job job = Job.getInstance(conf, "job1");
            FileSystem fs = FileSystem.get(conf);
            job.setJarByClass(Q2.class);
            job.setMapperClass(FirstMapper.class);
            job.setReducerClass(IntSumReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            TextInputFormat.addInputPath(job, new Path(args[0]));
            TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
            job.waitForCompletion(true);

            /**
             * JOB 2
             */
            Job job2 = new Job(conf, "Job 2");
            job2.setJarByClass(Q2.class);

            job2.setMapperClass(SecondMapper.class);
            job2.setReducerClass(SecondReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
            TextOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH2));
            job2.waitForCompletion(true);

            /**
             * JOB 3
             */
            Job job3 = new Job(conf, "Job 3");
            job3.setJarByClass(Q2.class);

            job3.setInputFormatClass(TextInputFormat.class);
            job3.setMapperClass(thirdMapper.class);
            job3.setReducerClass(thirdReducer.class);
            job3.setOutputFormatClass(SequenceFileOutputFormat.class);

            TextInputFormat.addInputPath(job3, new Path(OUTPUT_PATH2));
            TextOutputFormat.setOutputPath(job3, new Path(OUTPUT_PATH3));
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(IntWritable.class);
            job3.setOutputValueClass(Text.class);

            job3.waitForCompletion(true);

            Job job4 = new Job(conf, "Job 4");
            job4.setJarByClass(Q2.class);

            job4.setNumReduceTasks(3);
            FileInputFormat.setInputPaths(job4, new Path(OUTPUT_PATH3));
            TotalOrderPartitioner.setPartitionFile(job4.getConfiguration(), new Path(args[1]));
            job4.setInputFormatClass(SequenceFileInputFormat.class);
            job4.setMapOutputKeyClass(IntWritable.class);

            InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
            InputSampler.writePartitionFile(job4, sampler);

            job4.setPartitionerClass(TotalOrderPartitioner.class);
            job4.setMapperClass(Mapper.class);
            job4.setReducerClass(Reducer.class);
            FileOutputFormat.setOutputPath(job4, new Path(args[2]));

            return job4.waitForCompletion(true) ? 0 : 1;
        }
    }

        public static void main(String[] args) throws Exception {
            int res = ToolRunner.run(new Configuration(), new ChainJobs(), args);
            System.exit(res);
        }
}
