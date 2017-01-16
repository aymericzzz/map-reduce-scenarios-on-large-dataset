/**
 * Created by Aymeric on 02/12/2016.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Query3 {
    public static class compositeKeyComparator extends WritableComparator {

        protected compositeKeyComparator() {
            super(compositeKey.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {

            compositeKey key1 = (compositeKey) w1;
            compositeKey key2 = (compositeKey) w2;

            // order by venue name
            return key1.compareTo(key2);
        }
    }

    public static class actualKeyPartitioner extends Partitioner<compositeKey, LongWritable> {

        private HashPartitioner<Text, LongWritable> hashPartitioner = new HashPartitioner<Text, LongWritable>();
        private Text newKey = new Text();

        @Override
        public int getPartition(compositeKey key, LongWritable value, int numReduceTasks) {

            try {
                // Execute the default partitioner over the first part of the key
                newKey.set(key.getVenue());
                return hashPartitioner.getPartition(newKey, value, numReduceTasks);

            } catch (Exception e) {
                e.printStackTrace();
                return (int) (Math.random() * numReduceTasks); // this would return a random value in the range [0,numReduceTasks)
            }
        }
    }

    public static class actualKeyGroupingComparator extends WritableComparator {

        protected actualKeyGroupingComparator() {
            super(compositeKey.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {

            compositeKey key1 = (compositeKey) w1;
            compositeKey key2 = (compositeKey) w2;

            // order by venue name
            return key1.getVenue().compareTo(key2.getVenue());
        }
    }

    public static class compositeKey implements WritableComparable<compositeKey> {

        private String venuename;
        private int year;

        public compositeKey(String venuename, int year) {
            this.venuename = venuename;
            this.year = year;
        }

        public compositeKey() {
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append(venuename).append(',').append(year).toString();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeString(out, venuename);
            WritableUtils.writeVInt(out, year);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            venuename = WritableUtils.readString(in);
            year = WritableUtils.readVInt(in);
        }

        public String getVenue() {
            return venuename;
        }

        public void setYear(int year) {
            this.year = year;
        }

        public int getYear() {
            return year;
        }

        public void setVenue(String venuename) {
            this.venuename = venuename;
        }

        @Override
        public int compareTo(compositeKey o) {
            int first = venuename.compareTo(o.venuename);

            if (first != 0)
                return first;
            else if (year < o.year)
                return -1;
            else if (year > o.year)
                return 1;
            return 0;
        }
    }

    public static class Q3Mapper1_venue extends Mapper<Object, Text, LongWritable, Text>{
    /* Mapper 3: emit for each author, a string containing the coauthor and the number of papers*/

        private LongWritable venueid = new LongWritable(1);

        protected void map(Object key, Text line, Context context) throws IOException, InterruptedException{
            // id, name, year, school, number, type
            String[] split = line.toString().split("\t(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            StringBuilder output = new StringBuilder();
            context.write(venueid, line);

            if (split.length == 7 && split[2].trim().matches("^-?\\d+$")) {
                venueid.set(Long.parseLong(split[2].trim())); // venue_id
                //output.append(venueid);
                //output.append("\t");
                output.append(split[1].trim());  // name
                output.append("\t");
                output.append(split[2].trim());  // year
                context.write(venueid, new Text(output.toString()));
            }
        }
    }

    public static class Q3Mapper1_papers extends Mapper<Object, Text, LongWritable, Text>{
    /* Mapper 3: emit for each author, a string containing the coauthor and the number of papers*/

        private LongWritable venueid = new LongWritable(1);

        protected void map(Object key, Text line, Context context) throws IOException, InterruptedException{
            // id, name, venue, pages, url
            String[] split = line.toString().split("\t");
            StringBuilder output = new StringBuilder();

            if (split.length == 5 && split[2].trim().matches("^-?\\d+$")) {
                venueid.set(Long.parseLong(split[2].trim())); // venue_id
                //output.append(venueid);
                //output.append("\t");
                output.append(split[0].trim()); // paperid
                context.write(venueid, new Text(output.toString()));
            }
        }
    }


    public static class Q3Reducer1 extends Reducer<LongWritable, Text, Text, Text> {
    /* Reducer 3: find top K in each coauth_list -> sorted list! */

        public void reduce(LongWritable venueid, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
            String output = null;
            Text out = new Text();
            for (Text line : lines) {
                if (line.toString().contains(",")) {
                    output = line.toString();  // venue_name, year
                }
            }

            for (Text l : lines) {
                if (!l.toString().contains(",")) {
                    if(output != null)
                        context.write(new Text(output), l);
                }
            }
        }
    }


    public static class Q3Mapper2 extends Mapper<LongWritable, Text, compositeKey, LongWritable>{
    /* Mapper 2 */

        private compositeKey pair = new compositeKey();
        private LongWritable paperid = new LongWritable();

        protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
            // venue_name, year, paperid
            String[] split = line.toString().split("\t");

            // put venuename and year as keys
            pair.setVenue(split[0]);
            pair.setYear(Integer.parseInt(split[1]));

            paperid.set(Long.parseLong(split[2]));

            // output key-value pairs
            context.write(pair, paperid);
        }
    }

    public static class Q3Reducer2 extends Reducer<compositeKey, LongWritable, Text, LongWritable> {
    /* Reducer 3: find top K in each coauth_list -> sorted list! */

        int count = 0;
        Text out = new Text();

        public void reduce(compositeKey pair, Iterable<LongWritable> paper_list, Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));

            if (count < k){
                StringBuilder key = new StringBuilder();
                key.append("<");
                key.append(pair.getVenue());
                key.append(",");
                key.append(pair.getYear());
                key.append(">");

                out.set(key.toString());

                for (LongWritable paperid : paper_list) {

                    context.write(out, paperid);
                    count++;

                    if (count >= k)
                        break;
                }
            }
        }
    }
    public static class Q3 extends Configured implements Tool {

        public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            conf.set("k", args[4]);

            // FIRST JOB - JOIN TABLES
            Job job1 = new Job(conf);
            job1.setJarByClass(getClass());
            job1.setJobName(getClass().getSimpleName());

            // setting job 3 mapper, combiner and reducer classes
            MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Q3Mapper1_venue.class);
            MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Q3Mapper1_papers.class);

            job1.setReducerClass(Q3Reducer1.class);
            FileOutputFormat.setOutputPath(job1, new Path(args[2]));

            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            job1.waitForCompletion(true);

            // SECOND JOB - FINDING TOP K
            Job job2 = new Job(conf);
            job2.setJarByClass(getClass());
            job2.setJobName(getClass().getSimpleName());

            // setting job 3 mapper, combiner and reducer classes
            job2.setMapperClass(Q3Mapper2.class);
            job2.setReducerClass(Q3Reducer2.class);
            job2.setNumReduceTasks(1);

            // setting output classes for job 3
            job2.setOutputKeyClass(compositeKey.class);
            job2.setOutputValueClass(LongWritable.class);

            // setting input and output paths for job 2
            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            // sorting mapper keys and value list for reducer
            job2.setPartitionerClass(actualKeyPartitioner.class);
            job2.setGroupingComparatorClass(actualKeyGroupingComparator.class);
            job2.setSortComparatorClass(compositeKeyComparator.class);

            return job2.waitForCompletion(true) ? 0 : 1;
        }
    }
        public static void main(String[] args) throws Exception {
            int rc = ToolRunner.run(new Q3(), args);
            System.exit(rc);
        }

}
