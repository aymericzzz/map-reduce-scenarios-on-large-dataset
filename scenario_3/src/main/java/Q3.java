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

/**
 * Created by Aymeric on 01/12/2016.
 */
public class Q3 {
    public static class firstMapperVenue extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable venueId = new IntWritable();
        private Text nameYear = new Text();
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Our file contains space-limited values: paperid, authid
            String[] tokens = value.toString().split("\t");
            StringBuilder sb = new StringBuilder();
            // good length and integer id
            if(tokens.length == 7 && tokens[0].matches("^-?\\d+$"))
            {
                // name is char and year is not null
                if(tokens[2].matches("^-?\\d+$"))
                {
                    venueId.set(Integer.parseInt(tokens[0]));
                    sb.append(tokens[1]).append(",").append(tokens[2]);
                    // name,year
                    nameYear.set(sb.toString());
                    context.write(venueId, nameYear);
                }
            }
        }
    }

    public static class firstMapperPaper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable venueId = new IntWritable();
        private Text paperId = new Text();
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Our file contains space-limited values: paperid, authid
            String[] tokens = value.toString().split("\t");
            StringBuilder sb = new StringBuilder();
            // good length and integer id
            if(tokens.length == 7 && tokens[0].matches("^-?\\d+$"))
            {
                // name is char and year is not null
                if(tokens[2].matches("^-?\\d+$"))
                {
                    venueId.set(Integer.parseInt(tokens[0]));
                    sb.append(tokens[1]).append(",").append(tokens[2]);
                    // name,year
                    /*nameYear.set(sb.toString());
                    context.write(venueId, nameYear);*/
                }
            }
        }
    }
}
