package exo_6_9_1_question_3;

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

public class CheminEntreeEtSortiePourChaqueNoeud {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable IN = new IntWritable(1);
        private final static IntWritable OUT = new IntWritable(2);
        private Text source = new Text();
        private Text destination = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] association = value.toString().split(",");
            source.set(association[0]);
            context.write(source, OUT);
            destination.set(association[1]);
            context.write(destination, IN);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text, IntPairWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sumIn = 0;
            int sumOut = 0;
            for (IntWritable val : values) {
                if (val.get() == 1){
                    sumIn++;
                } else if (val.get() == 2) {
                    sumOut++;
                }
            }
            context.write(key, new IntPairWritable(sumIn, sumOut));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CheminSortiePourChaqueNoeud");
        job.setJarByClass(CheminEntreeEtSortiePourChaqueNoeud.class);
        job.setMapperClass(CheminEntreeEtSortiePourChaqueNoeud.TokenizerMapper.class);
        job.setReducerClass(CheminEntreeEtSortiePourChaqueNoeud.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("src/main/resources/01_input_associations"));
        FileOutputFormat.setOutputPath(job, new Path("target/03_output_associations"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
