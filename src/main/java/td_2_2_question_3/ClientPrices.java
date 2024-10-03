package td_2_2_question_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClientPrices {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "client prices");
        job.setJarByClass(ClientPrices.class);

        job.setMapperClass(ClientPricesMapper.class);
        job.setCombinerClass(ClientPricesCombiner.class);
        job.setReducerClass(ClientPricesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("src/main/resources/td2_2_input_document"));
        FileOutputFormat.setOutputPath(job, new Path("target/td2_2_3_output_document"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
