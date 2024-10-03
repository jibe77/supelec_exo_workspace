package exo_6_9_2_question_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiply {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("n", 2);  // Par exemple, taille de la matrice

        Job job = Job.getInstance(conf, "matrix multiply");
        job.setJarByClass(exo_6_9_2_question_2.MatrixMultiply.class);
        job.setMapperClass(MatrixMultiplyMapper.class);
        job.setReducerClass(MatrixMultiplyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path("src/main/resources/08_input_matrices"));
        FileOutputFormat.setOutputPath(job, new Path("target/exo_6_9_2_question_2"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
