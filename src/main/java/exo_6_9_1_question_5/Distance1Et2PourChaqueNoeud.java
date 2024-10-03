package exo_6_9_1_question_5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Distance1Et2PourChaqueNoeud {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text source = new Text();
        private Text distance1d = new Text("1d");
        private Text distance2d = new Text("1d");

        @Override
        protected void map(Object offset, Text line, Context context) throws IOException, InterruptedException {
            String[] lineAsArray = extractFourValues(line.toString());
            source.set(lineAsArray[0]);
            distance2d.set(lineAsArray[3]);
            context.write(source, distance1d);
            context.write(source, distance2d);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int nb1d = 0;
            int nb2d = 0;
            for(Text valeur : values) {
                if ("1d".equals(valeur.toString())) {
                    nb1d++;
                } else {
                    nb2d += Integer.parseInt(valeur.toString());
                }
            }
            result.set(nb1d + "," + nb2d);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Distance1Et2PourChaqueNoeud");
        job.setJarByClass(Distance1Et2PourChaqueNoeud.class);
        job.setMapperClass(Distance1Et2PourChaqueNoeud.TokenizerMapper.class);
        job.setReducerClass(Distance1Et2PourChaqueNoeud.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("src/main/resources/05_input_associations"));
        FileOutputFormat.setOutputPath(job, new Path("target/05_output_associations"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static String[] extractFourValues(String input) {
        // Utilisation d'une expression régulière pour extraire les nombres
        String[] values = input.split("\\D+"); // Séparer par tout ce qui n'est pas un chiffre

        // Filtrer les chaînes vides résultant de split et convertir en entiers
        String[] result = new String[4];
        int index = 0;

        for (String value : values) {
            if (!value.isEmpty() && index < 4) {
                result[index++] = value;
            }
        }

        return result;
    }
}
