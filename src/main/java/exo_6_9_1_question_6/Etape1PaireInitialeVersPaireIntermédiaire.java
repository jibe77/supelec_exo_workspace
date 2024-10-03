package exo_6_9_1_question_6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Etape1PaireInitialeVersPaireIntermédiaire {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split(",");
            String noeudSource = nodes[0];  // noeud source
            String nodeDestination = nodes[1];  // noeud destination

            // Emit pour les entrées dans Y
            context.write(new Text(nodeDestination), new Text(noeudSource + " in"));

            // Emit pour les sorties de X
            context.write(new Text(noeudSource), new Text(nodeDestination + " out"));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int nbIn = 0;
            int nbOut = 0;
            Map<String, String> pairsIn = new HashMap<>();
            Map<String, String> pairsOut = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split(" ");
                String X = parts[0];
                String direction = parts[1];

                if (direction.equals("in")) {
                    nbIn++;
                    pairsOut.put(X, key.toString());
                } else if (direction.equals("out")) {
                    nbOut++;
                    pairsIn.put(X, key.toString());
                }
            }

            // Emit results
            for (Map.Entry<String, String> entry : pairsIn.entrySet()) {
                String source = entry.getKey();
                String destination = entry.getValue();
                context.write(new Text("(" + source + " in " + destination + ")"), new Text(nbIn + ", " + nbOut));
            }
            for (Map.Entry<String, String> entry : pairsOut.entrySet()) {
                String source = entry.getKey();
                String destination = entry.getValue();
                context.write(new Text("(" + source + " out " + destination + ")"), new Text(nbIn + ", " + nbOut));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Etape1PaireInitialeVersPaireIntermédiaire");
        job.setJarByClass(Etape1PaireInitialeVersPaireIntermédiaire.class);
        job.setMapperClass(Etape1PaireInitialeVersPaireIntermédiaire.TokenizerMapper.class);
        job.setReducerClass(Etape1PaireInitialeVersPaireIntermédiaire.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("src/main/resources/01_input_associations"));
        FileOutputFormat.setOutputPath(job, new Path("target/06a_output_associations"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
