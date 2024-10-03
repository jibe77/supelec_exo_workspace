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
import java.util.ArrayList;
import java.util.List;

public class Etape2PaireIntermediaireVersPairesDistance2 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        Text xa = new Text();
        Text outputValue = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] valuesAsArray = parseString(value.toString());
            String direction = valuesAsArray[1];
            xa.set(valuesAsArray[0]);
            String nbIn = valuesAsArray[3];
            String nbOut = valuesAsArray[4];
            if ("out".equals(direction)) {
                outputValue.set("out " + nbOut);
            } else if ("in".equals(direction)) {
                outputValue.set("in " + nbIn);
            }
            context.write(xa, outputValue);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> aggregatedValues, Context context) throws IOException, InterruptedException {
            int nbIn = 0;
            int nbOut = 0;
            for(Text values : aggregatedValues) {
                String[] valuesAsArray = values.toString().split(" ");
                String direction = valuesAsArray[0];
                int counter = Integer.parseInt(valuesAsArray[1]);
                if ("in".equals(direction)){
                    nbIn += counter;
                } else if ("out".equals(direction)){
                    nbOut += counter;
                }
            }
            outputValue.set("(" + nbIn + "," + nbOut + "}");
            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Etape2PaireIntermédiaireVersPairesDistance2");
        job.setJarByClass(Etape2PaireIntermediaireVersPairesDistance2.class);
        job.setMapperClass(Etape2PaireIntermediaireVersPairesDistance2.TokenizerMapper.class);
        job.setReducerClass(Etape2PaireIntermediaireVersPairesDistance2.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("src/main/resources/06_input_associations"));
        FileOutputFormat.setOutputPath(job, new Path("target/06b_output_associations"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static String[] parseString(String input) {
        // Liste pour stocker les valeurs
        List<String> values = new ArrayList<>();

        // Diviser la chaîne d'entrée en utilisant une expression régulière
        String[] tokens = input.split("\\s+|,\\s*");

        // Parcourir les tokens et ignorer ceux qui contiennent des parenthèses
        for (String token : tokens) {
            if (token.startsWith("(")) {
                values.add(token.substring(1));
            } else if(token.endsWith(")")) {
                values.add(token.substring(0, token.length()-1));
            } else {
                values.add(token);
            }
        }

        // Convertir la liste en tableau de chaînes
        return values.toArray(new String[0]);
    }
}
