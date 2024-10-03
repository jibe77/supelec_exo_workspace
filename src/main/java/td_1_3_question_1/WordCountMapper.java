package td_1_3_question_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private IntWritable outputKey = new IntWritable();
    private IntWritable outputValue = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordsAsArray = getWordsFromText(value.toString());
        for (String word : wordsAsArray) {
            outputKey.set(word.length());
            context.write(outputKey, outputValue);
        }
    }

    private String[] getWordsFromText(String text) {
        // Suppression de la ponctuation et conversion en minuscules
        text = text.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
        // Division du texte en mots par les espaces
        return text.split("\\s+");
    }
}
