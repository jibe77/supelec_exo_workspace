package exo_6_9_2_question_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MatrixMultiplyReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        int n = context.getConfiguration().getInt("n", 0);
        Integer[] results = new Integer[n];
        Integer[] valuesToCompute = new Integer[n];

        // Sum all the partial products
        for (Text val : values) {
            String[] valuesAsArray = val.toString().split(" ");
            Integer position = Integer.parseInt(valuesAsArray[0]);
            Integer value = Integer.parseInt(valuesAsArray[1]);
            if (valuesToCompute[position] == null) {
                valuesToCompute[position] = value;
            } else {
                results[position] = value * valuesToCompute[position];
            }
        }

        for (Integer result : results) {
            sum += result;
        }

        result.set(sum + "");
        context.write(key, result);
    }
}
