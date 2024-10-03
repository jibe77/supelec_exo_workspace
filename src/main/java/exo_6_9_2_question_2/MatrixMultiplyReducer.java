package exo_6_9_2_question_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MatrixMultiplyReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int n = context.getConfiguration().getInt("n", 0);
        Integer[][] results = new Integer[n][n];
        Integer[][] valuesToCompute = new Integer[n][n];

        // Sum all the partial products
        for (Text val : values) {
            String[] valuesAsArray = val.toString().split(" ");
            Integer row = Integer.parseInt(valuesAsArray[0]);
            Integer position = Integer.parseInt(valuesAsArray[1]);
            Integer value = Integer.parseInt(valuesAsArray[2]);
            if (valuesToCompute[row][position] == null) {
                valuesToCompute[row][position] = value;
            } else {
                results[row][position] = value * valuesToCompute[row][position];
            }
        }
        int matrixResult = 0;
        for (Integer[] rows : results) {
            for (Integer result : rows) {
                matrixResult += result;
            }
        }
        result.set(String.valueOf(matrixResult));
        context.write(key, result);
    }
}
