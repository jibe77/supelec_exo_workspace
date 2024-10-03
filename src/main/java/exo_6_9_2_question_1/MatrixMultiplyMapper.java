package exo_6_9_2_question_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixMultiplyMapper extends Mapper<Object, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Each line is of the form: MatrixName i j value
        String[] parts = value.toString().split(",");
        String matrixName = parts[0];
        int i = Integer.parseInt(parts[1]);
        int j = Integer.parseInt(parts[2]);
        int val = Integer.parseInt(parts[3]);

        // Emit key-value pairs based on the matrix name
        if (matrixName.equals("A")) {
            // Emit all pairs (i, k) where k is a possible column in B
            for (int k = 0; k < context.getConfiguration().getInt("n", 0); k++) {
                outputKey.set(i + "," + k);
                outputValue.set(j + " "+ val);
                context.write(outputKey, outputValue);
            }
        } else if (matrixName.equals("B")) {
            // Emit all pairs (i, k) where i is a possible row in A
            for (int k = 0; k < context.getConfiguration().getInt("n", 0); k++) {
                outputKey.set(k + "," + j);
                outputValue.set(i + " " + val);
                context.write(outputKey, outputValue);
            }
        }
    }
}
