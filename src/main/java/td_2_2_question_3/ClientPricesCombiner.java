package td_2_2_question_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ClientPricesCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] keyValues = key.toString().split("/");
        String clientId = keyValues[0];
        Double amount = Double.parseDouble(keyValues[1]);

        int count = 0;
        for (Text text : values) {
            count++;
        }

        // Emission de la paire (ClientID, Price)
        context.write(new Text(clientId), new Text(count + "/" + amount));
    }
}
