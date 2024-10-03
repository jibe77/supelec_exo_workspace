package td_2_2_question_1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ClientPricesMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void map(Object key, Text record, Context context) throws IOException, InterruptedException {

        // Parser chaque ligne JSON
        JsonNode jsonNode = objectMapper.readTree(record.toString());

        // Extraire les champs ClientID et Price
        String clientID = jsonNode.get("ClientID").asText();
        double price = jsonNode.get("Price").asDouble();

        // Emission de la paire (ClientID, Price)
        context.write(new Text(clientID), new DoubleWritable(price));
    }
}
