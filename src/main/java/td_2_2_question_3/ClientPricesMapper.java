package td_2_2_question_3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ClientPricesMapper extends Mapper<Object, Text, Text, Text> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void map(Object key, Text record, Context context) throws IOException, InterruptedException {

        // Parser chaque ligne JSON
        JsonNode jsonNode = objectMapper.readTree(record.toString());

        // Extraire les champs ClientID et Price
        String clientID = jsonNode.get("ClientID").asText();
        double price = jsonNode.get("Price").asDouble();

        // Emission de la paire (ClientID, Price)
        context.write(new Text(clientID + "/" + price), new Text("1"));
    }
}
