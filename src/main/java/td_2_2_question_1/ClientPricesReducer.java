package td_2_2_question_1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClientPricesReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> prices, Context context) throws IOException, InterruptedException {
        List<Double> priceList = new ArrayList<>();
        double somme = 0d;
        // Transformation de l'Iterable en liste
        for (DoubleWritable price : prices) {
            priceList.add(price.get());
            somme += price.get();
        }

        // Calcul de la médiane
        Collections.sort(priceList);
        double mediane = calculerMediane(priceList);

        // Calcul de l'écart type
        double ecartType = calculerEcartType(priceList);

        // Emission du résultat pour ce client
        context.write(key, new Text("Somme:" + somme + ", Médiane: " + mediane + ", Ecart Type: " + ecartType));
    }

    private double calculerMediane(List<Double> priceList) {
        int taille = priceList.size();
        if (taille % 2 == 0) {
            return (priceList.get(taille / 2 - 1) + priceList.get(taille / 2)) / 2.0;
        } else {
            return priceList.get(taille / 2);
        }
    }

    private double calculerEcartType(List<Double> priceList) {
        double moyenne = priceList.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double variance = priceList.stream().mapToDouble(p -> Math.pow(p - moyenne, 2)).average().orElse(0.0);
        return Math.sqrt(variance);
    }
}
