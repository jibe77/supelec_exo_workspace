package exo_6_9_1_question_3;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPairWritable implements Writable {
    private int first;
    private int second;

    // Constructeurs
    public IntPairWritable() {}

    public IntPairWritable(int first, int second) {
        set(first, second);
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    // Getters
    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    public String toString() {
        return "{" +
                "NbIn=" + first +
                ", NbOut=" + second +
                '}';
    }
}
