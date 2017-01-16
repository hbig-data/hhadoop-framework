package com.ryan.hadoop.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Rayn on 2017/1/16.
 * @email liuwei412552703@163.com.
 */
public class TextPair implements WritableComparable<TextPair> {

    private Text first;
    private Text second;

    public TextPair() {
        this(new Text(), new Text());
    }

    public TextPair(String first, String second) {
        this(new Text(first), new Text(second));
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public TextPair(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(TextPair o) {
        int compareTo = first.compareTo(o.first);
        if(compareTo != 0){
            return compareTo;
        }
        return second.compareTo(o.second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TextPair)) return false;

        TextPair pair = (TextPair) o;

        if (first != null ? !first.equals(pair.first) : pair.first != null) return false;
        return second != null ? second.equals(pair.second) : pair.second == null;

    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TextPair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
