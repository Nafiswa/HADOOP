import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Simple pair (tag, count) with natural ordering on count (descending),
 * and tag (ascending) to break ties.
 * Implements WritableComparable for Hadoop serialization and sorting.
 */
public class StringAndInt implements WritableComparable<StringAndInt> {

    private String tag;
    private int count;

    public StringAndInt(String tag, int count) {
        this.tag = tag;
        this.count = count;
    }

    // Empty constructor required for Hadoop deserialization
    public StringAndInt() {
    }

    public String getTag() {
        return tag;
    }

    public int getCount() {
        return count;
    }

    public void add(int delta) {
        this.count += delta;
    }

    @Override
    public int compareTo(StringAndInt other) {
        // Higher counts should come first: reverse numeric order
        int cmp = Integer.compare(other.count, this.count);
        if (cmp != 0) return cmp;
        // Break ties lexicographically to keep deterministic order
        if (this.tag == null && other.tag == null) return 0;
        if (this.tag == null) return 1;
        if (other.tag == null) return -1;
        return this.tag.compareTo(other.tag);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(tag != null);
        if (tag != null) {
            out.writeUTF(tag);
        }
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        boolean hasTag = in.readBoolean();
        if (hasTag) {
            this.tag = in.readUTF();
        } else {
            this.tag = null;
        }
        this.count = in.readInt();
    }

    @Override
    public String toString() {
        return tag + ":" + count;
    }
}