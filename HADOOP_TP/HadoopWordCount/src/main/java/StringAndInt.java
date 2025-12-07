import java.io.Serializable;

/**
 * Simple pair (tag, count) with natural ordering on count (descending),
 * and tag (ascending) to break ties.
 */
public class StringAndInt implements Comparable<StringAndInt>, Serializable {
    private static final long serialVersionUID = 1L;

    private final String tag;
    private final int count;

    public StringAndInt(String tag, int count) {
        this.tag = tag;
        this.count = count;
    }

    public String getTag() {
        return tag;
    }

    public int getCount() {
        return count;
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
    public String toString() {
        return tag + ":" + count;
    }
}
