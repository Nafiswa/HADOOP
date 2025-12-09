import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKFromCounts {

    public static class CountryCountKey implements WritableComparable<CountryCountKey> {
        private Text country = new Text();
        private int count;
        private Text tag = new Text();

        public CountryCountKey() {}

        public CountryCountKey(String country, int count, String tag) {
            this.country.set(country);
            this.count = count;
            this.tag.set(tag);
        }

        public String getCountry() { return country.toString(); }
        public int getCount() { return count; }
        public String getTag() { return tag.toString(); }

        @Override
        public void write(DataOutput out) throws IOException {
            country.write(out);
            out.writeInt(count);
            tag.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            country.readFields(in);
            count = in.readInt();
            tag.readFields(in);
        }

        @Override
        public int compareTo(CountryCountKey o) {
            int c = this.country.compareTo(o.country);
            if (c != 0) return c;
            int cmp = Integer.compare(o.count, this.count);
            if (cmp != 0) return cmp;
            return this.tag.compareTo(o.tag);
        }

        @Override
        public String toString() {
            return country.toString() + ":" + tag.toString() + "\t" + count;
        }

        public static class GroupingComparator extends WritableComparator {
            public GroupingComparator() { super(CountryCountKey.class, true); }

            @Override
            public int compare(WritableComparable a, WritableComparable b) {
                CountryCountKey k1 = (CountryCountKey)a;
                CountryCountKey k2 = (CountryCountKey)b;
                return k1.country.compareTo(k2.country);
            }
        }

        public static class SortComparator extends WritableComparator {
            public SortComparator() { super(CountryCountKey.class, true); }

            @Override
            public int compare(WritableComparable a, WritableComparable b) {
                CountryCountKey k1 = (CountryCountKey)a;
                CountryCountKey k2 = (CountryCountKey)b;
                return k1.compareTo(k2);
            }
        }
    }

    public static class ParseMapper extends Mapper<LongWritable, Text, CountryCountKey, Text> {
        private final CountryCountKey outKey = new CountryCountKey();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            // Expect: country:tag\tcount
            int tab = line.lastIndexOf('\t');
            if (tab < 0) return;
            String left = line.substring(0, tab);
            String countStr = line.substring(tab+1).trim();
            int colon = left.indexOf(':');
            if (colon < 0) return;
            String country = left.substring(0, colon);
            String tag = left.substring(colon+1);
            int cnt;
            try { cnt = Integer.parseInt(countStr); } catch (NumberFormatException e) { return; }

            CountryCountKey k = new CountryCountKey(country, cnt, tag);
            outVal.set(tag + "\t" + cnt);
            context.write(k, outVal);
        }
    }

    public static class TopKReducer extends Reducer<CountryCountKey, Text, Text, Text> {
        private int k = 10;
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            this.k = conf.getInt("topk", 10);
            if (this.k <= 0) this.k = 10;
        }

        @Override
        protected void reduce(CountryCountKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String country = key.getCountry();
            outKey.set(country);
            int emitted = 0;
            for (Text v : values) {
                if (emitted >= k) break;
                outVal.set(v.toString().replace('\t', ':'));
                context.write(outKey, outVal);
                emitted++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TopKFromCounts <input> <output> <K>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        int k = Integer.parseInt(args[2]);
        conf.setInt("topk", k);

        Job job = Job.getInstance(conf, "TopKFromCounts");
        job.setJarByClass(TopKFromCounts.class);

        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(TopKReducer.class);

        job.setMapOutputKeyClass(CountryCountKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(CountryCountKey.GroupingComparator.class);
        job.setSortComparatorClass(CountryCountKey.SortComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
