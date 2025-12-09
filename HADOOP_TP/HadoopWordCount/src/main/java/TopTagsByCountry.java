import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.MinMaxPriorityQueue;

public class TopTagsByCountry {

    public static class TagsByCountryMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {
        private static final int IDX_TAGS = 8;
        private static final int IDX_LONGITUDE = 10;
        private static final int IDX_LATITUDE = 11;

        private final Text outKey = new Text();
        

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.isEmpty()) return;

            String[] fields = line.split("\t", -1);
            if (fields.length <= IDX_LATITUDE) return;

            String lonStr = fields[IDX_LONGITUDE];
            String latStr = fields[IDX_LATITUDE];
            if (lonStr == null || lonStr.isEmpty() || latStr == null || latStr.isEmpty()) return;

            double lon, lat;
            try {
                lon = Double.parseDouble(lonStr);
                lat = Double.parseDouble(latStr);
            } catch (NumberFormatException e) {
                return;
            }

            Country c = Country.getCountryAt(lat, lon);
            if (c == null) return;

            String tagsRaw = (fields.length > IDX_TAGS) ? fields[IDX_TAGS] : "";
            if (tagsRaw == null || tagsRaw.isEmpty()) return;

            String decoded;
            try {
                decoded = URLDecoder.decode(tagsRaw, StandardCharsets.UTF_8.name());
            } catch (IllegalArgumentException iae) {
                return;
            }

            if (decoded.isEmpty()) return;

            StringTokenizer st = new StringTokenizer(decoded, ",");
            if (!st.hasMoreTokens()) return;

            outKey.set(c.toString());
            while (st.hasMoreTokens()) {
                String tag = st.nextToken().trim().toLowerCase();
                if (tag.isEmpty()) continue;
                try {
                    tag = URLDecoder.decode(tag, StandardCharsets.UTF_8.name());
                } catch (IllegalArgumentException ignored) {
                }
                if (tag.isEmpty()) continue;
                context.write(outKey, new StringAndInt(tag, 1));
            }
        }
    }

    public static class TopKReducer extends Reducer<Text, StringAndInt, Text, Text> {
        private int k;

        @Override
        protected void setup(Context context) {
            this.k = context.getConfiguration().getInt("topk", 10);
            if (this.k <= 0) this.k = 10;
        }

        @Override
        protected void reduce(Text country, Iterable<StringAndInt> tagCounts, Context context) throws IOException, InterruptedException {
            Map<String, Integer> counts = new HashMap<>();
            for (StringAndInt sai : tagCounts) {
                String tag = sai.getTag();
                if (tag.isEmpty()) continue;
                counts.merge(tag, sai.getCount(), Integer::sum);
            }

            MinMaxPriorityQueue<StringAndInt> top = MinMaxPriorityQueue
                    .maximumSize(k)
                    .create();

            for (Map.Entry<String, Integer> e : counts.entrySet()) {
                top.add(new StringAndInt(e.getKey(), e.getValue()));
            }

            StringAndInt[] arr = top.toArray(new StringAndInt[0]);
            java.util.Arrays.sort(arr);

            StringBuilder sb = new StringBuilder();
            int emitted = 0;
            for (StringAndInt sai : arr) {
                if (emitted > 0) sb.append(", ");
                sb.append(sai.toString());
                emitted++;
            }

            context.write(country, new Text(sb.toString()));
        }
    }

    public static class TopKCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {
        private int kprime;

        @Override
        protected void setup(Context context) {
            int k = context.getConfiguration().getInt("topk", 10);
            if (k <= 0) k = 10;
            this.kprime = Math.max(1, k * 3);
        }

        @Override
        protected void reduce(Text country, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> counts = new HashMap<>();
            for (StringAndInt sai : values) {
                String tag = sai.getTag();
                if (tag == null || tag.isEmpty()) continue;
                counts.merge(tag, sai.getCount(), Integer::sum);
            }

            MinMaxPriorityQueue<StringAndInt> top = MinMaxPriorityQueue
                    .maximumSize(kprime)
                    .create();
            for (Map.Entry<String, Integer> e : counts.entrySet()) {
                top.add(new StringAndInt(e.getKey(), e.getValue()));
            }
            for (StringAndInt sai : top) {
                context.write(country, sai);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TopTagsByCountry <input> <output> <K>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        int k = Integer.parseInt(args[2]);
        conf.setInt("topk", k);

        Job job = Job.getInstance(conf, "TopTagsByCountry");
        job.setJarByClass(TopTagsByCountry.class);

        job.setMapperClass(TagsByCountryMapper.class);
        job.setCombinerClass(TopKCombiner.class);
        job.setReducerClass(TopKReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringAndInt.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}