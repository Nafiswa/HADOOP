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

/**
 * Hadoop job: compute top-K user tags per country from Flickr CC dataset.
 * Input format: TSV as described in flickrSpecs.txt
 * Fields (0-based): tags at 8, longitude at 10, latitude at 11.
 */
public class TopTagsByCountry {

    public static class TagsByCountryMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final int IDX_TAGS = 8;
        private static final int IDX_LONG = 10;
        private static final int IDX_LAT = 11;

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.isEmpty()) return;

            String[] fields = line.split("\t", -1); // keep empty trailing fields
            // We expect at least up to index 11
            if (fields.length <= IDX_LAT) return;

            String lonStr = fields[IDX_LONG];
            String latStr = fields[IDX_LAT];
            if (lonStr == null || lonStr.isEmpty() || latStr == null || latStr.isEmpty()) return;

            double lon, lat;
            try {
                lon = Double.parseDouble(lonStr);
                lat = Double.parseDouble(latStr);
            } catch (NumberFormatException e) {
                return; // malformed coords
            }

            Country c = Country.getCountryAt(lat, lon);
            if (c == null) return;

            String tagsRaw = (fields.length > IDX_TAGS) ? fields[IDX_TAGS] : "";
            if (tagsRaw == null || tagsRaw.isEmpty()) return;

            // URL-decode and split on commas
            String decoded;
            try {
                decoded = URLDecoder.decode(tagsRaw, StandardCharsets.UTF_8.name());
            } catch (IllegalArgumentException iae) {
                // Bad encoding, skip line
                return;
            }

            if (decoded.isEmpty()) return;

            // Tags are comma separated; individual tags can contain whitespace
            // Normalize to lowercase and trim; skip empties
            StringTokenizer st = new StringTokenizer(decoded, ",");
            if (!st.hasMoreTokens()) return;

            outKey.set(c.toString());
            while (st.hasMoreTokens()) {
                String tag = st.nextToken().trim().toLowerCase();
                if (tag.isEmpty()) continue;
                // A tag itself may still be URL-encoded remnants; best-effort decode again
                try {
                    tag = URLDecoder.decode(tag, StandardCharsets.UTF_8.name());
                } catch (IllegalArgumentException ignored) {
                }
                if (tag.isEmpty()) continue;
                outVal.set(tag);
                context.write(outKey, outVal);
            }
        }
    }

    public static class TopKReducer extends Reducer<Text, Text, Text, Text> {
        private int k;

        @Override
        protected void setup(Context context) {
            this.k = context.getConfiguration().getInt("topk", 10);
            if (this.k <= 0) this.k = 10;
        }

        @Override
        protected void reduce(Text country, Iterable<Text> tags, Context context) throws IOException, InterruptedException {
            // Count tag frequencies for this country
            Map<String, Integer> counts = new HashMap<>();
            for (Text t : tags) {
                String tag = t.toString();
                if (tag.isEmpty()) continue;
                counts.merge(tag, 1, Integer::sum);
            }

            // Maintain a bounded priority queue of top-K by count (desc)
            MinMaxPriorityQueue<StringAndInt> top = MinMaxPriorityQueue
                    .maximumSize(k)
                    .create();

            for (Map.Entry<String, Integer> e : counts.entrySet()) {
                top.add(new StringAndInt(e.getKey(), e.getValue()));
            }

            // Drain queue into a string in order (highest first)
            // MinMaxPriorityQueue doesn't guarantee iteration order; extract to array then sort using natural ordering
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
        job.setReducerClass(TopKReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
