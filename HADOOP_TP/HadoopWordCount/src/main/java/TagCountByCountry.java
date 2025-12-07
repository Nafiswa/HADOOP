import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hadoop job: count occurrences of tags per country.
 * Intermediate key: "<country>:<tag>" (as Text)
 * Output: key = country:tag, value = count (IntWritable)
 */
public class TagCountByCountry {

    public static class TagsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final int IDX_TAGS = 8;
        private static final int IDX_LONGITUDE = 10;
        private static final int IDX_LATITUDE = 11;

        private final Text outKey = new Text();
        private final static IntWritable ONE = new IntWritable(1);

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

            String countryStr = c.toString();
            while (st.hasMoreTokens()) {
                String tag = st.nextToken().trim().toLowerCase();
                if (tag.isEmpty()) continue;
                try {
                    tag = URLDecoder.decode(tag, StandardCharsets.UTF_8.name());
                } catch (IllegalArgumentException ignored) {
                }
                if (tag.isEmpty()) continue;
                outKey.set(countryStr + ":" + tag);
                context.write(outKey, ONE);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TagCountByCountry <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TagCountByCountry");
        job.setJarByClass(TagCountByCountry.class);

        job.setMapperClass(TagsCountMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
