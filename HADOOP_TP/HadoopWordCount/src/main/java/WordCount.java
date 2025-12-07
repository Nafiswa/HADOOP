/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

    // Enum pour définir le nom du compteur
    public enum LINE_COUNTERS {
        EMPTY_LINES
    }

    // Mapper classique (sans in-mapper combiner)
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString().trim();
            
            // Compter les lignes vides
            if (line.isEmpty()) {
                // Utilisation d'une méthode plus compatible
                context.getCounter("WordCount", "EMPTY_LINES").increment(1);
                return; // Ne pas traiter les lignes vides
            }
            
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    // Mapper avec in-mapper combiner
    public static class InMapperCombinerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private HashMap<String, Integer> buffer = new HashMap<>();
        private Text word = new Text();
        private IntWritable count = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            buffer.clear();
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString().trim();
            
            // Compter les lignes vides
            if (line.isEmpty()) {
                // Utilisation d'une méthode plus compatible
                context.getCounter("WordCount", "EMPTY_LINES").increment(1);
                return; // Ne pas traiter les lignes vides
            }
            
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                buffer.put(token, buffer.getOrDefault(token, 0) + 1);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (HashMap.Entry<String, Integer> entry : buffer.entrySet()) {
                word.set(entry.getKey());
                count.set(entry.getValue());
                context.write(word, count);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount [-inmapper] <in> [<in>...] <out>");
            System.exit(2);
        }

        boolean useInMapper = false;
        int inputStartIndex = 0;
        
        // Vérifier si l'option -inmapper est utilisée
        if (otherArgs.length > 0 && otherArgs[0].equals("-inmapper")) {
            useInMapper = true;
            inputStartIndex = 1;
        }

        Job job;
        if (useInMapper) {
            job = Job.getInstance(conf, "word count with in-mapper combiner");
            job.setMapperClass(InMapperCombinerMapper.class);
            // Pas de combiner externe - combinaison faite dans le mapper
            System.out.println("Mode: In-Mapper Combiner");
        } else {
            job = Job.getInstance(conf, "word count with external combiner");
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            System.out.println("Mode: External Combiner");
        }
        
        job.setJarByClass(WordCount.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(3);  // Configuration de 3 reducers
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        for (int i = inputStartIndex; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
