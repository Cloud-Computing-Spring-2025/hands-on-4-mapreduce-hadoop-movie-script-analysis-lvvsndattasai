package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class HadoopCountersMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public enum COUNTERS {
        TOTAL_LINES_PROCESSED,
        TOTAL_WORDS_PROCESSED,
        TOTAL_CHARACTERS_PROCESSED,
        TOTAL_UNIQUE_WORDS_IDENTIFIED,
        NUMBER_OF_CHARACTERS_SPEAKING
    }

    private final static IntWritable one = new IntWritable(1);
    private Text character = new Text();
    private IntWritable wordCount = new IntWritable();
    private HashSet<String> uniqueWords = new HashSet<>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        if (!line.isEmpty() && line.contains(":")) {
            context.getCounter(COUNTERS.TOTAL_LINES_PROCESSED).increment(1);

            String[] parts = line.split(":", 2);
            if (parts.length < 2) return;

            String characterName = parts[0].trim().toUpperCase();
            String dialogue = parts[1].trim();
            
            character.set(characterName);
            context.getCounter(COUNTERS.NUMBER_OF_CHARACTERS_SPEAKING).increment(1);
            context.getCounter(COUNTERS.TOTAL_CHARACTERS_PROCESSED).increment(dialogue.length());

            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            int count = 0;
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!token.isEmpty()) {
                    uniqueWords.add(token);
                    count++;
                }
            }

            // Write total words processed per line
            wordCount.set(count);
            context.write(new Text("WORDS_PROCESSED"), wordCount);
            context.getCounter(COUNTERS.TOTAL_WORDS_PROCESSED).increment(count);

            // Write total unique words count
            wordCount.set(uniqueWords.size());
            context.write(new Text("UNIQUE_WORDS_IDENTIFIED"), wordCount);
            context.getCounter(COUNTERS.TOTAL_UNIQUE_WORDS_IDENTIFIED).increment(uniqueWords.size());
        }
    }
}
