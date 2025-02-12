package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.contains(":")) {
            String[] parts = line.split(":", 2);
            character.set(parts[0].trim());
            StringTokenizer tokenizer = new StringTokenizer(parts[1].trim());

            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
                if (!word.toString().isEmpty()) {
                    context.write(character, word);
                }
            }
        }
    }
}
