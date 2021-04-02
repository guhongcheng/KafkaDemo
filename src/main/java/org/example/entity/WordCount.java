package org.example.entity;

public class WordCount {
    public String word;
    public long count;

    public WordCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return word + " : " + count;
    }

    public WordCount() {
    }
}
