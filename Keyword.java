package com.net7.scre.processors.authors;


public class Keyword {

    private String word;
    private Long weight;

    public Keyword(String word, Long weight){
        this.word = word;
        this.weight = weight;
    }

    public String getWord() {
        return word;
    }

    public Long getWeight() {
        return weight;
    }

    public void increaseWeight(int value){
        weight+=value;
    }

    public boolean equals(String s){
        return word.equals(s);
    }
}
