package com.net7.scre.processors.authors;

import org.jdom2.output.support.SAXOutputProcessor;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.gcardone.junidecode.Junidecode.unidecode;

public class AuthorData {

    private String author;
    private int name_type;  //1 -> name
                            //2 -> combination
                            //3 -> initial
    private ArrayList<String> keywords;
    private ArrayList<String> publisher;

    private String name;
    private String combination;
    private String initial;

    public AuthorData(ArrayList<String> publisher, String author, ArrayList<String> keywords) throws ParseException {
        this.publisher = publisher;
        this.author = author;
        this.keywords = keywords;

        normalizePublisher();
        normalizeAuthor();
        normalizeKeywords();

        name_type = 0;  // first, not recognized
        name = null;
        combination = null;
        initial = null;
        recognizeType();
    }

    private void normalizeAuthor(){
        author = unidecode(author);
        author = author.toLowerCase();
        author = author.replaceAll("dr ", "");
        author = author.replaceAll("dra ", "");
        author = author.replaceAll("dr\\. ", "");
        author = author.replaceAll("dra\\. ", "");
        author = author.replaceAll("lic\\. ", "");
        author = author.replaceAll(", phd", "");
        //caso particolare: "Federico Rossi, "
        if(author.contains(",")){
            String[] tmp = author.split(",");
            if(tmp.length==1) author = tmp[0].replaceAll(",", "");  // "," senza spazio
            else if(tmp[1].equals(" ")) author = author = tmp[0].replaceAll(",", ""); // ", " con spazio
        }
        author = author.trim();

    }

    private void normalizeKeywords(){
        ArrayList<String> tmp = new ArrayList<>();
        for(String k : keywords){
            //remove all non-alphabetic character
            if(k.replaceAll("[^A-Za-z]", "").length() > 2)
                tmp.add(k.replaceAll("[^A-Za-z]", "").toLowerCase());
        }
        keywords = tmp;
    }

    private void normalizePublisher(){
        for(String s : publisher) {
            s = unidecode(s);
            s = s.toLowerCase();
            s = s.trim();
            s = s.replaceAll("\\p{Punct}", "");
        }
    }

    public void recognizeType(){
        if(!author.contains(",")){      //Name case (type recognized: 1)
            name_type = 1;
            name = author;
        }
        else{   //initial case (type recognized: 3)
            Pattern pattern = Pattern.compile(", \\w\\.", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(author);
            if(matcher.find()){
                name_type = 3;
                //String initial = matcher.group(); //e.g. Di Donato, F. => matcher.group = F.
                initial = author;
            }
            else{   //combination case (type recognized: 2)
                name_type = 2;
                combination = author;
                //creo il "name"
                String[] tmp = author.split(",");
                name = tmp[1].trim() + " " + tmp[0].trim();
                //creo "initial"
                initial = tmp[0].trim() + ", " + (tmp[1].trim()).charAt(0) + ".";
            }
        }

    }

    public String getAuthor(){
        return author;
    }

    public int getType(){
        return name_type;
    }

    public String getNameField(){
        return name;
    }

    public String getCombinationField(){
        return combination;
    }

    public String getInitialField(){
        return initial;
    }

    public ArrayList<String> getPublisher(){
        return publisher;
    }

    public ArrayList<String> getKeywords(){
        return keywords;
    }
}
