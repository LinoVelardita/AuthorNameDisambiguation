package com.net7.scre.processors.authors;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.security.Key;
import java.time.Year;
import java.util.*;

import static java.util.Collections.singletonMap;

//Author Name Disambiguation Euristic
public class ANDEuristic {

    private String name;
    private String combination;
    private String initial;

    private ArrayList<String> keywords;
    private ArrayList<String> publishers;
    private Integer year;
    private String publication_id;

    //thresholds and scores
    private int min_score = 20;

    private final int name_match_score = 10;
    private final int combination_match_score = 10;
    private final int initial_match_score = 5;

    private final int keyword_score = 1;
    private final int increase_keyword_weight = 1;
    private final int publisher_score = 8;
    private final int year_in_range_score = 7;
    private final int five_year_diff_score = 5;
    private final int ten_year_diff_score = 2;

    //to manage prints
    private final boolean ALLOW_PRINT = false;

    public ANDEuristic(String name, String combination, String initial,
                       ArrayList<String> keywords, ArrayList<String> publishers, Integer year, String publication_id){
        this.name = name;
        this.combination = combination;
        this.initial = initial;
        this.keywords = keywords;
        this.publishers = publishers;
        this.year = year;
        this.publication_id = publication_id;
    }

    //main function
    public void euristic(String scre_authors_cache) throws IOException, ParseException {
        RestHighLevelClient client = connect();
        SearchRequest request = new SearchRequest(scre_authors_cache);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.from(0);
        builder.size(9000);

        //"scores" HashTable save positive matches with the respective score
        Hashtable<SearchHit, Integer> scores = new Hashtable<>();

        //Searching for Name, Combination or Initial match
        if(name!=null){
            MatchQueryBuilder name_query = new MatchQueryBuilder("name", name);
            builder.query(name_query);
            request.source(builder);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            for(SearchHit h : response.getHits()){
                scores.put(h, name_match_score);
                print("Name "+name+" match. New score: "+scores.get(h));
            }
        }
        else if(combination!=null){
            MatchQueryBuilder combination_query = new MatchQueryBuilder("combination", combination);
            builder.query(combination_query);
            request.source(builder);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            for(SearchHit h : response.getHits()){
                scores.put(h, combination_match_score);
                print("Combination "+combination+" match. New score: "+scores.get(h));
            }
        }
        else if(initial!=null){
            MatchQueryBuilder initial_query = new MatchQueryBuilder("initial", initial);
            builder.query(initial_query);
            request.source(builder);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            for(SearchHit h : response.getHits()){
                scores.put(h, initial_match_score);
                print("Initial "+initial+" match. New score: "+scores.get(h));
            }
        }

        /*Giving scores
            1) keywords
            2) publisher
            3) years
         */

        for(SearchHit h : scores.keySet()){
            //keywords matching
            ArrayList<Keyword> keywords_hit = retrieveKeywords(h);  //retrieve keywords array (word and weight)
            //increase score
            for(Keyword k : keywords_hit){
                if(keywords.contains(k.getWord())) {
                    scores.replace(h, scores.get(h) + (keyword_score * k.getWeight().intValue()));  //THRESHOLD
                    print("keyword " + k.getWord() + " match. New score: " +scores.get(h));
                }
            }

            //publisher matching
            ArrayList<String> publisher_hit = retrievePublisher(h); //retrieve publisher array
            //increase score
            for(String s : publisher_hit){
                if(publishers.contains(s)) {
                    scores.replace(h, scores.get(h) + publisher_score);   //THRESHOLD
                    print("publisher "+ s +" match. New score: "+scores.get(h));
                }
            }

            //year matching (only if year is not null)
                //retrieve years.gte and years.lte
            if(year!=null) {
                YearRange year_hit = retrieveYear(h);
                //check if "year" is in range
                if (year >= year_hit.getLower() & year <= year_hit.getGreater()) {
                    scores.replace(h, scores.get(h) + year_in_range_score);
                    print("Year in range. New score: " + scores.get(h));
                }
                //check if difference from range is at most 5
                else if (Math.abs(year - year_hit.getGreater()) <= 5 || Math.abs(year - year_hit.getLower()) <= 5) {
                    scores.replace(h, scores.get(h) + five_year_diff_score);
                    print("Year difference from range is at most 5. New score: " + scores.get(h));
                }
                //check if difference from range is at most 10
                else if (Math.abs(year - year_hit.getGreater()) <= 10 || Math.abs(year - year_hit.getLower()) <= 10) {
                    scores.replace(h, scores.get(h) + ten_year_diff_score);
                    print("Year difference from range is ta most 10. New score: " + scores.get(h));
                }
            }

        }//END GIVING SCORE

        //remove low score match (under min_score threshold)
        ArrayList<SearchHit> to_remove = new ArrayList<>();
        for(SearchHit h : scores.keySet()){
            if(scores.get(h) < min_score){  //threshold
                print(h.getId()+" has "+scores.get(h)+" score(removed)");
                to_remove.add(h);
            }
        }
        for(SearchHit h : to_remove)
            scores.remove(h);

        //some positive match (score > min_score threshold) -> merge
        if(!scores.isEmpty()) {
            //DATA TO STORE IN AUTHORS CACHE
            //create ArrayList of keyword
            ArrayList<Keyword> keywords_to_store = new ArrayList<>();

            //create a YearRange from input year
            YearRange years_to_store = new YearRange(year, year);
            //crate a ArrayList of publisher
            ArrayList<String> publishers_to_store = new ArrayList<>();
            if(publishers!=null) publishers_to_store.addAll(publishers);
            //create a ArrayList of publication_id
            ArrayList<String> publicationId_to_store = new ArrayList<>();
            publicationId_to_store.add(publication_id);

            //MERGE DATA
            for(SearchHit h : scores.keySet()) {
                //fill name, combination or initial field
                if(name==null) name = retrieveName(h);
                if(combination==null) combination = retrieveCombination(h);
                if(initial==null) initial = retrieveInitial(h);

                //update keywords (add the new keywords and increase matching keywords weight)
                keywords_to_store = retrieveKeywords(h);
                for(Keyword k : keywords_to_store) {
                    if (keywords.contains(k.getWord())) {  //keywords match -> increase weight
                        k.increaseWeight(increase_keyword_weight);  //threshold
                        keywords.remove(k.getWord()); //in this way, in "keywords" theres's only the new keyword to add
                    }
                }
                //add new keywords (keyword contains only the new keyword)
                for(String s : keywords)
                    keywords_to_store.add(new Keyword(s, 1L));

                //update years range
                    //retrieve years range
                YearRange years_hit = retrieveYear(h);
                //update year range (see YearRange Class)
                years_to_store.updateYear(years_hit);

                //add new publisher(s)
                ArrayList<String> publisher_hit = new ArrayList<>();
                publisher_hit = retrievePublisher(h);
                for(String s : publisher_hit){
                    if(!publishers_to_store.contains(s))
                        publishers_to_store.add(s);
                }

                //add new publication id
                ArrayList<String> publicationId_hit = new ArrayList<>();
                publicationId_hit = retrievePublicationId(h);
                for(String s : publicationId_hit){
                    if(!publicationId_to_store.contains(s))
                        publicationId_to_store.add(s);
                }

                //DELETE OLD RECORD (NOW MERGED WITH NEW RECORD)
                DeleteRequest deleteRequest = new DeleteRequest(scre_authors_cache, h.getId());
                DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
                print(response.toString());
            }//END FOR

            //STORE NEW RECORD (MERGED)
            Map<String, Object> data = new HashMap<>();
            if(name!=null) data.put("name", name);
            if(combination!=null) data.put("combination", combination);
            if(initial!=null) data.put("initial", initial);
            data.put("publisher", publishers_to_store);
            data.put("publication_id", publicationId_to_store);
            ArrayList<Map> keywords_array = new ArrayList<>();
            for(Keyword k : keywords_to_store){
                Map<String, Object> one_keyword = new HashMap<>();
                one_keyword.put("word", k.getWord());
                one_keyword.put("weight", k.getWeight());
                keywords_array.add(one_keyword);
            }
            data.put("keywords", keywords_array);

            Map<String, Object> years_bound = new HashMap<>();
            years_bound.put("gte", years_to_store.getLower());
            years_bound.put("lte", years_to_store.getGreater());
            data.put("years", years_bound);

            IndexRequest index_request = new IndexRequest(scre_authors_cache).source(data);
            index_request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            IndexResponse indexResponse = client.index(index_request, RequestOptions.DEFAULT);
            print("Merge the new record, now with id: "+indexResponse.getId());

        }//END IF
        else { //zero positive match (all scores < 20) -> STORE THE NEW RECORD
            Map<String, Object> data = new HashMap<>();
            //name, combination and initial field
            if(name!=null)  data.put("name", name);
            if(combination!=null)   data.put("combination", combination);
            if(initial!=null)   data.put("initial", initial);

            //publisher field (array)
            data.put("publisher", publishers);

            //publication_id field (array)
            ArrayList<String> publicationId_array = new ArrayList<>();
            publicationId_array.add(publication_id);
            if(publication_id!=null) data.put("publication_id", publicationId_array);

            //years field
            Map<String, Object> years_bound = new HashMap<>();
            years_bound.put("gte", year);
            years_bound.put("lte", year);
            data.put("years", years_bound);

            //keywords field (array)
            ArrayList<Map> keywords_array = new ArrayList<>();
            for(String s : keywords){
                Map<String, Object> one_keyword = new HashMap<>();
                one_keyword.put("word", s);
                one_keyword.put("weight", 1);
                keywords_array.add(one_keyword);
            }
            data.put("keywords", keywords_array);

            IndexRequest index_request = new IndexRequest(scre_authors_cache).source(data);
            IndexResponse indexResponse = client.index(index_request, RequestOptions.DEFAULT);
            print("Store the new record with id :"+indexResponse.getId());
        }
        client.close();
    }

    private ArrayList<Keyword> retrieveKeywords(SearchHit h) throws ParseException {
        ArrayList<Keyword> keywords = new ArrayList<>();
        JSONParser tmpParser = new JSONParser();
        String tmp = h.toString();
        JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
        JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");
        JSONArray keywords_array = (JSONArray) jsonObject1.get("keywords");
        for(Object j : keywords_array){
            jsonObject = (JSONObject) tmpParser.parse(j.toString());
            keywords.add(new Keyword((String) jsonObject.get("word"), (Long) jsonObject.get("weight")));
        }
        return keywords;
    }

    private ArrayList<String> retrievePublisher(SearchHit h) throws ParseException {
        ArrayList<String> publishers = new ArrayList<>();
        JSONParser tmpParser = new JSONParser();
        String tmp = h.toString();
        JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
        JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");
        JSONArray publisher_array = (JSONArray) jsonObject1.get("publisher");
        if(publisher_array!=null) {
            for (Object j : publisher_array) {
                publishers.add((String) j);
            }
        }
        return publishers;
    }

    private YearRange retrieveYear(SearchHit h) throws ParseException {
        JSONParser tmpParser = new JSONParser();
        String tmp = h.toString();
        JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
        JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");
        jsonObject = (JSONObject) jsonObject1.get("years");
        long lower = (long) jsonObject.get("gte");
        long greater = (long) jsonObject.get("lte");
        return new YearRange(lower, greater);
    }

    private ArrayList<String> retrievePublicationId(SearchHit h) throws ParseException {
        ArrayList<String> publicationIds = new ArrayList<>();
        JSONParser tmpParser = new JSONParser();
        String tmp = h.toString();
        JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
        JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");
        JSONArray publicationId_array= (JSONArray) jsonObject1.get("publication_id");
        for(Object j : publicationId_array){
            publicationIds.add((String) j);
        }
        return publicationIds;
    }

    private String retrieveName(SearchHit h) throws ParseException {
        JSONParser tmpParser = new JSONParser();
        String tmp = h.toString();
        JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
        JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");
        return (String) jsonObject1.get("name");
    }

    private String retrieveCombination(SearchHit h) throws ParseException {
        JSONParser tmpParser = new JSONParser();
        String tmp = h.toString();
        JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
        JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");
        return (String) jsonObject1.get("combination");
    }

    private String retrieveInitial(SearchHit h) throws ParseException {
        JSONParser tmpParser = new JSONParser();
        String tmp = h.toString();
        JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
        JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");
        return (String) jsonObject1.get("initial");
    }

    private RestHighLevelClient connect(){
        //connect to tirocini Elastic
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("tirocini", "dpZfBAXLF7qq438T"));
        RestClientBuilder builder =
                RestClient.builder(new HttpHost("es.tirocini.netseven.it", 443, "https"))
                        .setHttpClientConfigCallback(httpClientBuilder ->
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
    }

    private void print(String s){
        if(ALLOW_PRINT)
            System.out.println(s);
    }

}
