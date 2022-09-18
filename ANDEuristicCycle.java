package com.net7.scre.processors.authors;

import net.arnx.jsonic.JSON;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.io.IOException;
import java.util.*;

//Authors Name Disambiguation Euristic
public class ANDEuristicCycle implements Processor {

    private String cache_index;
    private String data_index;
    private String id; //optional, for testing ANDEuristic

    private RestHighLevelClient client;
    private final int score_threshold = 20;

//    public ANDEuristicCycle(String cache_index, String data_index, String id){
//        this.cache_index = cache_index;
//        this.data_index = data_index;
//        this.id = id;
//    }
//    public ANDEuristicCycle(String cache_index, String data_index){
//        this.cache_index = cache_index;
//        this.data_index = data_index;
//        this.id = null;
//    }



    @Override
    public void process(Exchange exchange) throws Exception {
        List<HashMap<String, Object>> map_list = exchange.getIn().getBody(List.class);

        this.cache_index = "scre_authors_cache";
        this.data_index = "scre_cache";
        this.id = null;
        this.euristicCache();
    }


    public void euristic() throws IOException, ParseException {
        //connect to data_index
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("triple_adm", "ZLEWfd8nUDuY6uE"));
        RestClientBuilder builder =
                RestClient.builder(new HttpHost("essearch.huma-num.fr", 9200, "https"))
                        .setHttpClientConfigCallback(httpClientBuilder ->
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        this.client = new RestHighLevelClient(builder);

        SearchRequest searchRequest = new SearchRequest(data_index);
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        searchRequest.scroll(scroll);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.searchAfter();
        if(this.id!=null){
            sourceBuilder.query(new MatchQueryBuilder("id", id));
        }
        else {
            sourceBuilder.query(QueryBuilders.matchAllQuery());
        }
        //sourceBuilder.from(0);
        //sourceBuilder.size(1000);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        JSONParser tmpParser = new JSONParser();
        String publication_id;
        Integer year;
        ArrayList<String> authors;
        ArrayList<String> keywords;
        ArrayList<String> publishers;
        String tmp;

        int count = 0;
        while(searchHits != null && searchHits.length > 0) {
            for(SearchHit hit : searchHits) {
                System.out.println(count++);

                authors = new ArrayList<String>();
                keywords = new ArrayList<String>();
                publishers = new ArrayList<>();

                tmp = hit.toString();
                JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
                JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");

                //parsing authors
                JSONArray authors_json_array = (JSONArray) jsonObject1.get("author");
                for (Object j : authors_json_array) {
                    JSONObject author_obj = (JSONObject) tmpParser.parse(j.toString());
                    String author_name = (String) author_obj.get("fullname");
                    if (!authors.contains(author_name) && author_name != null) authors.add(author_name);
                }

                //parsing keywords
                JSONArray keywords_json_array = (JSONArray) jsonObject1.get("keywords");
                for (Object j : keywords_json_array) {
                    JSONObject keyword_obj = (JSONObject) tmpParser.parse(j.toString());
                    String keyword_text = (String) keyword_obj.get("text");
                    if (!keywords.contains(keyword_text) && keyword_text != null) keywords.add(keyword_text);
                }

                //parsing date-published
                String date = (String) ((JSONArray) jsonObject1.get("date_published")).get(0);
                if (date != null) year = extractYear(date);
                else year = null;

                //parsing publisher
                JSONArray publisher_json_array = (JSONArray) jsonObject1.get("publisher");
                for (Object j : publisher_json_array) {
                    if (j != null) publishers.add((String) j);
                }

                //parsing id
                publication_id = (String) jsonObject1.get("id");    //can be null(?)


                for (String author : authors) {
                    AuthorData a = new AuthorData(publishers, author, keywords);
                    //print
                    /*
                    System.out.println("\n");
                    System.out.println(a.getNameField() + " - " + a.getCombinationField() + " - " + a.getInitialField());
                    System.out.println(a.getKeywords());    //can be empty
                    System.out.println(a.getPublisher());   //can be empty
                    System.out.println(year);   //can be null
                    System.out.println(publication_id);
                    System.out.println("\n");
                    */
                    //end print
                    ANDEuristic and = new ANDEuristic(a.getNameField(), a.getCombinationField(), a.getInitialField(),
                            a.getKeywords(), a.getPublisher(), year, hit.getId());
                    and.euristic("scre_authors_cache");
                }
            }
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            System.out.println(count++);
        }
        client.close();
    }

    private int extractYear(String date){
        String[] tmp = date.split("-");
        return Integer.parseInt(tmp[0]);
    }

    public void euristicCache() throws IOException, ParseException {
        //connect to data_index
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("tirocini", "dpZfBAXLF7qq438T"));
        RestClientBuilder builder =
                RestClient.builder(new HttpHost("es.tirocini.netseven.it", 443, "https"))
                        .setHttpClientConfigCallback(httpClientBuilder ->
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        this.client = new RestHighLevelClient(builder);

        SearchRequest searchRequest = new SearchRequest(data_index);
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        searchRequest.scroll(scroll);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.searchAfter();
        if(this.id!=null){
            sourceBuilder.query(new MatchQueryBuilder("id", id));
        }
        else {
            sourceBuilder.query(QueryBuilders.matchAllQuery());
        }
        //sourceBuilder.from(0);
        //sourceBuilder.size(1000);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        JSONParser tmpParser = new JSONParser();
        String publication_id;
        Integer year;
        ArrayList<String> authors;
        ArrayList<String> keywords;
        ArrayList<String> publishers;
        String tmp;

        int count = 0;
        while(searchHits != null && searchHits.length > 0) {
            for(SearchHit hit : searchHits) {
                System.out.println(count++);

                authors = new ArrayList<String>();
                keywords = new ArrayList<String>();
                publishers = new ArrayList<>();

                tmp = hit.toString();
                JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
                JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");

                //parsing authors
                JSONArray authors_json_array = (JSONArray) jsonObject1.get("authors");
                for (Object j : authors_json_array) {
                    String author_name = (String) j;
                    if (!authors.contains(author_name) && author_name != null) authors.add(author_name);
                }

                //parsing keywords
                JSONArray keywords_json_array = (JSONArray) jsonObject1.get("normalized_keywords");
                for (Object j : keywords_json_array) {
                    JSONObject keyword_obj = (JSONObject) tmpParser.parse(j.toString());
                    String keyword_text = (String) keyword_obj.get("text");
                    if (!keywords.contains(keyword_text) && keyword_text != null) keywords.add(keyword_text);
                }

                //parsing date-published
                Long date = (Long) ((JSONArray) jsonObject1.get("normalized_dates")).get(0);
                if (date != null) year = date.intValue();
                else year = null;

                //parsing publisher
                JSONArray publisher_json_array = (JSONArray) jsonObject1.get("publisher");
                for (Object j : publisher_json_array) {
                    if (j != null) publishers.add((String) j);
                }

                //parsing id
                publication_id = (String) jsonObject1.get("id");    //can be null(?)


                for (String author : authors) {
                    AuthorData a = new AuthorData(publishers, author, keywords);
                    //print
                    /*
                    System.out.println("\n");
                    System.out.println(a.getNameField() + " - " + a.getCombinationField() + " - " + a.getInitialField());
                    System.out.println(a.getKeywords());    //can be empty
                    System.out.println(a.getPublisher());   //can be empty
                    System.out.println(year);   //can be null
                    System.out.println(publication_id);
                    System.out.println("\n");
                    */
                    //end print
                    ANDEuristic and = new ANDEuristic(a.getNameField(), a.getCombinationField(), a.getInitialField(),
                            a.getKeywords(), a.getPublisher(), year, hit.getId());
                    and.euristic("scre_authors_cache");
                }
            }
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            System.out.println(count++);
        }
        client.close();
    }


}
