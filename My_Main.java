package com.net7.scre.processors.authors;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class My_Main {
    public static void main(String[] args) throws ParseException, IOException {
        /*
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("tirocini", "dpZfBAXLF7qq438T"));
        RestClientBuilder builder =
                RestClient.builder(new HttpHost("es.tirocini.netseven.it", 443, "https"))
                        .setHttpClientConfigCallback(httpClientBuilder ->
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        RestHighLevelClient client = new RestHighLevelClient(builder);

         */


        //DeleteIndexDocs d = new DeleteIndexDocs("authors_cache");
        //d.deleteDocs();
        ANDEuristicCycle and = new ANDEuristicCycle("authors_cache", "scre_cache");
        and.euristicCache();


        //client.close();
    }
    /*  //PUT NEW RECORD
        Map<String, Object> data = new HashMap<>();
        data.put("name", "michele velardita");
        data.put("combination", "velardita, michele");
        data.put("initial", "velardita, m.");

        ArrayList<String> publisher_array = new ArrayList<>();
        publisher_array.add("universita di pisa");
        data.put("publisher", publisher_array);

        ArrayList<String> publicationId_array = new ArrayList<>();
        publicationId_array.add("http:pisa-07071999");
        data.put("publication_id", publicationId_array);

        IndexRequest request = new IndexRequest("authors_cache").source(data);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println(indexResponse.getId());
        */

    /*  //UPDATE KEYWORDS ARRAY DATA (NESTED FIELD IN ELASTIC) !!!->OVERWRITE PREV KEYWORDS ARRAY IN ELASTIC
        UpdateRequest request = new UpdateRequest("authors_cache", "EyPUxoABqOkRLeq1V0Ak");
        Map<String, Object> data = new HashMap<>();

        ArrayList<Map> keywords_array = new ArrayList<>();

        Map<String, Object> nested_tirocinio = new HashMap<>();
        nested_tirocinio.put("weight", 1);
        nested_tirocinio.put("word", "tirocinio");
        keywords_array.add(nested_tirocinio);

        Map<String, Object> nested_gotriple = new HashMap<>();
        nested_gotriple.put("weight", 1);
        nested_gotriple.put("word", "gotriple");
        keywords_array.add(nested_gotriple);

        data.put("keywords", keywords_array);

        request.doc(data);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
     */

    /*  //UPDATE YEARS (INTEGER RANGE FIELD IN ELASTIC) !!!-> gte: lower bound, lte: upper bound
        UpdateRequest request = new UpdateRequest("authors_cache", "EyPUxoABqOkRLeq1V0Ak");
        Map<String, Object> years_data = new HashMap<>();
        Map<String, Object> years_bound = new HashMap<>();
        years_bound.put("gte", 2000);
        years_bound.put("lte", 2010);
        years_data.put("years", years_bound);
        request.doc(years_data);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
     */

    /*  //RETRIEVE KEYWORDS DATA FROM CACHE
        SearchRequest searchRequest = new SearchRequest("authors_cache");
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        ArrayList<Keyword> keywords = new ArrayList<>();
        for(SearchHit h : response.getHits()) {

            JSONParser tmpParser = new JSONParser();
            String tmp = h.toString();
            JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
            JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");
            JSONArray keywords_array = (JSONArray) jsonObject1.get("keywords");
            for (Object j : keywords_array) {
                JSONObject obj = (JSONObject) tmpParser.parse(j.toString());
                String word = (String) obj.get("word");
                System.out.println(word);
                Long weight = (Long)( obj.get("weight"));
                System.out.println(weight);
            }
        }
         */

    /*
        //RETRIEVE YEARS FIELD
        SearchRequest request = new SearchRequest("authors_cache");
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        for(SearchHit h : response.getHits()){
            JSONParser tmpParser = new JSONParser();
            String tmp = h.toString();
            JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
            JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");
            jsonObject = (JSONObject) jsonObject1.get("years");
            Long lower = (Long) jsonObject.get("gte");
            Long greater = (Long) jsonObject.get("lte");
            System.out.println(lower + " " + greater);
        }
         */
}
