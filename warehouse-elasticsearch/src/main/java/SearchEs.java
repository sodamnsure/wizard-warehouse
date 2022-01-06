import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


/**
 * @Author: sodamnsure
 * @Date: 2022/1/6 10:18 AM
 * @Desc: 查询es
 */
@Slf4j
public class SearchEs {
    public static void main(String[] args) throws IOException {
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("192.168.1.1", 9200, "http"));
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("test", "test"));
        builder.setHttpClientConfigCallback(f -> f.setDefaultCredentialsProvider(credentialsProvider));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        BufferedWriter out = new BufferedWriter(new FileWriter("test.txt", true));


        QueryBuilder matchQueryBuilder = QueryBuilders.matchAllQuery();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(matchQueryBuilder);

        searchSourceBuilder.size(5000); //max is 10000
        SearchRequest searchRequest = new SearchRequest("test");

        searchRequest.indices("test");

        searchRequest.source(searchSourceBuilder);

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(10L));

        searchRequest.scroll(scroll);


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();

        SearchHit[] allHits = new SearchHit[0];

        SearchHit[] searchHits = searchResponse.getHits().getHits();

        while (searchHits != null && searchHits.length > 0) {

            SearchHit[] hits = searchResponse.getHits().getHits();//create a function which concatenate two arrays
            for (SearchHit hit : searchHits) {
                String json = hit.getId();
                System.out.println(json);
                out.write(json);
                out.write("\r\n");
            }

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);

            scrollRequest.scroll(scroll);

            searchResponse = client.searchScroll(scrollRequest, RequestOptions.DEFAULT);

            scrollId = searchResponse.getScrollId();

            searchHits = searchResponse.getHits().getHits();
            for (SearchHit hit : searchHits) {
                String json = hit.getId();
                System.out.println(json);
                out.write(json);
                out.write("\r\n");
            }

        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);

    }
}
