import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: sodamnsure
 * @Date: 2021/12/29 9:55 AM
 * @Desc:
 */
public class SaveEs {
    public static void main(String[] args) throws IOException {


        RestClientBuilder builder = RestClient.builder(
                new HttpHost("192.128.1.1", 9200, "http"));
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("test", "test"));
        builder.setHttpClientConfigCallback(f -> f.setDefaultCredentialsProvider(credentialsProvider));
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(builder);
        int length = 6434434 - 6334417;
        List<String> userList = new ArrayList<>(length);
        long x = 19000000004L;

        for (int i = 6334417; i <= 6434434; i++) {
            if (x <= 19000099999L) {
                userList.add(i + "-" + x);
                System.out.println(i + "-" + x);
                x = x + 1;
            }
        }

        // 存储到es
        BulkRequest bulkRequest = new BulkRequest();

        for (String userId : userList) {
            bulkRequest.add(new IndexRequest("test").id(userId).source(XContentType.JSON, "user_id", userId));
        }
        restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }
}
