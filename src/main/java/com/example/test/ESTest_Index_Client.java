package com.example.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

import java.io.IOException;

/**
 * ES客户端测试
 *
 * @author : snail
 * @date : 2022-03-21 17:18
 **/
public class ESTest_Index_Client {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

        //创建索引
        // CreateIndexRequest request = new CreateIndexRequest("user");
        // CreateIndexResponse response = esClient.indices().create(request, RequestOptions.DEFAULT);
        // boolean acknowledged = response.isAcknowledged();
        // System.out.println("acknowledged = " + acknowledged);

        // 查询索引
        // GetIndexRequest user = new GetIndexRequest("user");
        // GetIndexResponse getIndexResponse = esClient.indices().get(user, RequestOptions.DEFAULT);
        // System.out.println(getIndexResponse.getAliases());
        // System.out.println(getIndexResponse.getMappings());
        // System.out.println(getIndexResponse.getSettings());

        // 删除索引
        // AcknowledgedResponse response = esClient.indices().delete(new DeleteIndexRequest("user"), RequestOptions.DEFAULT);
        // System.out.println(response.isAcknowledged());


        // 关闭ES客户端
        esClient.close();
    }
}
