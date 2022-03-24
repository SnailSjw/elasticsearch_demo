package com.example.test;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * ES客户端测试
 *
 * @author : snail
 * @date : 2022-03-21 17:18
 **/
public class ESTest_Client {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

        // 关闭ES客户端
        esClient.close();
    }
}
