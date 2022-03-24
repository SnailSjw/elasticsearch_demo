package com.example.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * ES客户端测试
 *
 * @author : snail
 * @date : 2022-03-21 17:18
 **/
public class ESTest_Doc {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

        // 1.插入数据
        // insertData(esClient);

        // 2.修改数据
        // updateData(esClient);

        // 3.查看数据
        // queryData(esClient);

        // 4. 文档删除
        // deleteData(esClient);

        // 5.批量插入数据
        // batchIndexData(esClient);

        // 6.批量删除数据
        // batchDeleteData(esClient);

        // 7.查询索引中全部的数据
        // queryAll(esClient);

        // 8.条件查询
        // queryByCondition(esClient);

        // 9.分页查询
        // queryByPage(esClient);

        // 10.查询排序
        // queryBySort(esClient);

        // 11. 过滤字段
        // queryFilterField(esClient);

        // 12.组合查询
        // queryByComb(esClient);

        // 13.范围查询
        // queryByRange(esClient);

        // 14.模糊查询
        // queryByFuzz(esClient);

        // 15.高亮查询
        // queryByHighLight(esClient);

        // 16.1聚合查询
        // queryByAggregation(esClient);

        // 16.2分组查询
        // queryByAggrterms(esClient);


        // 关闭ES客户端
        esClient.close();
    }

    private static void queryByAggrterms(RestHighLevelClient esClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("user")
                .source(new SearchSourceBuilder()
                        .aggregation(
                                AggregationBuilders.terms("groupAge").field("age")
                        )
                );
        printSomeInfo(esClient, searchRequest);
    }

    private static void queryByAggregation(RestHighLevelClient esClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("user")
                .source(new SearchSourceBuilder()
                        .aggregation(
                                AggregationBuilders.max("maxAge").field("age")
                        )
                );
        printSomeInfo(esClient, searchRequest);
    }

    private static void queryByHighLight(RestHighLevelClient esClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("user")
                .source(new SearchSourceBuilder()
                        .query(
                                QueryBuilders.termsQuery("name","zhangsan")
                        ).highlighter(
                                new HighlightBuilder()
                                .preTags("<font color='red'>")
                                .postTags("</font>")
                                .field("name")
                        )
                );
        printSomeInfo(esClient, searchRequest);
    }

    private static void queryByFuzz(RestHighLevelClient esClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("user")
                .source(new SearchSourceBuilder()
                        .query(
                                QueryBuilders.fuzzyQuery("name","zhansan").fuzziness(Fuzziness.TWO)
                        )
                );
        printSomeInfo(esClient, searchRequest);
    }

    private static void queryByRange(RestHighLevelClient esClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("user")
                .source(new SearchSourceBuilder()
                        .query(
                                QueryBuilders.rangeQuery("age").gte("18").lte("30")
                        )
                );
        printSomeInfo(esClient, searchRequest);
    }

    private static void printSomeInfo(RestHighLevelClient esClient,SearchRequest searchRequest) throws IOException {
        SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
        System.out.println(hits.getTotalHits());
    }

    private static void queryByComb(RestHighLevelClient esClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("user")
                .source(
                        new SearchSourceBuilder()
                        .query(
                                QueryBuilders.boolQuery()
                                .must(QueryBuilders.matchQuery(
                                        "sex","女"
                                ))
                        )
                );
        printSomeInfo(esClient, searchRequest);
    }

    private static void queryFilterField(RestHighLevelClient esClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("user")
                .source(
                        new SearchSourceBuilder()
                        .fetchSource(new String[]{"name","sex"},new String[]{"age"})
                        .query(
                                QueryBuilders.matchAllQuery()
                        )
                        .from(0)
                        .size(2)
                );
        printSomeInfo(esClient, searchRequest);
    }

    private static void queryBySort(RestHighLevelClient esClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("user")
                .source(
                        new SearchSourceBuilder()
                        .query(QueryBuilders.matchAllQuery())
                        .from(0)
                        .size(3)
                        .sort("age", SortOrder.DESC)
                );
        SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
        System.out.println(hits.getTotalHits());
        System.out.println(searchResponse.getTook());
    }

    private static void queryByPage(RestHighLevelClient esClient) throws IOException {
        SearchRequest searchRequest =new SearchRequest();
        searchRequest.indices("user")
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        QueryBuilders.matchAllQuery()
                                )
                                .from(2)
                                .size(2)
                );
        SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
        System.out.println(searchResponse.getTook());
        System.out.println(hits.getTotalHits());
    }

    private static void queryByCondition(RestHighLevelClient esClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("user")
                .source(
                        new SearchSourceBuilder().query(
                                QueryBuilders.termQuery(
                                        "sex","女"
                                )
                        )
                );
        SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        System.out.println(searchResponse.getTook());
        System.out.println(hits.getTotalHits());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void queryAll(RestHighLevelClient esClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("user")
                .source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(searchResponse.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void batchDeleteData(RestHighLevelClient esClient) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new DeleteRequest().index("user").id("1001"));
        bulkRequest.add(new DeleteRequest().index("user").id("1002"));
        bulkRequest.add(new DeleteRequest().index("user").id("1003"));
        BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.getTook());
        System.out.println(bulkResponse.getItems());
    }

    private static void batchIndexData(RestHighLevelClient esClient) throws IOException {
        BulkRequest bulkRequest =new BulkRequest();
        bulkRequest.add(new IndexRequest().index("user").id("1004").source(XContentType.JSON,"name","zhangsan","sex","女","age",50));
        bulkRequest.add(new IndexRequest().index("user").id("1005").source(XContentType.JSON,"name","zhanfei","sex","男","age",20));
        bulkRequest.add(new IndexRequest().index("user").id("1006").source(XContentType.JSON,"name","lifei","sex","女","age",32));

        BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.getTook());
        System.out.println(bulkResponse.getItems());
    }

    private static void deleteData(RestHighLevelClient esClient) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index("user").id("1001");

        DeleteResponse deleteResponse = esClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.getResult());
    }

    private static void queryData(RestHighLevelClient esClient) throws IOException {
        GetRequest getRequest = new GetRequest();
        getRequest.index("user")
                .id("1001");
        GetResponse getResponse = esClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
    }

    private static void updateData(RestHighLevelClient esClient) throws IOException {

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("user")
                .id("1001")
                .doc(XContentType.JSON,"sex","女");

        UpdateResponse updateResponse = esClient.update(updateRequest, RequestOptions.DEFAULT);

        System.out.println(updateResponse.getResult());
    }

    private static void insertData(RestHighLevelClient esClient) throws IOException {

        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index("user").id("1001");
        // 创建数据对象
        User user = new User("张三", "男", 18);
        // 格式化为JSON
        ObjectMapper mapper = new ObjectMapper();
        String userJson = mapper.writeValueAsString(user);
        //添加到request中
        indexRequest.source(userJson, XContentType.JSON);

        IndexResponse response = esClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
    }
}
