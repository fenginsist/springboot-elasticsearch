package com.feng.es;

import com.alibaba.fastjson.JSON;
import com.feng.es.bean.User;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class ElasticsearchRestApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Test
    void contextLoads() {
        System.out.println("111");
    }


    // 测试 索引的创建 request
    @Test
    void  testCreateIndex() throws IOException {
        // 1、创建索引请求  fengIndex 就会报错 ,因为 索引名称必须是小写
        CreateIndexRequest request = new CreateIndexRequest("feng_index");
        // 2、 客户端执行请求 IndicesClient ,请求后获取响应。
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    // 测试 索引是否存在, 这里有问题
    @Test
    void testGetIndex() throws IOException {
        //1.创建索引请求
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices("feng_index");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // 测试 删除索引
    @Test
    void testDeleteIndex() throws IOException {
        //1.创建索引请求
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("feng_index");
        DeleteIndexResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }


    // 测试 添加文档
    @Test
    void testCreateDocument() throws IOException {
        // 创建对象
        User user = new User("冯凡利", 3);
        // 创建请求
        IndexRequest indexRequest = new IndexRequest("feng_index");
        // 规则 put /feng_index/_doc/1
        indexRequest.id("1");
        indexRequest.type("_doc");
        indexRequest.timeout(TimeValue.timeValueSeconds(3000));
        indexRequest.timeout("1s");

        // 数据放入请求  json
        indexRequest.source(JSON.toJSONString(user), XContentType.JSON);

        // 客户端发送请求，获取响应的结果
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.toString()); //  IndexResponse[index=feng_index,type=_doc,id=1,version=2,result=updated,seqNo=1,primaryTerm=2,shards={"total":2,"successful":1,"failed":0}]
        System.out.println(indexResponse.status()); // 对应命令返回的状态: OK
    }

    // 获取文档， 判断是否存在 get /index/doc/1
    @Test
    public void testIsExists() throws IOException {
        GetRequest getRequest = new GetRequest("feng_index","_doc", "1");
        // 不获取返回的 _source 的上下文
//        getRequest.fetchSourceContext(new FetchSourceContext(false));
//        getRequest.storedFields("_none_");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // 获取文档信息
    @Test
    public void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("feng_index","_doc", "1");
        GetResponse documentFields = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(documentFields.getIndex()); // feng_index
        System.out.println(documentFields.getId());    // 1
        System.out.println(documentFields.getSourceAsMap());  // {name=冯凡利, age=3}
        System.out.println(documentFields.getSourceAsString()); //  打印文档的内容 {"age":3,"name":"冯凡利"}
        System.out.println(documentFields);// 返回的全部内容和命令式一样的  {"_index":"feng_index","_type":"_doc","_id":"1","_version":2,"found":true,"_source":{"age":3,"name":"冯凡利"}}
    }


    // 更新 文档信息
    @Test
    public void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("feng_index", "_doc", "1");
        updateRequest.timeout("3000S");

        User user = new User("冯凡利java", 18);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update.getGetResult());
        System.out.println(update.status());
    }

    // 删除 文档信息
    @Test
    public void testDeleteDocument() throws IOException {

        DeleteRequest deleteRequest = new DeleteRequest("feng_index", "_doc", "1");
        deleteRequest.timeout("3000s");

        DeleteResponse delete = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete.status()); // ok , 去head中查看，没有文档记录了
    }

    // 特殊， 真的项目一般都会批量插入数据！！
    @Test
    public void testBulkDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> userArrayList = new ArrayList<User>();
        userArrayList.add(new User("fengfanli1", 12));
        userArrayList.add(new User("fengfanli2", 12));
        userArrayList.add(new User("fengfanli3", 12));
        userArrayList.add(new User("fengfanli4", 12));

//        批处理请求
        for (int i = 0; i< userArrayList.size(); i++){
//            批量更新和批量删除，就在这里修改对应的请求就可以了
            bulkRequest.add(new IndexRequest("feng_index")
                    .type("_doc")
                    .id(""+(i+1))
                    .source(JSON.toJSONString(userArrayList.get(i)), XContentType.JSON));
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());// 是否失败， 返回false，代表成功
    }

    /*
    * 查询
    *   SearchRequest： 搜索请求
    *   SearchSourceBuilder： 条件构造
    *   HighlightBuilder：   构建高亮
    *   TermsQueryBuilder：  精确查询
    *   MatchAllQueryBuilder：匹配全部
    *   XXX  QueryBuilder：
    * */
    @Test
    public void testSearch() throws IOException{
        SearchRequest searchRequest = new SearchRequest("feng_index");
        // 构建搜索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        searchSourceBuilder.highlighter(highlightBuilder);

        // 查询条件，我们可以使用 QueryBuilders 工具类来实现
        // QueryBuilders.termsQuery() 精确
        // QueryBuilders.matchAllQuery() 匹配所有
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("name", "fengfanli1");
        // MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(termsQueryBuilder);
        // 构建 分页信息
        // searchSourceBuilder.from();
        // searchSourceBuilder.size();
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        // 打印：{"fragment":true,"hits":[{"fields":{},"fragment":false,"highlightFields":{},"id":"1","matchedQueries":[],"score":1.0,"sortValues":[],"sourceAsMap":{"name":"fengfanli1","age":12},"sourceAsString":"{\"age\":12,\"name\":\"fengfanli1\"}","sourceRef":{"childResources":[],"fragment":true},"type":"_doc","version":-1}],"maxScore":1.0,"totalHits":1}
        System.out.println("==================================");
        for (SearchHit documentFields : searchResponse.getHits().getHits()){
            System.out.println(documentFields.getSourceAsMap()); //{name=fengfanli1, age=12}
        }
    }














}
