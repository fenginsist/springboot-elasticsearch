package com.feng.es.service.impl;

import com.alibaba.fastjson.JSON;
import com.feng.es.bean.Content;
import com.feng.es.service.ContentService;
import com.feng.es.utils.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class ContentServiceImpl implements ContentService {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Override
    public Boolean parseContent(String keywords) throws IOException {
        List<Content> contents = HtmlParseUtil.parseJD(keywords);
        // 把查询的数据放入到 es 中
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("2m");

        for (int i = 0; i < contents.size(); i++){
            bulkRequest.add(new IndexRequest("goods_index")
                    .type("_doc")
                    .source(JSON.toJSONString(contents.get(i)), XContentType.JSON));
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        return !bulk.hasFailures();  //bulk.hasFailures(): 返回false，代表成功
    }

    /**
     * 获取这些数据 实现搜索功能
     * @param keyword
     * @param pageNo
     * @param pageSize
     * @return
     */
    @Override
    public List<Map<String, Object>> searchPage(String keyword, int pageNo, int pageSize) {
        if (pageNo<=1){
            pageNo=1;
        }
        // 条件搜索
        SearchRequest searchRequest = new SearchRequest("goods_index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 分页
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);

        // 精准匹配
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("title", keyword);
        searchSourceBuilder.query(termsQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        // 执行搜索
        searchRequest.source(searchSourceBuilder);
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            // 解析结果
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit documentFields : hits){
                list.add(documentFields.getSourceAsMap());
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e.getLocalizedMessage());
        }
        return list;
    }

    /**
     * 获取这些数据 实现搜索功能
     * @param keyword
     * @param pageNo
     * @param pageSize
     * @return
     */
    @Override
    public List<Map<String, Object>> searchPageHighlight(String keyword, int pageNo, int pageSize) {
        if (pageNo<=1){
            pageNo=1;
        }
        // 条件搜索
        SearchRequest searchRequest = new SearchRequest("goods_index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 分页
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);

        // 精准匹配
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("title", keyword);
        searchSourceBuilder.query(termsQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        // 高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.requireFieldMatch(false); // 多个高亮显示！
        highlightBuilder.preTags("<span style= 'color:red'>");
        highlightBuilder.postTags("</span>");
        searchSourceBuilder.highlighter(highlightBuilder);

        // 执行搜索
        searchRequest.source(searchSourceBuilder);
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            // 解析结果
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits){
                Map<String, HighlightField> highlightFields = hit.getHighlightFields(); // 获取高亮字段
                HighlightField title = highlightFields.get("title");
                Map<String, Object> sourceAsMap = hit.getSourceAsMap(); // 获取结果集
                // 解析高亮的字段，将原来的字段替换为我们高亮的字段即可！
                if (title != null){
                    // 如果高亮字段存在
                    Text[] fragments = title.fragments(); // 取出高亮字段
                    String new_title = "";                // 新高亮标题
                    for (Text text : fragments){
                        new_title += text;
                    }
                    sourceAsMap.put("title", new_title);
                }
                list.add(sourceAsMap);
            }
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
        }
        return list;
    }
}
