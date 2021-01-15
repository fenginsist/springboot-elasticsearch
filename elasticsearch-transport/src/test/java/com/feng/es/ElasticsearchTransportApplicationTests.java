package com.feng.es;

import com.alibaba.fastjson.JSONObject;
import com.feng.es.bean.EsPage;
import com.feng.es.utils.ElasticSearchUtil;
import org.apache.http.client.utils.DateUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

@SpringBootTest
class ElasticsearchTransportApplicationTests {

    /**
     * 测试索引
     */
    private String indexName = "test_index";

    /**
     * 类型
     */
    private String esType = "external";

    @Test
    void contextLoads() {
        System.out.println("111");
    }

    /**
     * 创建索引
     */
    @Test
    void test01CreateIndex() {
        if (!ElasticSearchUtil.isIndexExist(indexName)) {
            ElasticSearchUtil.createIndex(indexName);
        } else {
            System.out.println("索引已经存在");
        }
        System.out.println("索引创建成功");
    }

    /**
     * 插入记录
     */
    @Test
    void test02InsertJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", DateUtils.formatDate(new Date()));
        jsonObject.put("age", 25);
        jsonObject.put("name", "j-" + new Random(100).nextInt());
        jsonObject.put("date", new Date());
        String id = ElasticSearchUtil.addData(jsonObject, indexName, esType, jsonObject.getString("id"));
        System.out.println("id:" + id);
    }

    /**
     * 删除 记录
     */
    @Test
    void test03Delete() {
        String id = "Fri, 15 Jan 2021 06:17:50 GMT";
        if (StringUtils.isNotBlank(id)) {
            ElasticSearchUtil.deleteDataById(indexName, esType, id);
            System.out.println("删除id=" + id);
        } else {
            System.out.println("id为空");
        }
    }

    /**
     * 更新 记录
     */
    @Test
    void test04Update() {
        String id = "Fri, 15 Jan 2021 06:17:50 GMT";
        if (StringUtils.isNotBlank(id)) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id", id);
            jsonObject.put("age", 31);
            jsonObject.put("name", "修改");
            jsonObject.put("date", new Date());
            ElasticSearchUtil.updateDataById(jsonObject, indexName, esType, id);
            System.out.println("id=" + id);
        } else {
            System.out.println("id为空");
        }
    }

    /**
     * 获取数据
     */
    @Test
    void test05GetData() {
        String id = "Fri, 15 Jan 2021 05:53:33 GMT";
        if (StringUtils.isNotBlank(id)) {
            Map<String, Object> map = ElasticSearchUtil.searchDataById(indexName, esType, id, null);
            System.out.println(JSONObject.toJSONString(map));
        } else {
            System.out.println("id为空");
        }
    }

    /**
     * 查询数据
     */
    @Test
    void test06QueryMatchData() {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolean matchPhrase = false;
        if (matchPhrase == Boolean.TRUE) {
            boolQuery.must(QueryBuilders.matchPhraseQuery("name", "修"));
        } else {
            boolQuery.must(QueryBuilders.matchQuery("name", "修"));
        }
        List<Map<String, Object>> list = ElasticSearchUtil.searchListData(indexName, esType, boolQuery, 10, null, null, null);
        System.out.println(JSONObject.toJSONString(list));
    }

    /**
     * 通配符查询数据
     * 通配符查询 ? 用来匹配1个任意字符，* 用来匹配零个或者多个字符
     * 0 条数据
     */
    @Test
    void test07QueryWildcardData() {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("name.keyword", "j-*466");
        List<Map<String, Object>> list = ElasticSearchUtil.searchListData(indexName, esType, queryBuilder, 10, null, null, null);
        System.out.println(JSONObject.toJSONString(list));
    }

    /**
     * 正则查询
     * 0 条数据
     */
    @Test
    void test08QueryRegexpData() {
        QueryBuilder queryBuilder = QueryBuilders.regexpQuery("name.keyword", "j--[0-9]{1,11}");
        List<Map<String, Object>> list = ElasticSearchUtil.searchListData(indexName, esType, queryBuilder, 10, null, null, null);
        System.out.println(JSONObject.toJSONString(list));
    }

    /**
     * 查询数字范围数据
     */
    @Test
    void test09QueryIntRangeData() {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.rangeQuery("age").from(21).to(100));
        List<Map<String, Object>> list = ElasticSearchUtil.searchListData(indexName, esType, boolQuery, 10, null, null, null);
        System.out.println(JSONObject.toJSONString(list));
    }

    /**
     * 查询日期范围数据
     */
    @Test
    void test10QueryDateRangeData() {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.rangeQuery("date").from("2018-04-25T08:33:44.840Z").to("2021-04-25T10:03:08.081Z"));
        List<Map<String, Object>> list = ElasticSearchUtil.searchListData(indexName, esType, boolQuery, 10, null, null, null);
        System.out.println(JSONObject.toJSONString(list));
    }

    /**
     * 查询分页
     * startPage   第几条记录开始 测试 1
     * pageSize    每页大小 测试 10
     *
     * @return
     */
    @Test
    void test11QueryPage() {
        int startPage = 1;
        int pageSize = 10;
        if (StringUtils.isNotBlank(String.valueOf(startPage)) && StringUtils.isNotBlank(String.valueOf(pageSize))) {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            boolQuery.must(QueryBuilders.rangeQuery("date").from("2018-04-25T08:33:44.840Z")
                    .to("2021-09-25T10:03:08.081Z"));
            EsPage list = ElasticSearchUtil.searchDataPage(indexName, esType, Integer.parseInt(startPage + ""), Integer.parseInt(pageSize + ""), boolQuery, null, null, null);
            System.out.println(JSONObject.toJSONString(list));
        } else {
            System.out.println("startPage或者pageSize缺失");
        }
    }
}
