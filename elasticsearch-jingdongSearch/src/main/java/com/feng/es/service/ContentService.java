package com.feng.es.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ContentService {

    // 解析 关键词
    Boolean parseContent(String keywords) throws IOException;

    // 搜索
    List<Map<String,Object>> searchPage(String keyword, int pageNo, int pageSize);

    // 搜索并高亮
    List<Map<String,Object>> searchPageHighlight(String keyword, int pageNo, int pageSize);
}
