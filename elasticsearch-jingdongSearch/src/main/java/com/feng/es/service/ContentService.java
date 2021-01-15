package com.feng.es.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ContentService {

    Boolean parseContent(String keywords) throws IOException;

    List<Map<String,Object>> searchPage(String keyword, int pageNo, int pageSize);

    List<Map<String,Object>> searchPageHighlight(String keyword, int pageNo, int pageSize);
}
