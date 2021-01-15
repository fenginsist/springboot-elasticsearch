package com.feng.es.controller;

import com.feng.es.service.ContentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Controller
public class ContentController {

    @Autowired
    private ContentService contentService;


    /**
     * 往 es 中添加数据
     * @param keyword
     * @return
     * @throws IOException
     */
    @ResponseBody
    @GetMapping("/parse/{keyword}")
    public Boolean parse(@PathVariable("keyword") String keyword) throws IOException {
        return contentService.parseContent(keyword);
    }

    /**
     * 检索
     * @param keyword
     * @param pageNo
     * @param pageSize
     * @return
     */
    @ResponseBody
    @GetMapping("/search/{keyword}/{pageNo}/{pageSize}")
    public List<Map<String, Object>> search(@PathVariable("keyword") String keyword,
                                            @PathVariable("pageNo") Integer pageNo,
                                            @PathVariable("pageSize") Integer pageSize){
        return contentService.searchPage(keyword, pageNo, pageSize);
    }

    /**
     * 检索高亮
     * @param keyword
     * @param pageNo
     * @param pageSize
     * @return
     */
    @ResponseBody
    @GetMapping("/searchHight/{keyword}/{pageNo}/{pageSize}")
    public List<Map<String, Object>> searcHighlight(@PathVariable("keyword") String keyword,
                                            @PathVariable("pageNo") Integer pageNo,
                                            @PathVariable("pageSize") Integer pageSize){
        return contentService.searchPageHighlight(keyword, pageNo, pageSize);
    }
}
