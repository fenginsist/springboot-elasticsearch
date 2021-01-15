package com.feng.es.utils;

import com.feng.es.bean.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class HtmlParseUtil {

    /**
     * @Author fengfanli
     * @Description //TODO 测试方法
     * @Date 14:39 2021/1/15
     * @Param [args]
     * @return void
     **/
    public static void main(String[] args) throws IOException {
        HtmlParseUtil.parseJD("京东超市").forEach(System.out::println);
    }
    
    /**
     * @Author fengfanli
     * @Description //TODO 详细步骤
     * @Date 14:38 2021/1/15
     * @Param [args]
     * @return void
     **/
    public static void main1(String[] args) throws IOException {
        // 获取请求 https://search.jd.com/Search?keyword=java
        // 前提，需要联网
        String url = "https://search.jd.com/Search?keyword=java";
        // 解析网页 （jsoup 返回的 Document 就是浏览器 Document）
        Document document = Jsoup.parse(new URL(url), 30000);
        // 所有在js中可以使用的方法，这里都能用！
        Element element = document.getElementById("J_goodsList"); // 这是包裹所有商品列表的 div

        System.out.println(element.html());

        // 获取所有的 li 元素
        Elements elements = element.getElementsByTag("li");
        // 获取元素中的内容，这里的 el 就是每一个 li 标签了
        for (Element el : elements){
            // 关于这种图片特别多的网站，所有的图片都是延迟加载的
            // source-data-lazy-img->data-lazy-img
            String img = el.getElementsByTag("img").eq(0).attr("data-lazy-img");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String title = el.getElementsByClass("p-name").eq(0).text();

            System.out.println("============================");
            System.out.println(img);
            System.out.println(price);
            System.out.println(title);
        }
    }

    /**
     * @Author fengfanli
     * @Description //TODO 解析京东数据静态方法
     * @Date 14:39 2021/1/15
     * @Param [keywords]
     * @return java.util.List<com.feng.es.bean.Content>
     **/
    public static List<Content> parseJD(String keywords) throws IOException {
        String url = "https://search.jd.com/Search?keyword="+ keywords;
        Document document = Jsoup.parse(new URL(url), 30000);
        Element element = document.getElementById("J_goodsList"); // 这是包裹所有商品列表的 div

        Elements elements = element.getElementsByTag("li");
        List<Content> goodslist = new ArrayList<>();
        for (Element el : elements){
            String img = el.getElementsByTag("img").eq(0).attr("data-lazy-img");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String title = el.getElementsByClass("p-name").eq(0).text();
            Content content = new Content(img, price, title);
            goodslist.add(content);
        }
        return goodslist;
    }
}
