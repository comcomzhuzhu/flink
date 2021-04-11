package com.atguigu.transform;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @ClassName Filter_Class
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/11 21:37
 * @Version 1.0
 */
public class Filter_Class {
    public static void main(String[] args) {

        new KeyWordFilter("flink");
    }

    public static class KeyWordFilter implements FilterFunction<String> {
        private String keyword;

        KeyWordFilter(String keyword) {
            this.keyword = keyword;
        }

        @Override
        public boolean filter(String value) {
            return value.contains(keyword);
        }
    }
}
