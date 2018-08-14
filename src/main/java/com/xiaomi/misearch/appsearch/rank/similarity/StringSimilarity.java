package com.xiaomi.misearch.appsearch.rank.similarity;

import java.io.Serializable;
import java.util.List;

/**
 * Created by wangjie5 on 2017/8/3.
 */
public interface StringSimilarity extends Serializable {
    /**
     * Compute and return a measure of similarity between 2 strings.
     * @param s1
     * @param s2
     * @return similarity (0 means both strings are completely different)
     */
    double similarity(String s1, String s2);

    double similarity(List<String> s1, List<String> s2);
}
