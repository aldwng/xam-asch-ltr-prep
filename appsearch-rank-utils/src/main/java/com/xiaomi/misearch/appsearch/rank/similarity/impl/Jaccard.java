package com.xiaomi.misearch.appsearch.rank.similarity.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.xiaomi.misearch.appsearch.rank.similarity.core.ShingleBased;
import com.xiaomi.misearch.appsearch.rank.similarity.StringSimilarity;

/**
 * Created by wangjie5 on 2017/8/3.
 */
public class Jaccard extends ShingleBased implements StringSimilarity {
    /**
     * The strings are first transformed into sets of k-shingles (sequences of k
     * characters), then Jaccard index is computed as |A inter B| / |A union B|.
     * The default value of k is 3.
     *
     * @param k
     */
    public Jaccard(final int k) {
        super(k);
    }

    /**
     * The strings are first transformed into sets of k-shingles (sequences of k
     * characters), then Jaccard index is computed as |A inter B| / |A union B|.
     * The default value of k is 3.
     */
    public Jaccard() {
        super();
    }

    /**
     * Compute Jaccard index: |A inter B| / |A union B|.
     *
     * @param s1 The first string to compare.
     * @param s2 The second string to compare.
     * @return The Jaccard index in the range [0, 1]
     * @throws NullPointerException if s1 or s2 is null.
     */
    public final double similarity(final String s1, final String s2) {
        if (s1 == null) {
            throw new NullPointerException("s1 must not be null");
        }

        if (s2 == null) {
            throw new NullPointerException("s2 must not be null");
        }

        if (s1.equals(s2)) {
            return 1;
        }

        Map<String, Integer> profile1 = getProfile(s1);
        Map<String, Integer> profile2 = getProfile(s2);

        Set<String> union = new HashSet<>();
        union.addAll(profile1.keySet());
        union.addAll(profile2.keySet());

        int size1 = profile1.keySet().size();
        int size2 = profile2.keySet().size();

        int inter = size1 + size2 - union.size();

        return 1.0 * inter / union.size();
    }

    public final double similarity(List<String> s1, List<String> s2) {
        if (s1 == null || s1.isEmpty()) {
            throw new NullPointerException("s1 must not be null or empty");
        }

        if (s2 == null || s2.isEmpty()) {
            throw new NullPointerException("s2 must not be null or empty");
        }

        Set<String> union = new HashSet<>();
        union.addAll(s1);
        union.addAll(s2);

        int size1 = s1.size();
        int size2 = s2.size();

        int inter = size1 + size2 - union.size();

        return 1.0 * inter / union.size();
    }

    public final double similarity(final Map<Integer, Double> m1, final Map<Integer, Double> m2) {
        double threshold = 0.00001;
        if (m1 == null) {
            throw new NullPointerException("m1 must not be null");
        }

        if (m2 == null) {
            throw new NullPointerException("m2 must not be null");
        }

        if (m1.size() == 0 || m2.size() == 0) {
            return 0;
        }

        Set<Integer> inter = new HashSet<>();
        inter.addAll(m1.keySet());
        inter.retainAll(m2.keySet());

        double interScore = 0.0;
        for (Integer id : inter) {
            interScore += Math.min(m1.get(id), m2.get(id));
        }

        Set<Integer> union = new HashSet<>();
        union.addAll(m1.keySet());
        union.addAll(m2.keySet());

        double unionScore = 0.0;
        for (Integer id : union) {
            unionScore += Math.max(m1.getOrDefault(id, 0d), m2.getOrDefault(id, 0d));
        }

        return interScore / (unionScore + threshold);
    }
}
