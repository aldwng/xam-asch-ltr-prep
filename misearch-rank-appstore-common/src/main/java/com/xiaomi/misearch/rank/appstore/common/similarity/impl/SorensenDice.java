package com.xiaomi.misearch.rank.appstore.common.similarity.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.xiaomi.misearch.rank.appstore.common.similarity.core.ShingleBased;
import com.xiaomi.misearch.rank.appstore.common.similarity.StringSimilarity;

/**
 * Created by wangjie5 on 2018/1/17.
 */
public class SorensenDice extends ShingleBased implements StringSimilarity {
    /**
     * Sorensen-Dice coefficient, aka Sørensen index, Dice's coefficient or
     * Czekanowski's binary (non-quantitative) index.
     *
     * The strings are first converted to boolean sets of k-shingles (sequences
     * of k characters), then the similarity is computed as 2 * |A inter B| /
     * (|A| + |B|). Attention: Sorensen-Dice distance (and similarity) does not
     * satisfy triangle inequality.
     *
     * @param k
     */
    public SorensenDice(final int k) {
        super(k);
    }

    /**
     * Sorensen-Dice coefficient, aka Sørensen index, Dice's coefficient or
     * Czekanowski's binary (non-quantitative) index.
     *
     * The strings are first converted to boolean sets of k-shingles (sequences
     * of k characters), then the similarity is computed as 2 * |A inter B| /
     * (|A| + |B|). Attention: Sorensen-Dice distance (and similarity) does not
     * satisfy triangle inequality. Default k is 3.
     */
    public SorensenDice() {
        super();
    }

    /**
     * Similarity is computed as 2 * |A inter B| / (|A| + |B|).
     *
     * @param s1 The first string to compare.
     * @param s2 The second string to compare.
     * @return The computed Sorensen-Dice similarity.
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

        Set<String> union = new HashSet<String>();
        union.addAll(profile1.keySet());
        union.addAll(profile2.keySet());

        int inter = 0;

        for (String key : union) {
            if (profile1.containsKey(key) && profile2.containsKey(key)) {
                inter++;
            }
        }

        return 2.0 * inter / (profile1.size() + profile2.size());
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

        int inter = s1.size() + s2.size() - union.size();

        return 2.0 * inter / (s1.size() + s2.size());
    }
}
