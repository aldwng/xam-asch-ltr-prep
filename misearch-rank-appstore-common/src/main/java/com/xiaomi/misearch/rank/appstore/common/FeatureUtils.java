package com.xiaomi.misearch.rank.appstore.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.lang.StringUtils;

import com.xiaomi.misearch.rank.appstore.common.model.Feature;
import com.xiaomi.misearch.rank.appstore.common.similarity.impl.Jaccard;
import com.xiaomi.misearch.rank.appstore.common.similarity.impl.SorensenDice;
import com.xiaomi.misearch.rank.appstore.common.model.App;
import com.xiaomi.misearch.rank.appstore.common.model.FeatureName;

/**
 * @author Shenglan Wang
 */
public class FeatureUtils {

  public static List<Feature> extractFeatures(App app, String query) {
    query = StringUtils.lowerCase(query);
    Set<String> querySeg = getWordSegment(query);

    String displayName = StringUtils.lowerCase(app.getDisplayName());
    Set<String> displayNameSeg = getWordSegment(displayName);

    String brief = StringUtils.lowerCase(app.getBrief());
    Set<String> briefSeg = getWordSegment(brief);

    String level1Cate = StringUtils.lowerCase(app.getLevel1Category());
    Set<String> level1CateSeg = getWordSegment(level1Cate);

    String level2Cate = StringUtils.lowerCase(app.getLevel2Category());
    Set<String> level2CateSeg = getWordSegment(level2Cate);

    List<Feature> features = new ArrayList<>();
    features.add(new Feature(FeatureName.app_active_rank, app.getAppActiveRank()));
    features.add(new Feature(FeatureName.app_download_rank, app.getAppDownloadRank()));
    features.add(new Feature(FeatureName.app_cdr, app.getAppCdr()));
    features.add(new Feature(FeatureName.app_rank, app.getAppRank()));
    features.add(new Feature(FeatureName.app_dn_len, StringUtils.isEmpty(displayName) ? 0f : displayName.length()));
    features.add(new Feature(FeatureName.app_dn_chinese_char_ratio, calcChineseRatio(displayName)));
    features.add(new Feature(FeatureName.app_dn_english_char_ratio, calcEnglishRatio(displayName)));

    features.add(new Feature(FeatureName.query_len, StringUtils.isEmpty(query) ? 0f : query.length()));
    features.add(new Feature(FeatureName.query_chinese_char_ratio, calcChineseRatio(query)));
    features.add(new Feature(FeatureName.query_english_char_ratio, calcEnglishRatio(query)));

    if (StringUtils.equals(query, displayName)) {
      features.add(new Feature(FeatureName.query_app_dn_match, 1.0f));
    } else {
      features.add(new Feature(FeatureName.query_app_dn_match, 0f));
    }

    if (StringUtils.contains(query, displayName) || StringUtils.contains(displayName, query)) {
      features.add(new Feature(FeatureName.query_app_dn_contains, 1.0f));
    } else {
      features.add(new Feature(FeatureName.query_app_dn_contains, 0f));
    }

    features.add(new Feature(FeatureName.query_app_dn_bi_jac, calcJaccardScore(2, query, displayName)));
    features.add(new Feature(FeatureName.query_app_dn_tri_jac, calcJaccardScore(3, query, displayName)));
    features.add(new Feature(FeatureName.query_app_dn_seg_jac, calcJaccardScore(querySeg, displayNameSeg)));
    features.add(new Feature(FeatureName.query_app_dn_bi_dice, calcDiceScore(2, query, displayName)));
    features.add(new Feature(FeatureName.query_app_dn_tri_dice, calcDiceScore(3, query, displayName)));
    features.add(new Feature(FeatureName.query_app_dn_seg_dice, calcDiceScore(querySeg, displayNameSeg)));

    features.add(new Feature(FeatureName.query_app_brief_bi_jac, calcJaccardScore(2, query, brief)));
    features.add(new Feature(FeatureName.query_app_brief_tri_jac, calcJaccardScore(3, query, brief)));
    features.add(new Feature(FeatureName.query_app_brief_seg_jac, calcJaccardScore(querySeg, briefSeg)));
    features.add(new Feature(FeatureName.query_app_brief_bi_dice, calcDiceScore(2, query, brief)));
    features.add(new Feature(FeatureName.query_app_brief_tri_dice, calcDiceScore(3, query, brief)));
    features.add(new Feature(FeatureName.query_app_brief_seg_dice, calcDiceScore(querySeg, briefSeg)));

    if (StringUtils.contains(query, level1Cate)) {
      features.add(new Feature(FeatureName.query_app_level1_cate_contains, 1.0f));
    } else {
      features.add(new Feature(FeatureName.query_app_level1_cate_contains, 0f));
    }

    features.add(new Feature(FeatureName.query_app_level1_cate_bi_jac, calcJaccardScore(2, query, level1Cate)));
    features.add(new Feature(FeatureName.query_app_level1_cate_tri_jac, calcJaccardScore(3, query, level1Cate)));
    features.add(new Feature(FeatureName.query_app_level1_cate_seg_jac, calcJaccardScore(querySeg, level1CateSeg)));
    features.add(new Feature(FeatureName.query_app_level1_cate_bi_dice, calcDiceScore(2, query, level1Cate)));
    features.add(new Feature(FeatureName.query_app_level1_cate_tri_dice, calcDiceScore(3, query, level1Cate)));
    features.add(new Feature(FeatureName.query_app_level1_cate_seg_dice, calcDiceScore(querySeg, level1CateSeg)));

    if (StringUtils.contains(query, level2Cate)) {
      features.add(new Feature(FeatureName.query_app_level2_cate_contains, 1.0f));
    } else {
      features.add(new Feature(FeatureName.query_app_level2_cate_contains, 0f));
    }

    features.add(new Feature(FeatureName.query_app_level2_cate_bi_jac, calcJaccardScore(2, query, level2Cate)));
    features.add(new Feature(FeatureName.query_app_level2_cate_tri_jac, calcJaccardScore(3, query, level2Cate)));
    features.add(new Feature(FeatureName.query_app_level2_cate_seg_jac, calcJaccardScore(querySeg, level2CateSeg)));
    features.add(new Feature(FeatureName.query_app_level2_cate_bi_dice, calcDiceScore(2, query, level2Cate)));
    features.add(new Feature(FeatureName.query_app_level2_cate_tri_dice, calcDiceScore(3, query, level2Cate)));
    features.add(new Feature(FeatureName.query_app_level2_cate_seg_dice, calcDiceScore(querySeg, level2CateSeg)));

    List<String> keywords = app.getKeywords();
    features.add(new Feature(FeatureName.query_app_keywords_bi_jac_sum, calcJaccardScoreSum(2, query, keywords)));
    features.add(new Feature(FeatureName.query_app_keywords_tri_jac_sum, calcJaccardScoreSum(3, query, keywords)));
    features.add(new Feature(FeatureName.query_app_keywords_bi_dice_sum, calcDiceScoreSum(2, query, keywords)));
    features.add(new Feature(FeatureName.query_app_keywords_tri_dice_sum, calcDiceScoreSum(3, query, keywords)));
    features.add(new Feature(FeatureName.query_app_keywords_overlap, calcTagOverlapRatio(query, keywords)));

    Set<String> pnSeg = SetUtils.EMPTY_SET;
    if (StringUtils.isNotEmpty(app.getPackageName())) {
      pnSeg = new HashSet<>(Arrays.asList(app.getPackageName().split("\\.")));
    }
    features.add(new Feature(FeatureName.query_app_pn_seg_jac, calcJaccardScore(querySeg, pnSeg)));
    features.add(new Feature(FeatureName.query_app_pn_seg_dice, calcDiceScore(querySeg, pnSeg)));
    return features;
  }

  private static Set<String> getWordSegment(String text) {
    if (StringUtils.isEmpty(text)) {
      return SetUtils.EMPTY_SET;
    }
    List<Term> terms = ToAnalysis.parse(text).getTerms();
    List<String> result = terms.stream().map(Term::getName).collect(Collectors.toList());
    return new HashSet<>(result);
  }

  private static double calcJaccardScore(int num, String text1, String text2) {
    if (StringUtils.isEmpty(text1) || StringUtils.isEmpty(text2)) {
      return 0f;
    }
    if (text1.length() >= num || text2.length() >= num) {
      Jaccard jaccard = new Jaccard(num);
      return jaccard.similarity(text1, text2);
    } else {
      return 0f;
    }
  }

  private static double calcJaccardScore(Set<String> set1, Set<String> set2) {
    if (set1.size() > 0 && set2.size() > 0) {
      Jaccard jaccard = new Jaccard();
      return jaccard.similarity(new ArrayList<>(set1), new ArrayList<>(set2));
    } else {
      return 0f;
    }
  }

  private static double calcDiceScore(int num, String text1, String text2) {
    if (StringUtils.isEmpty(text1) || StringUtils.isEmpty(text2)) {
      return 0f;
    }
    if (text1.length() >= num || text2.length() >= num) {
      SorensenDice dice = new SorensenDice(num);
      return dice.similarity(text1, text2);
    } else {
      return 0f;
    }
  }

  private static double calcDiceScore(Set<String> set1, Set<String> set2) {
    if (set1.size() > 0 && set2.size() > 0) {
      SorensenDice dice = new SorensenDice();
      return dice.similarity(new ArrayList<>(set1), new ArrayList<>(set2));
    } else {
      return 0f;
    }
  }


  private static double calcTagOverlapRatio(String text, List<String> tags) {
    if (CollectionUtils.isEmpty(tags) || StringUtils.isEmpty(text)) {
      return 0f;
    }

    double overlapCount = tags.stream().mapToDouble(tag -> {
      if (text.contains(tag)) {
        return 1.0f;
      } else {
        return 0f;
      }
    }).sum();
    return overlapCount / tags.size();
  }

  private static double calcJaccardScoreSum(int num, String text, List<String> tags) {
    if (CollectionUtils.isEmpty(tags) || StringUtils.isEmpty(text)) {
      return 0f;
    }
    return tags.stream().mapToDouble(tag -> calcJaccardScore(num, text, tag)).sum();
  }

  private static double calcDiceScoreSum(int num, String text, List<String> tags) {
    if (CollectionUtils.isEmpty(tags) || StringUtils.isEmpty(text)) {
      return 0f;
    }
    return tags.stream().mapToDouble(tag -> calcDiceScore(num, text, tag)).sum();
  }

  private static double calcEnglishRatio(String str) {
    if (StringUtils.isEmpty(str)) {
      return 0f;
    }

    int i = 0;
    for (char c : str.toCharArray()) {
      if (isEnglish(c)) {
        i++;
      }
    }
    return i * 1.0 / str.length();
  }

  private static boolean isEnglish(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
  }

  private static double calcChineseRatio(String str) {
    if (StringUtils.isEmpty(str)) {
      return 0f;
    }

    int i = 0;
    for (char c : str.toCharArray()) {
      if (isChinese(c)) {
        i++;
      }
    }
    return i * 1.0 / str.length();
  }

  private static boolean isChinese(char c) {
    return c >= 0x4E00 && c <= 0x9FA5;// 根据字节码判断
  }
}
