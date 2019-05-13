package com.xiaomi.misearch.appsearch.rank.utils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.xiaomi.misearch.appsearch.rank.model.Feature;
import com.xiaomi.misearch.appsearch.rank.model.RankSample;

/**
 * @author Shenglan Wang
 */
public class SampleUtils {

  public static String convertToText(RankSample sample) {
    String appId = sample.getApp().getRawId();
    int label = sample.getLabel();
    int qid = sample.getQid();
    List<Feature> features = sample.getFeatures();

    List<String> featureTextList = features.stream().sorted(
        Comparator.comparingInt(o -> o.getName().getId())).map(f -> {
      int fid = f.getName().getId();
      double value = f.getValue();
      return String.format("%d:%.4f", fid, value);
    }).collect(Collectors.toList());
    String featureText = StringUtils.join(featureTextList, " ");
    return String.format("%d qid:%s %s # %s", label, qid, featureText, appId);
  }
}
