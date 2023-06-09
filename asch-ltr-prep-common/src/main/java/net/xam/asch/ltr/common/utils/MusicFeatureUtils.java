package net.xam.asch.ltr.common.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import net.xam.asch.ltr.common.model.Feature;
import net.xam.asch.ltr.common.model.FeatureName;
import net.xam.asch.ltr.common.model.MusicItem;
import net.xam.asch.ltr.common.model.RankSample;


public class MusicFeatureUtils {

  private static final String TAGS_ONE_HOT_ = "tag_";

  public static List<Feature> extractFeatures(MusicItem musicItem, Map<String, String> tagIndexMap) {
    List<Feature> features = new ArrayList<>();
    int featureIdx = 1;
    features.add(new Feature(FeatureName.META_RANK.getName(), featureIdx++, musicItem.getMetaRank()));
    features.add(new Feature(FeatureName.RES_RANK.getName(), featureIdx++, musicItem.getResRank()));
    features.add(new Feature(FeatureName.QUALITY.getName(), featureIdx++, musicItem.getQuality()));
    features.add(new Feature(FeatureName.QQ_SONG_RAW_RANK.getName(), featureIdx++, musicItem.getQqSongRawRank()));
    features.add(new Feature(FeatureName.QQ_ARTIST_RAW_RANK.getName(), featureIdx++, musicItem.getQqArtistRawRank()));
    features.add(new Feature(FeatureName.QQ_RANK.getName(), featureIdx++, musicItem.getQqRank()));
    features.add(new Feature(FeatureName.HAS_LYRIC.getName(), featureIdx++, musicItem.getHasLyric()));
    features.add(
        new Feature(FeatureName.SONG_SEARCH_PLAY_COUNT.getName(), featureIdx++, musicItem.getSongSearchPlayCount()));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_PLAY_COUNT.getName(), featureIdx++,
                             musicItem.getSongArtistSearchPlayCount()));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_FINISH_RATE.getName(), featureIdx++,
                             musicItem.getSongArtistSearchFinishRate()));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_COUNT.getName(), featureIdx++,
                             musicItem.getSongArtistSearchCount()));
    features
        .add(new Feature(FeatureName.ARTIST_SEARCH_COUNT.getName(), featureIdx++, musicItem.getArtistSearchCount()));
    features.add(new Feature(FeatureName.ARTIST_MUSIC_COUNT.getName(), featureIdx++, musicItem.getArtistMusicCount()));
    features
        .add(new Feature(FeatureName.ARTIST_ORIGIN_COUNT.getName(), featureIdx, musicItem.getArtistOriginCount()));

    features.addAll(generateOneHotFeatures(features.size() + 1, musicItem.getStyleTags(), tagIndexMap, TAGS_ONE_HOT_));
    return features;
  }

  private static List<Feature> generateOneHotFeatures(int featureIdx, List<String> elemList,
                                                      Map<String, String> elemIndexMap, String name) {
    List<Feature> oneHotFeatures = new ArrayList<>();
    if (MapUtils.isEmpty(elemIndexMap)) {
      return oneHotFeatures;
    }
    Set<String> elemIndexSet = new HashSet<>();
    if (CollectionUtils.isNotEmpty(elemList)) {
      for (String elem : elemList) {
        if (elemIndexMap.containsKey(elem)) {
          elemIndexSet.add(elemIndexMap.get(elem));
        }
      }
    }
    int maxIndex = elemIndexMap.size();
    for (int i = 0; i < maxIndex; i++) {
      oneHotFeatures.add(new Feature(name + featureIdx, featureIdx, elemIndexSet.contains(String.valueOf(i)) ? 1D : 0D));
      featureIdx++;
    }
    return oneHotFeatures;
  }

  public static String convertToText(RankSample rankSample) {
    String musicId = rankSample.getMusic().getId();
    int label = rankSample.getLabel();
    int qid = rankSample.getQid();
    List<Feature> features = rankSample.getFeatures();

    List<String> featureTextList = features.stream().sorted(
        Comparator.comparingInt(Feature::getId)).map(f -> {
      int fid = f.getId();
      double value = f.getValue();
      return String.format("%d:%.4f", fid, value);
    }).collect(Collectors.toList());
    String featureText = StringUtils.join(featureTextList, " ");
    return String.format("%d qid:%d %s # %s", label, qid, featureText, musicId);
  }
}