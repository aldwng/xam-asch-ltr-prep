package com.xiaomi.misearch.rank.music.common.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import org.apache.commons.collections.CollectionUtils;

import com.xiaomi.misearch.rank.music.common.model.Feature;
import com.xiaomi.misearch.rank.music.common.model.FeatureName;
import com.xiaomi.misearch.rank.music.common.model.MusicItem;
import com.xiaomi.misearch.rank.music.common.model.StoredMusicItem;


public class MusicFeatureUtils {

  private static int ARTIST_VECTOR_LENGTH = 32;

  private static String ARTIST_VECTOR_ = "artist_vector_";
  private static String COVER_ARTIST_VECTOR_ = "cover_artist_vector_";
  private static String TAGS_ONE_HOT_ = "tag_";

  public static List<Feature> extractFeatures(MusicItem musicItem, Map<String, Integer> tagIndexMap) {
    List<Feature> features = new ArrayList<>();
    features.add(new Feature(FeatureName.META_RANK.getName(), FeatureName.META_RANK.getId(), musicItem.getMetaRank()));
    features.add(new Feature(FeatureName.RES_RANK.getName(), FeatureName.RES_RANK.getId(), musicItem.getResRank()));
    features.add(new Feature(FeatureName.QUALITY.getName(), FeatureName.QUALITY.getId(), musicItem.getQuality()));
    features.add(new Feature(FeatureName.QQ_SONG_RAW_RANK.getName(), FeatureName.QQ_SONG_RAW_RANK.getId(),
                             musicItem.getQqSongRawRank()));
    features.add(new Feature(FeatureName.QQ_ARTIST_RAW_RANK.getName(), FeatureName.QQ_ARTIST_RAW_RANK.getId(),
                             musicItem.getQqArtistRawRank()));
    features.add(new Feature(FeatureName.QQ_RANK.getName(), FeatureName.QQ_RANK.getId(), musicItem.getQqRank()));
    features.add(new Feature(FeatureName.HAS_LYRIC.getName(), FeatureName.HAS_LYRIC.getId(), musicItem.getHasLyric()));
    features.add(new Feature(FeatureName.SONG_SEARCH_PLAY_COUNT.getName(), FeatureName.SONG_SEARCH_PLAY_COUNT.getId(),
                             musicItem.getSongSearchPlayCount()));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_PLAY_COUNT.getName(),
                             FeatureName.SONG_ARTIST_SEARCH_PLAY_COUNT.getId(), musicItem.getSongArtistSearchPlayCount()));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_FINISH_RATE.getName(),
                             FeatureName.SONG_ARTIST_SEARCH_FINISH_RATE.getId(), musicItem.getSongArtistSearchFinishRate()));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_COUNT.getName(), FeatureName.SONG_ARTIST_SEARCH_COUNT.getId(),
                             musicItem.getSongArtistSearchCount()));
    features.add(new Feature(FeatureName.ARTIST_SEARCH_COUNT.getName(), FeatureName.ARTIST_SEARCH_COUNT.getId(),
                             musicItem.getArtistSearchCount()));
    features.add(new Feature(FeatureName.ARTIST_MUSIC_COUNT.getName(), FeatureName.ARTIST_MUSIC_COUNT.getId(),
                             musicItem.getArtistMusicCount()));
    features.add(new Feature(FeatureName.ARTIST_ORIGIN_COUNT.getName(), FeatureName.ARTIST_ORIGIN_COUNT.getId(),
                             musicItem.getArtistOriginCount()));

    features.addAll(generateVectorFeatures(features.size(), musicItem.getArtistVector(), ARTIST_VECTOR_LENGTH,
                                           ARTIST_VECTOR_));
    features.addAll(generateVectorFeatures(features.size(), musicItem.getCoverArtistVector(), ARTIST_VECTOR_LENGTH,
                                           COVER_ARTIST_VECTOR_));

    features.addAll(generateOneHotFeatures(features.size(), musicItem.getTags(), tagIndexMap, TAGS_ONE_HOT_));
    return features;
  }

  public static List<Feature> extractFeatures(StoredMusicItem musicItem) {
    List<Feature> features = new ArrayList<>();
    features.add(new Feature(FeatureName.SONG_SEARCH_PLAY_COUNT.getName(), FeatureName.SONG_SEARCH_PLAY_COUNT.getId(),
                             musicItem.songSearchPlayCount));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_PLAY_COUNT.getName(),
                             FeatureName.SONG_ARTIST_SEARCH_PLAY_COUNT.getId(), musicItem.songArtistSearchPlayCount));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_FINISH_RATE.getName(),
                             FeatureName.SONG_ARTIST_SEARCH_FINISH_RATE.getId(), musicItem.songArtistSearchFinishRate));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_COUNT.getName(), FeatureName.SONG_ARTIST_SEARCH_COUNT.getId(),
                             musicItem.songArtistSearchCount));
    features.add(new Feature(FeatureName.ARTIST_SEARCH_COUNT.getName(), FeatureName.ARTIST_SEARCH_COUNT.getId(),
                             musicItem.artistSearchCount));
    features.add(new Feature(FeatureName.ARTIST_MUSIC_COUNT.getName(), FeatureName.ARTIST_MUSIC_COUNT.getId(),
                             musicItem.artistMusicCount));
    features.add(new Feature(FeatureName.ARTIST_ORIGIN_COUNT.getName(), FeatureName.ARTIST_ORIGIN_COUNT.getId(),
                             musicItem.artistOriginCount));
    return features;
  }

  public static String convertFeaturesToText(List<Feature> features) {
    if (CollectionUtils.isEmpty(features)) {
      return null;
    }
    List<String> featureTexts = features.stream().map(f -> String.format("%d:%.3f", f.getId(), f.getValue()))
        .collect(Collectors.toList());
    return Joiner.on(" ").join(featureTexts);
  }

  private static List<Feature> generateVectorFeatures(int startId, List<Double> vector, int dimension, String name) {
    List<Feature> vectorFeatures = new ArrayList<>();
    if (dimension < 1) {
      return vectorFeatures;
    }
    if (CollectionUtils.isNotEmpty(vector) && vector.size() == dimension) {
      for (int i = 0; i < dimension; i++) {
        vectorFeatures.add(new Feature(name + i + 1, startId + i + 1, vector.get(i)));
      }
    } else {
      for (int i = 0; i < dimension; i++) {
        vectorFeatures.add(new Feature(name + i + 1, startId + i + 1, 0D));
      }
    }
    return vectorFeatures;
  }

  private static List<Feature> generateOneHotFeatures(int startId, List<String> elemList,
                                                      Map<String, Integer> elemIndexMap, String name) {
    List<Feature> oneHotFeatures = new ArrayList<>();
    if (elemIndexMap == null || elemIndexMap.size() < 1) {
      return oneHotFeatures;
    }
    Set<Integer> elemIndexSet = new HashSet<>();
    if (CollectionUtils.isNotEmpty(elemList)) {
      for (String elem : elemList) {
        if (elemIndexMap.containsKey(elem)) {
          elemIndexSet.add(elemIndexMap.get(elem));
        }
      }
    }
    int maxIndex = elemIndexMap.size();
    for (int i = 0; i < maxIndex; i++) {
      oneHotFeatures.add(new Feature(name + i + 1, startId + i + 1, elemIndexSet.contains(i) ? 1D : 0D));
    }
    return  oneHotFeatures;
  }
}
