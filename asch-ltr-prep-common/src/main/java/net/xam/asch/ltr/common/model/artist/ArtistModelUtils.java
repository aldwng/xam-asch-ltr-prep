package net.xam.asch.ltr.common.model.artist;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import net.xam.asch.ltr.common.model.Feature;


public class ArtistModelUtils {

  public static List<Feature> extractFeatures(ArtistMusicItem artistMusicItem) {
    List<Feature> features = new ArrayList<>();
    int featureIdx = 1;
    features.add(new Feature(FeatureName.SONG_SEARCH_PLAY_COUNT.getName(), featureIdx++,
                             artistMusicItem.getSongSearchPlayCount()));
    features.add(new Feature(FeatureName.SONG_SEARCH_SKIP_COUNT.getName(), featureIdx++,
                             artistMusicItem.getSongSearchSkipCount()));
    features.add(new Feature(FeatureName.SONG_SEARCH_SKIP_30S_COUNT.getName(), featureIdx++,
                             artistMusicItem.getSongSearchSkip30sCount()));
    features.add(new Feature(FeatureName.SONG_SEARCH_FINISH_RATE.getName(), featureIdx++,
                             artistMusicItem.getSongSearchFinishRate()));
    features.add(new Feature(FeatureName.SONG_SEARCH_FINISH_30S_RATE.getName(), featureIdx++,
                             artistMusicItem.getSongSearchFinish30sRate()));

    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_PLAY_COUNT.getName(), featureIdx++,
                             artistMusicItem.getSongArtistSearchPlayCount()));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_SKIP_COUNT.getName(), featureIdx++,
                             artistMusicItem.getSongArtistSearchSkipCount()));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_SKIP_30S_COUNT.getName(), featureIdx++,
                             artistMusicItem.getSongArtistSearchSkip30sCount()));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_FINISH_RATE.getName(), featureIdx++,
                             artistMusicItem.getSongArtistSearchFinishRate()));
    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_FINISH_30S_RATE.getName(), featureIdx++,
                             artistMusicItem.getSongArtistSearchFinish30sRate()));

    features.add(new Feature(FeatureName.ARTIST_SEARCH_FINISH_RATE.getName(), featureIdx++,
                             artistMusicItem.getArtistSearchFinishRate()));
    features.add(new Feature(FeatureName.ARTIST_SEARCH_FINISH_30S_RATE.getName(), featureIdx++,
                             artistMusicItem.getArtistSearchFinish30sRate()));

    features.add(new Feature(FeatureName.SONG_SEARCH_COUNT.getName(), featureIdx++,
                             artistMusicItem.getSongSearchCount()));

    features.add(new Feature(FeatureName.SONG_ARTIST_SEARCH_COUNT.getName(), featureIdx++,
                             artistMusicItem.getSongArtistSearchCount()));

    features.add(new Feature(FeatureName.IS_ORIGIN.getName(), featureIdx,
                             artistMusicItem.isOrigin() ? 1D : 0D));
    return features;
  }

  public static String convertToText(ArtistSample artistSample) {
    String musicId = artistSample.getArtistMusicItem().getId();
    List<String> featureTextList = artistSample.getFeatures().stream().sorted(
        Comparator.comparingInt(Feature::getId)).map(f -> {
      int fid = f.getId();
      double value = f.getValue();
      return String.format("%d:%.4f", fid, value);
    }).collect(Collectors.toList());
    String featureText = StringUtils.join(featureTextList, " ");
    return String.format("%d qid:%d %s # %s", artistSample.getLabel(), artistSample.getQid(), featureText, musicId);
  }
}
