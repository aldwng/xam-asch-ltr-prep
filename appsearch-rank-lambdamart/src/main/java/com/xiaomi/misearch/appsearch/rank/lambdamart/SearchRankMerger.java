package com.xiaomi.misearch.appsearch.rank.lambdamart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.PathUtils;

import com.xiaomi.misearch.appsearch.rank.hdfs.HdfsAccessor;

/**
 * @author Shenglan Wang
 */
public class SearchRankMerger {

  private static final Logger LOG = LoggerFactory.getLogger(SearchRankMerger.class);

  private static final HdfsAccessor hdfsAccessor = HdfsAccessor.getInstance();

  public static void main(String[] args) throws IOException {
    String day = null;
    if (args.length >= 1) {
      day = args[0];
    } else {
      LOG.error("Input Date: 20180101");
      System.exit(1);
    }

    boolean dev = false;
    if (args.length >= 2) {
      dev = true;
    }

    LOG.info("Day: {}", day);
    LOG.info("Dev: {}", dev);

    String downloadHistoryPath = PathUtils.download_history_path_local();
    String predictRankPath = PathUtils.predict_rank_path_local() + "/part-00000";
    String outputPath = PathUtils.predict_rank_merged_path_local() + "/rank_merged.txt";

    if (!dev) {
      downloadHistoryPath = PathUtils.download_history_path();
      predictRankPath =
          PathUtils.IntermediateDatePath(PathUtils.predict_rank_path(), Integer.parseInt(day)) + "/part-00000";
      outputPath =
          PathUtils.IntermediateDatePath(PathUtils.predict_rank_merged_path(), Integer.parseInt(day))
          + "/rank_merged.txt";
    }

    List<String> downloadHistory = hdfsAccessor.readLines(downloadHistoryPath);
    Map<String, Long> maxDownloadWeightMap = getMaxDownloadWeightMap(downloadHistory);
    Map<String, Long> downloadCountMap = getDownloadCountMap(downloadHistory);


    List<String> predictRankLines = hdfsAccessor.readLines(predictRankPath);
    Map<String, List<String>>
        predictResult =
        generatePredictResult(predictRankLines, maxDownloadWeightMap, downloadCountMap);

    List<String> result = mergeDownloadHistory(downloadHistory, predictResult);
    LOG.info("lines count: {}", result.size());
    hdfsAccessor.writeLines(outputPath, result);
  }

  private static Map<String, Long> getMaxDownloadWeightMap(List<String> downloadHistory) {
    Map<String, Long> maxDownloadWeightMap = new HashMap<>();
    for (String line : downloadHistory) {
      String[] splits = line.split("\t");
      String query = splits[0];

      List<String> appInfos = Arrays.asList(splits[3].split(","));
      if (CollectionUtils.isEmpty(appInfos)) {
        continue;
      }

      String downloadWeightStr = appInfos.get(0).split(":")[1];
      maxDownloadWeightMap.put(query, Long.parseLong(downloadWeightStr));
    }
    return maxDownloadWeightMap;
  }

  private static Map<String, Long> getDownloadCountMap(List<String> downloadHistory) {
    Map<String, Long> downloadCountMap = new HashMap<>();
    for (String line : downloadHistory) {
      String[] splits = line.split("\t");
      String query = splits[0];
      List<String> appInfos = Arrays.asList(splits[3].split(","));

      if (CollectionUtils.isEmpty(appInfos)) {
        continue;
      }

      for (String appInfo : appInfos) {
        String[] items = appInfo.split(":");
        String appId = items[0];
        String downloadCount = items[2];
        downloadCountMap.put(getDownloadCountMapKey(query, appId), Long.parseLong(downloadCount));
      }
    }
    return downloadCountMap;
  }

  private static String getDownloadCountMapKey(String query, String appId) {
    return query + "#" + appId;
  }

  private static Map<String, List<String>> generatePredictResult(List<String> predictLines,
                                                                 Map<String, Long> maxDownloadWeightMap,
                                                                 Map<String, Long> downloadCountMap) {
    Map<String, List<String>> result = new HashMap<>();
    for (String line : predictLines) {
      String[] splits = line.split(",");
      String query = splits[0];

      Long maxDownloadWeight = maxDownloadWeightMap.get(query);
      if (maxDownloadWeight == null) {
        continue;
      }

      List<String> appInfos = new ArrayList<>();
      String[] items = splits[1].split(";");
      // Reserve top10 apps, apps should be with the order of rank score
      int i = 11;
      for (String item : items) {
        i--;
        if (i <= 0) {
          break;
        }

        String appId = item.split(":")[0];
        Long downloadCount = downloadCountMap.get(getDownloadCountMapKey(query, appId));
        if (downloadCount == null) {
          continue;
        }

        Long rankScore = maxDownloadWeight + i;
        appInfos.add(String.format("%s:%d:%d:%d", appId, rankScore, downloadCount, rankScore));
      }
      result.put(query, appInfos);
    }
    return result;
  }

  private static List<String> mergeDownloadHistory(List<String> downloadHistory, Map<String, List<String>> predictResult){
    List<String> result = new ArrayList<>();
    for (String downloadHistoryItem : downloadHistory) {
      String[] splits = downloadHistoryItem.split("\t");
      String query = splits[0];
      String searchCount = splits[1];
      String downloadCount = splits[2];

      List<String> appInfos = new LinkedList<>(Arrays.asList(splits[3].split(",")));
      List<String> predictAppInfos = predictResult.get(query);

      if (CollectionUtils.isEmpty(appInfos)) {
        LOG.info("empty line: {}", downloadHistoryItem);
        continue;
      }

      if (CollectionUtils.isNotEmpty(predictAppInfos)) {
        appInfos.addAll(0, predictAppInfos);

        // Remove duplicate apps
        Set<String> appIds = new HashSet();
        Iterator<String> iterator = appInfos.iterator();
        while (iterator.hasNext()) {
          String appInfo = iterator.next();
          String[] items = appInfo.split(":");
          String appId = items[0];
          if (appIds.contains(appId)) {
            iterator.remove();
          } else {
            appIds.add(appId);
          }
        }
      } else {
        LOG.warn("Predict empty, query={}", query);
      }

      String appInfoStr = StringUtils.join(appInfos, ",");
      String line = StringUtils.join(Arrays.asList(query, searchCount, downloadCount, appInfoStr), "\t");
      result.add(line);
    }
    return result;
  }
}
