package com.xiaomi.misearch.rank.music.prepare;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang3.StringUtils;

import com.xiaomi.misearch.rank.music.utils.Paths;
import com.xiaomi.misearch.rank.utils.SerializationUtils;
import com.xiaomi.misearch.rank.utils.hdfs.HdfsAccessor;

/**
 * @author Shenglan Wang
 */
public class QQRankFetcher {

  private static final HdfsAccessor hdfsAccessor = HdfsAccessor.getInstance();

  private static final String QQ_SEARCH_URL_PATTERN = "http://api.ai.srv/qq_proxy/search_without_xiaomiid?w=%s&p=1";

  private static final HttpClient httpClient = new HttpClient();

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class QQRankItem {

    private String song_id;
  }

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class QQRankList {

    private List<QQRankItem> list;
  }


  public static void main(String[] args) throws IOException {
    boolean dev = false;
    if (args.length > 0 && args[0].equalsIgnoreCase("dev")) {
      dev = true;
    }

    String queryPath = Paths.QUERY_PATH() + "/part-00000";
    String qqRankPath = Paths.QQ_RANK_PATH();
    if (dev) {
      queryPath = Paths.QUERY_PATH_LOCAL() + "/part-00000";
      qqRankPath = Paths.QQ_RANK_PATH_LOCAL();
    }
    List<String> lines = hdfsAccessor.readLines(queryPath);

    List<String> resultLines = lines.stream().map(x -> extractQuery(x)).filter(StringUtils::isNotBlank).map(q -> {
      QQRankList qqRankList = getQQRankList(q);
      if (qqRankList == null || CollectionUtils.isEmpty(qqRankList.getList())) {
        return null;
      }
      List<String>
          songIds =
          qqRankList.getList().stream().map(x -> "xiaowei_" + x.getSong_id()).limit(10).collect(Collectors.toList());
      return new StringBuilder(q).append("\t").append(StringUtils.join(songIds.toArray(), ",")).toString();
    }).filter(StringUtils::isNotBlank).collect(Collectors.toList());
    hdfsAccessor.writeLines(qqRankPath, resultLines);
  }

  private static String extractQuery(String line) {
    String[] items = line.split("\t");
    if (items.length < 1) {
      return null;
    }
    if (items.length == 1) {
      return items[0];
    }

    String song = items[0];
    String displaySong = items[1];
    if (StringUtils.isNotBlank(displaySong)) {
      song = displaySong;
    }
    return song;
  }

  private static QQRankList getQQRankList(String query) {
    try {
      String url = String.format(QQ_SEARCH_URL_PATTERN, URLEncoder.encode(query, "UTF-8"));
      GetMethod get = new GetMethod(url);
      get.getParams().setParameter("http.protocol.content-charset", "UTF-8");
      int status = httpClient.executeMethod(get);
      String content = get.getResponseBodyAsString().trim();
      if (status == 200) {
        return SerializationUtils.fromJson(content, QQRankList.class);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
