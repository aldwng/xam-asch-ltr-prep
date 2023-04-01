package net.xam.ltr.prep.run.prepare;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import net.xam.asch.ltr.utils.HdfsAccessor;
import net.xam.asch.ltr.utils.SerializationUtils;
import net.xam.ltr.prep.run.utils.Paths;
import net.xam.ltr.prep.run.utils.QQRankUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang3.StringUtils;

/**
 * @author aldwang
 */
public class QQRankFetcher {

  private static final HdfsAccessor hdfsAccessor = HdfsAccessor.getInstance();

  private static final String QQ_SEARCH_URL_PATTERN = "http://api.xa.net/qp/s_id?w=%s&p=1";

  private static final HttpClient httpClient = new HttpClient();

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class QQRankItem {

    private String song_id;
    private String album_name;
    private String singer_name;
    private List<Singer> other_singer_list;

  }

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Singer {

    private String singer_name;
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
      List<String> songInfo = qqRankList.getList().stream().map(x -> {
        String albumName = x.getAlbum_name();
        List<String> singerNames = new LinkedList<>();
        singerNames.add(x.getSinger_name());
        if (CollectionUtils.isNotEmpty(x.getOther_singer_list())) {
          for (Singer singer : x.getOther_singer_list()) {
            singerNames.add(singer.getSinger_name());
          }
        }
        return QQRankUtils.createMusicBasicInfo("xiaowei_" + x.getSong_id(), albumName, singerNames);
      }).limit(10).collect(Collectors.toList());
      return new StringBuilder(q).append("\t").append(StringUtils.join(songInfo.toArray(), "@")).toString();
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
      System.out.println("query: " + query);
      System.out.println("url: " + url);
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
