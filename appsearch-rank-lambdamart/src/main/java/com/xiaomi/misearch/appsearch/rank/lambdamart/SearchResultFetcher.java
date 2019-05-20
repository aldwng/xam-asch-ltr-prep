package com.xiaomi.misearch.appsearch.rank.lambdamart;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.PathUtils;

import com.xiaomi.appstore.thrift.model.AppMarketSearchParam;
import com.xiaomi.appstore.thrift.model.AppMarketSearchResult;
import com.xiaomi.appstore.thrift.service.AppMarketSearch;
import com.xiaomi.miliao.thrift.ClientFactory;
import com.xiaomi.miliao.zookeeper.EnvironmentType;
import com.xiaomi.miliao.zookeeper.ZKFacade;
import com.xiaomi.misearch.appsearch.rank.hdfs.HdfsAccessor;

public class SearchResultFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(SearchResultFetcher.class);

  private static final Joiner JOINER = Joiner.on(",");
  private static final HdfsAccessor hdfsAccessor = HdfsAccessor.getInstance();

  public static void main(String[] args) throws Exception {
    boolean dev = false;
    if (args.length > 0) {
      dev = true;
    }

    String downloadHistoryPath = PathUtils.download_history_path();
    String outputPath = PathUtils.natural_results_path();
    EnvironmentType env = EnvironmentType.C3;

    if (dev) {
      downloadHistoryPath = PathUtils.download_history_path_local();
      outputPath = PathUtils.natural_results_path_local();
      env = EnvironmentType.STAGING;
    }

    ZKFacade.getZKSettings().setEnviromentType(env);
    AppMarketSearch.Iface
        client =
        ClientFactory.getClient(AppMarketSearch.Iface.class, 600000, env);

    List<String> queries = getQueries(downloadHistoryPath);
    List<String> lines = queries.stream().map(query -> fetchApps(client, query))
        .filter(line -> StringUtils.isNotEmpty(line))
        .collect(Collectors.toList());
    hdfsAccessor.writeLines(outputPath, lines);
  }

  private static String fetchApps(AppMarketSearch.Iface client, String query) {
    String filterStr =
        "{\"deviceType\":0,\"imei\":\"6P1400214d6e4f05aa6254a7eec622c50\",\"locale\":\"CN\",\"suitableType\":[0,2]}";

    AppMarketSearchParam param = new AppMarketSearchParam();
    param.setOffset(0);
    param.setLength(100);
    param.setFilterStr(filterStr);
    param.setKeyword(query);

    try {
      AppMarketSearchResult result = client.search(param);
      if (result.isSuccess()) {
        List<String> ids = result.getIds();
        LOG.info("query is {}, result is {}", query, result);
        if (CollectionUtils.isNotEmpty(ids)) {
          return String.format("%s\t%s", query, JOINER.join(ids));
        }
      }
    } catch (TException e) {
      LOG.error("fetch natural results error , query is {} {}", query, Throwables.getStackTraceAsString(e));
    }
    return null;
  }

  private static List<String> getQueries(String downloadHistoryPath) throws IOException {
    List<String> lines = hdfsAccessor.readLines(downloadHistoryPath);
    List<String> queries = lines.stream().map(line -> {
      String[] items = line.split("\t");
      return StringUtils.strip(items[0]);
    }).filter(query -> StringUtils.isNotBlank(query)).collect(Collectors.toList());
    LOG.info("queries size: {}", queries.size());
    return queries;
  }
}
