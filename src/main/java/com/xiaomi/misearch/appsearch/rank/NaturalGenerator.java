package com.xiaomi.misearch.appsearch.rank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.misearch.appsearch.rank.common.HdfsAccessor;
import com.xiaomi.appstore.thrift.model.AppMarketSearchParam;
import com.xiaomi.appstore.thrift.model.AppMarketSearchResult;
import com.xiaomi.appstore.thrift.service.AppMarketSearch;
import com.xiaomi.miliao.thrift.ClientFactory;
import com.xiaomi.miliao.zookeeper.EnvironmentType;
import com.xiaomi.miliao.zookeeper.ZKFacade;

public class NaturalGenerator {

  private static final Logger log = LoggerFactory.getLogger(NaturalGenerator.class);
  private static final Joiner joiner = Joiner.on(",");
  private static final HdfsAccessor hdfsAccessor = HdfsAccessor.getInstance();
  private static final String TOP_QUERY_PATH_FORMATER = "/user/h_misearch/jobs/app_search/topquery/%s.txt";
  private static final String OUTPUT_PATH_FORMATER = "/user/h_misearch/appmarket/rank/predict/natural_results.txt";

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: NaturalGenerator <DateTime>");
      System.out.println("Example: NaturalGenerator 20170810");
      return;
    }
    String dateTime = args[0];
    String outputPathPath = String.format(OUTPUT_PATH_FORMATER, dateTime);

    List<String> queryList = getTopQuery(dateTime);
    List<String> resultList = new ArrayList<>();
    ZKFacade.getZKSettings().setEnviromentType(EnvironmentType.C3);
    AppMarketSearch.Iface
        serviceIface =
        ClientFactory.getClient(AppMarketSearch.Iface.class, 600000, EnvironmentType.C3);

    String filterStr =
        "{\"deviceType\":0,\"imei\":\"6P1400214d6e4f05aa6254a7eec622c50\",\"locale\":\"CN\",\"suitableType\":[0,2]}";

    AppMarketSearchParam param = new AppMarketSearchParam();
    param.setOffset(0);
    param.setLength(150);
    param.setFilterStr(filterStr);

    for (String query : queryList) {
      param.setKeyword(query.trim());
      try {
        AppMarketSearchResult result = serviceIface.search(param);
        if (result.isSuccess()) {
          List<String> ids = result.getIds();
          log.info("query is {}, result is {}", query, result);
          if (ids != null) {
            resultList.add(String.format("%s\t%s", query, joiner.join(ids)));
          }
        }
      } catch (TException e) {
        log.error("get natural results error , query is {} {}", query, e);
      }
    }
    hdfsAccessor.writeLines(outputPathPath, resultList);
  }

  private static List<String> getTopQuery(String dateTime) throws IOException {
    List<String> queryList = new ArrayList<>();
    String topQueryFilePath = String.format(TOP_QUERY_PATH_FORMATER, dateTime);
    List<String> lines = hdfsAccessor.readLines(topQueryFilePath);
    for (String line : lines.subList(0, 100)) {
      String[] items = line.split("\t");
      String query = StringUtils.strip(items[0]);
      if (StringUtils.isNotBlank(query)) {
        queryList.add(query);
      }
    }
    log.info("get query list count of {}", queryList.size());
    return queryList;
  }
}
