package com.xiaomi.misearch.appsearch.rank.utils;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.misearch.appsearch.rank.model.App;
import com.xiaomi.misearch.appsearch.rank.model.Feature;
import com.xiaomi.misearch.appsearch.rank.model.RankSample;

/**
 * @author Shenglan Wang
 */
public class SampleUtilsTest {

  @Test
  public void testConvertToText() {
    String query = "很好abc";
    App app = new App();
    app.setRawId("10104");
    app.setDisplayName("微信很好");
    app.setBrief("很好很好很好，是吗是吗");
    app.setPackageName("abc.b.d.def");
    app.setKeywords(Arrays.asList("很好abc", "完美bbb", "太好了"));
    app.setLevel1Category("游戏");
    app.setLevel2Category("体育很好");
    app.setAppCdr((double) 1.0f);
    app.setAppActiveRank((double) 2.0f);
    app.setAppDownloadRank((double) 3.0f);
    app.setAppRank((double) 4.0f);

    List<Feature> features = FeatureUtils.extractFeatures(app, query);
    Assert.assertEquals(45, features.size());

    RankSample sample = new RankSample(app, query, 0, 0, features);
    String text = SampleUtils.convertToText(sample);
    System.out.println(text);
    String
        expectedText =
        "0 qid:0 100:3.0000 101:2.0000 102:4.0000 103:1.0000 104:4.0000 105:0.0000 106:1.0000 201:5.0000 202:0.6000 203:0.4000 301:0.0000 302:0.0000 303:0.1667 304:0.0000 305:0.5000 306:0.2857 307:0.0000 308:0.6667 401:0.1111 402:0.0000 403:0.3333 404:0.2000 405:0.0000 406:0.5000 501:0.0000 502:0.0000 503:0.0000 504:0.0000 505:0.0000 506:0.0000 507:0.0000 601:0.0000 602:0.1667 603:0.0000 604:0.5000 605:0.2857 606:0.0000 607:0.6667 701:1.0000 702:1.0000 703:1.0000 704:1.0000 705:0.3333 801:0.1667 802:0.2857 # 10104";
    Assert.assertEquals(expectedText, text);
  }
}
