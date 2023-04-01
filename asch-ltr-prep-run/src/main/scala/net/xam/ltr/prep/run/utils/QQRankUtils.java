package net.xam.ltr.prep.run.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * @author aldwang
 */
public class QQRankUtils {

  public static String createMusicBasicInfo(String resId, String albumName, List<String> singerNames) {
    new LinkedList<>(singerNames).sort(Comparator.naturalOrder());
    return StringUtils
        .join(Arrays.asList(resId, albumName, StringUtils.join(singerNames, ";")), "#");
  }
}
