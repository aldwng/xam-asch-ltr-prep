package com.xiaomi.misearch.rank.appstore.common.model;

/**
 * @author Shenglan Wang
 */
public enum FeatureName {
  app_download_rank(100),
  app_active_rank(101),
  app_rank(102),
  app_cdr(103),
  app_dn_len(104),
  app_dn_english_char_ratio(105),
  app_dn_chinese_char_ratio(106),

  query_len(201),
  query_english_char_ratio(202),
  query_chinese_char_ratio(203),

  query_app_dn_match(301),
  query_app_dn_contains(302),
  query_app_dn_bi_jac(303),
  query_app_dn_tri_jac(304),
  query_app_dn_seg_jac(305),
  query_app_dn_bi_dice(306),
  query_app_dn_tri_dice(307),
  query_app_dn_seg_dice(308),

  query_app_brief_bi_jac(401),
  query_app_brief_tri_jac(402),
  query_app_brief_seg_jac(403),
  query_app_brief_bi_dice(404),
  query_app_brief_tri_dice(405),
  query_app_brief_seg_dice(406),

  query_app_level1_cate_contains(501),
  query_app_level1_cate_bi_jac(502),
  query_app_level1_cate_tri_jac(503),
  query_app_level1_cate_seg_jac(504),
  query_app_level1_cate_bi_dice(505),
  query_app_level1_cate_tri_dice(506),
  query_app_level1_cate_seg_dice(507),

  query_app_level2_cate_contains(601),
  query_app_level2_cate_bi_jac(602),
  query_app_level2_cate_tri_jac(603),
  query_app_level2_cate_seg_jac(604),
  query_app_level2_cate_bi_dice(605),
  query_app_level2_cate_tri_dice(606),
  query_app_level2_cate_seg_dice(607),

  query_app_keywords_bi_jac_sum(701),
  query_app_keywords_tri_jac_sum(702),
  query_app_keywords_bi_dice_sum(703),
  query_app_keywords_tri_dice_sum(704),
  query_app_keywords_overlap(705),

  query_app_pn_seg_jac(801),
  query_app_pn_seg_dice(802);

  private int id;

  public int getId() {
    return id;
  }

  FeatureName(int id) {
    this.id = id;
  }
}
