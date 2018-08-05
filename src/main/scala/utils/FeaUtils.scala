package utils

import com.xiaomi.miui.ad.appstore.feature.{BaseFea, RankInstance, Sample}

import scala.collection.JavaConverters._

object FeaUtils {

  def makeFea(feature: String, value:Double): BaseFea = {
    val ans = new BaseFea()
    ans.setFea(feature)
    ans.setValue(value)
    ans
  }

  def makeSample(features: List[BaseFea], instance: RankInstance): Sample = {
    val ans = new Sample()
    ans.setLabel(instance.getData.getLabel)
    ans.setFeatures(features.asJava)
    ans
  }
}
