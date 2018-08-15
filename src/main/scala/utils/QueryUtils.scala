package utils

import com.xiaomi.miui.ad.appstore.feature._
import org.apache.spark.rdd.RDD

object QueryUtils {

  def convertToQueryMap(query: RDD[String]): RDD[QueryMap] = {
    return query.zipWithIndex().map {
      case (qy, id) =>
        val ans = new QueryMap()
        ans.setQuery(qy)
        ans.setId(id)
        ans
    }
  }
}
