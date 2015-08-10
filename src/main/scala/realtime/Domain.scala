package realtime

import org.joda.time.{MutableDateTime, DateTime}
/**
 * Created by mike on 09.08.2015.
 */
object Domain {


  class DataSchema(g: Granularity) {

    private val granularity: Granularity = g

    def getGranularity = granularity

  }
}
