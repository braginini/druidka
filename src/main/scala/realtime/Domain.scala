package realtime

import org.joda.time.{MutableDateTime, DateTime}
/**
 * Created by mike on 09.08.2015.
 */
object Domain {


  class InputRow(b: String, t: Long) {

    private val body: String = b
    private val timestamp: Long = t

    def getBody: String = body
    def getTimestamp: Long = timestamp
  }

  class DataSchema(g: Granularity) {

    private val granularity: Granularity = g

    def getGranularity = granularity

  }
}
