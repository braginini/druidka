package realtime

/**
 * Created by mike on 09.08.2015.
 */
object Domain {


  class DataSchema(g: Granularity) {

    private val granularity: Granularity = g

    def getGranularity = granularity

  }

  trait RejectionPolicy {

    def accept(timestamp: Long): Boolean

  }

  class SimpleRejectionPolicy extends RejectionPolicy {
    override def accept(timestamp: Long): Boolean = {
      val now: Long = System.currentTimeMillis

      timestamp >= (now - 80 * 1000)
    }
  }
}
