package realtime

/**
 * Created by mike on 09.08.2015.
 */
class InputRow(b: String, t: Long) {

  private val body: String = b
  private val timestamp: Long = t

  def getBody: String = body
  def getTimestamp: Long = timestamp

  override def toString: String = {
    "body=" + b.toString + " timestamp=" + timestamp.toString
  }
}
