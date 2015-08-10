package realtime

/**
 * Created by mike on 09.08.2015.
 */
trait RejectionPolicy {

  def accept(timestamp: Long): Boolean

}

class SimpleRejectionPolicy extends RejectionPolicy {
  override def accept(timestamp: Long): Boolean = {
    true
  }
}
