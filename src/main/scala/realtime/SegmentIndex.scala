package realtime

import realtime.SegmentManagerNode.NewEvent

import scala.collection.immutable.HashMap

/**
 * Created by mikhail on 10/08/2015.
 */
class SegmentIndex extends Serializable {

  private var index : HashMap[String, Long] = HashMap()

  def addRow(row : NewEvent): Unit = {
    index += (row.body -> row.timestamp)
  }

  def getRow(key : String): Option[NewEvent] = {
    index.get(key) match {
      case Some(v) => Option(NewEvent(key, v))
      case None => None
    }
  }

  override def toString : String = index.toString()

}
