package realtime.index

import realtime.SegmentManagerNode.AddEvent

import scala.collection.immutable.HashMap

/**
 * Created by mikhail on 10/08/2015.
 */
class SegmentIndex extends Serializable {

  private var index : HashMap[String, Long] = HashMap()

  def addRow(row : AddEvent): Unit = {
    index += (row.body -> row.timestamp)
  }

  def getRow(key : String): Option[AddEvent] = {
    index.get(key) match {
      case Some(v) => Option(AddEvent(key, v))
      case None => None
    }
  }

  override def toString : String = index.toString()

}
