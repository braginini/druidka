package realtime

import akka.actor.Actor.Receive
import akka.actor.ActorLogging
import akka.persistence.{SnapshotOffer, PersistentView, PersistentActor}
import realtime.index.SegmentIndex

/**
 * Created by mikhail on 16/09/2015.
 */
object QueryNode {

  class QueryNode extends PersistentView with ActorLogging {

    override def viewId: String = "test_view_id"

    override def persistenceId: String = context.parent.path.name

    override def receive: Receive = {
      case payload  =>
        println(payload)
    }
  }

}
