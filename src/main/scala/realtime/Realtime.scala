package realtime

import akka.actor._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import realtime.Domain.DataSchema

/**
 * Created by mike on 09.08.2015.
 */
object Realtime {

  class RealtimeManager extends Actor with ActorLogging {

    //todo load from config
    val rejectionPolicy = new SimpleRejectionPolicy
    val dataSchema = new DataSchema(Granularity.DAY)
    val indexes = scala.collection.mutable.HashMap[Long, ActorRef]()

    override def receive: Receive = {
      case msg: InputRow =>
        log.debug("Received new input row: {}", msg)
        if (rejectionPolicy.accept(msg.getTimestamp)) {
          log.debug("Accepted timestamp: {}", msg.getTimestamp)
          val truncatedTimestamp: Long = dataSchema.getGranularity.truncate(new DateTime(msg.getTimestamp)).getMillis
          //todo add actor name creation

          val index = indexes.getOrElseUpdate(truncatedTimestamp, context.actorOf(Props.create(classOf[RealtimeIndex]),
            "index-" + dataSchema.getGranularity.format(new DateTime(truncatedTimestamp))))

          index ! msg
        } else {
          log.debug("Not accepted timestamp: {}", msg.getTimestamp)
        }

      case r: Any => unhandled(r)
    }
  }

  class RealtimeIndex extends Actor with ActorLogging {
    override def receive: Actor.Receive = {
      case r: InputRow =>
        log.debug("Received new input row: {}", r)
      //todo add to index
    }
  }

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("Realtime")
    val a = system.actorOf(Props[RealtimeManager], "realtime-manager")

    a ! new InputRow("test1", System.currentTimeMillis())
    a ! new InputRow("test2", System.currentTimeMillis() - 24 * 60 * 60 * 1000)
    a ! new InputRow("test3", System.currentTimeMillis() - 3 * 24 * 60 * 60 * 1000)
    a ! "kkk"
  }

}
