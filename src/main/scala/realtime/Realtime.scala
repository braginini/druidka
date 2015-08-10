package realtime

import akka.actor._
import org.joda.time.DateTime
import realtime.Domain.DataSchema

/**
 * Created by mike on 09.08.2015.
 */
object Realtime {

  class RealtimeManager extends Actor with ActorLogging {

    //todo load from config
    val rejectionPolicy = new SimpleRejectionPolicy
    val dataSchema = new DataSchema(Granularity.DAY)
    val indexes = Map[Long, ActorRef]()

    override def receive: Receive = {
      case r: InputRow =>
        log.debug("Received new input row: {}", r)
        if (rejectionPolicy.accept(r.getTimestamp)) {
          log.debug("Accepted timestamp: {}", r.getTimestamp)
          val truncatedTimestamp: Long = dataSchema.getGranularity.truncate(new DateTime(r.getTimestamp)).getMillis
          //todo add actor name creation
          val index = indexes.getOrElse(truncatedTimestamp, context.actorOf(Props.create(classOf[RealtimeIndex])))
          index ! r
        }
        else
            log.debug("Not accepted timestamp: {}", r.getTimestamp)


    }
  }

  class RealtimeIndex extends Actor with ActorLogging {
    override def receive: Actor.Receive = ???
  }

}
