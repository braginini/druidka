package realtime

import java.security.Timestamp

import akka.actor._
import org.joda.time.{Period, DateTime}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import realtime.Domain._
import realtime.RealtimeIndex._
import scala.concurrent.duration._
import akka.persistence._

/**
 * Created by mike on 09.08.2015.
 */
object RealtimeManager {

  case class InputRow(b: String, t: Long) {

    private val body: String = b
    private val timestamp: Long = t

    def getBody: String = body
    def getTimestamp: Long = timestamp
  }

  class RealtimeManager extends Actor with ActorLogging {

    import context.dispatcher

    //todo load from config
    val rejectionPolicy = new SimpleRejectionPolicy
    val dataSchema = new DataSchema(Granularity.HOUR)
    val indexes = scala.collection.mutable.HashMap[String, ActorRef]()
    val persistDelay = new Period(0, 1, 0, 0) // 1 minute
    val strugglingWindow = new Period(0, 1, 0, 0) //1 minute


    override def receive: Receive = {

      case InputRow(body, timestamp) =>
        log.debug("Received new input row: {}, {}", body, timestamp)
        if (rejectionPolicy.accept(timestamp)) {
          log.debug("Accepted timestamp: {}", timestamp)
          getIndexChild(InputRow(body, timestamp)) ! InputRow(body, timestamp)
        } else {
          log.debug("Not accepted timestamp: {}", timestamp)
        }

      case HandsOffed(failed) =>
        log.debug("Received hand offed: {}, {}", failed, sender().path.name)
        //todo remove from indexes map
        indexes.remove(sender().path.name)

      case Persisted(failed) =>
        log.debug("Received persisted: {}, {}", failed, sender().path)
        if (failed) {
          //todo do something here
        } else {
          //reschedule persistence
          context.system.scheduler.scheduleOnce(persistDelay.toStandardDuration.getMillis.millis) {
            sender() ! Persist()
          }
        }

      case r: Any => unhandled(r)
    }

    def getIndexChild(msg: InputRow) : ActorRef = {
      val truncatedTimestamp: Long = dataSchema.getGranularity.truncate(new DateTime(msg.getTimestamp)).getMillis
      val indexName = "index-" + dataSchema.getGranularity.format(new DateTime(truncatedTimestamp))

      //create an index child actor with a given granularity
      val index : ActorRef = indexes.getOrElseUpdate(indexName, context.actorOf(Props.create(classOf[RealtimeIndex]),
        indexName))
      //todo add a choice from config to allow persist every message or do it via snapshots
      //schedule 1st persist but we will reschedule it again after child confirms persistence b ysending Persisted msg
      context.system.scheduler.scheduleOnce(persistDelay.toStandardDuration.getMillis.millis) {
        index ! Persist()
      }

      //schedule hands-off
      val handsOff = dataSchema.getGranularity.duration().toStandardDuration.getMillis +
        strugglingWindow.toStandardDuration.getMillis
      context.system.scheduler.scheduleOnce(handsOff.millis) {
        index ! HandsOff()
      }

      index
    }
  }


  def main(args: Array[String]): Unit = {
    val system = ActorSystem("Realtime")
    val a = system.actorOf(Props[RealtimeManager], "realtime-manager")

    a ! new InputRow("test1", System.currentTimeMillis())
    a ! new InputRow("test2", System.currentTimeMillis() - 24 * 60 * 60 * 1000)
    /*a ! new InputRow("test3", System.currentTimeMillis() - 3 * 24 * 60 * 60 * 1000)
    a ! "kkk"*/
  }

}
