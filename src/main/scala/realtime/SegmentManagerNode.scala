package realtime

import akka.actor._
import akka.persistence.{SnapshotOffer, PersistentActor}
import org.joda.time.{Period, DateTime}
import realtime.Domain._
import realtime.SegmentWorkerNode._
import scala.collection.immutable.HashSet
import scala.concurrent.duration._

/**
 * Created by mike on 09.08.2015.
 */
object SegmentManagerNode {

  //cmd to save snapshot
  case object Persist

  //cmd to finish segment
  case object HandsOff

  case class NewEvent(body: String, timestamp: Long)

  //the basic action for a worker
  sealed trait WorkerAction

  case class AddedWorker(workerRef: ActorRef) extends WorkerAction

  case class RemovedWorker(name: String) extends WorkerAction

  /**
   * a state object used to keep the list of active workers in order to recover after failures
   *
   * @param activeWorkers
   */
  case class SegmentManagerState(activeWorkers: HashSet[String] = HashSet.empty) {
    def updated(action: WorkerAction): SegmentManagerState = {
      action match {
        case RemovedWorker(name) => copy(activeWorkers - name)
        case AddedWorker(workerRef) => copy(activeWorkers + workerRef.path.name)
      }
    }

    def size(): Int = activeWorkers.size
  }

  val indexChildPrefix: String = "index-"

  //cmd to stop handling index and load to deep storage

  class SegmentManagerNode extends PersistentActor with ActorLogging {

    /**
     * keeps the names of current children
     */
    var state = SegmentManagerState()

    import context.dispatcher

    //todo load from config
    val rejectionPolicy = new SimpleRejectionPolicy
    val dataSchema = new DataSchema(Granularity.HOUR)
    val persistDelay = new Period(0, 0, 10, 0)
    //10 secs
    val strugglingWindow = new Period(0, 10, 0, 0) //10 minutes

    def updateState(event: WorkerAction): Unit =
      state = state.updated(event)


    override def receiveRecover: Receive = {
      case e: WorkerAction => updateState(e)
      case s: SnapshotOffer => log.debug("Received snapshot {}", s)
    }

    override def receiveCommand: Receive = {

      case NewEvent(body, timestamp) =>
        log.debug("Received NewEvent: {}, {}", body, timestamp)
        if (rejectionPolicy.accept(timestamp)) {
          log.debug("Accepted timestamp: {}", timestamp)
          val worker: ActorRef = getOrCreateChildWorker(timestamp, indexChildPrefix, classOf[SegmentWorkerNode])
          //watch a child to receive terminated when it hands off
          context.watch(worker)
          scheduleChildTasks(worker)

          worker ! NewEvent(body, timestamp)
        } else {
          log.debug("Not accepted timestamp: {}", timestamp)
        }

      case Terminated(_) =>
        log.debug("Received Terminated: {}, {}", sender().path.name)
        //the child should not be longer in a child list
        persist(RemovedWorker(sender().path.name)) { action =>
          updateState(action)
        }

      case r: Any => unhandled(r)
    }

    override def persistenceId: String = self.path.name


    def scheduleChildTasks(index: ActorRef): Unit = {
      //schedule hands-off
      val handsOff = dataSchema.getGranularity.duration().toStandardDuration.getMillis +
        strugglingWindow.toStandardDuration.getMillis
      context.system.scheduler.scheduleOnce(handsOff millis) {
        index ! HandsOff
      }
    }

    def getOrCreateChildWorker(timestamp: Long, namePrefix: String, actorClass: Class[_], constructorArgs: String*): ActorRef = {
      val truncatedTimestamp: Long = dataSchema.getGranularity.truncate(new DateTime(timestamp)).getMillis
      val indexName = namePrefix + dataSchema.getGranularity.format(new DateTime(truncatedTimestamp))

      //get an existing child index actor or create an index child actor with a given granularity (name)
      val index: Option[ActorRef] = context.child(indexName)
      index match {
        case Some(a) => a
        case None =>
          val worker = context.actorOf(Props.create(actorClass, constructorArgs: _*), indexName)
          persist(AddedWorker(worker)) { action =>
            updateState(action)
          }
          worker
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val system = ActorSystem("Realtime")
    val a = system.actorOf(Props[SegmentManagerNode], "segment-manager")

    a ! NewEvent("fff", System.currentTimeMillis())

    /*val b = system.actorOf(Props.create(classOf[MessageGenerator], a), "generator")*/
  }

}
