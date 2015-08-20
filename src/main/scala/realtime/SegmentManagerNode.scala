package realtime

import akka.actor._
import akka.persistence._
import org.joda.time.{Period, DateTime}
import realtime.Domain._
import realtime.SegmentManagerNode._
import realtime.SegmentWorkerNode._
import scala.collection.immutable.{HashMap}
import scala.concurrent.duration._

/**
 * Created by mike on 09.08.2015.
 */
object SegmentManagerNode {

  //cmd to save snapshot
  case object Persist

  //cmd to finish segment
  case object HandsOff

  case class AddEvent(body: String, timestamp: Long)

  //the basic action for a worker
  sealed trait WorkerAction

  case class AddedWorker(workerRef: Worker) extends WorkerAction

  case class RemovedWorker(name: String) extends WorkerAction


  @SerialVersionUID(1L)
  class Worker(
                val name: String,
                @transient var ref: Option[ActorRef] = None)
    extends Serializable

  /**
   * a state object used to keep the list of active workers in order to recover after failures
   *
   * @param activeWorkers
   */
  case class SegmentManagerState(activeWorkers: HashMap[String, Worker] = HashMap.empty) {
    def updated(action: WorkerAction): SegmentManagerState = {
      action match {
        case RemovedWorker(name) => copy(activeWorkers - name)
        case AddedWorker(worker) => copy(activeWorkers + (worker.ref.get.path.name -> worker))
      }
    }

    def get(name: String): Option[Worker] = {
      activeWorkers.get(name)
    }

    def size(): Int = activeWorkers.size
  }

}
  //cmd to stop handling index and load to deep storage

  class SegmentManagerNode extends PersistentActor with ActorLogging {

    var lastSnapshot: Option[SnapshotMetadata] = None

    /**
     * keeps the names of current children
     */
    var state = SegmentManagerState()

    val workerNamePrefix: String = "index-"

    import context.dispatcher

    //todo load from config
    val rejectionPolicy = new SimpleRejectionPolicy
    val dataSchema = new DataSchema(Granularity.HOUR)
    val persistDelay = new Period(0, 0, 10, 0)
    //10 secs
    val strugglingWindow = new Period(0, 10, 0, 0) //10 minutes

    def updateState(event: WorkerAction): Unit =
      state = state.updated(event)

    /**
     * Gets a worker ref from the state and if not exists, creates one
     *
     * @param name
     * @return
     */
    def getWorker(name: String): ActorRef = {
      state.get(name).flatMap(_.ref) match {
        case Some(ref) => ref
        case None => create(name, recovery = false)
      }
    }

    /**
     * Schedules hands-off for a given worker. Worker will stop serving the segment.
     * Usually workers may have struggling events, that for some reason (network) arrived after the granularity maximum timestamp.
     * We need to add some window for that.
     *
     * @param worker
     * @param granularity
     * @param strugglingWindow
     */
    def scheduleHandsOff(worker: ActorRef, granularity: Granularity, strugglingWindow: Period): Unit = {
      val handsOff = granularity.duration().toStandardDuration.getMillis +
        strugglingWindow.toStandardDuration.getMillis
      context.system.scheduler.scheduleOnce(handsOff millis) {
        worker ! HandsOff
      }
    }

    /**
     * Creates a child worker and saves snapshot if no recovery is detected
     *
     * @param name
     * @param recovery
     * @return
     */
    def create(name: String, recovery: Boolean): ActorRef = {
      val worker = context.actorOf(Props.create(classOf[SegmentWorkerNode]), name)
      updateState(AddedWorker(new Worker(name, Some(worker))))
      if (!recovery) saveSnapshot(state)
      worker
    }

    /**
     * Builds a worker name based on granularity, given timestamp and prefix.
     * Truncates the timestamp to correspond granularity.
     *
     * @param timestamp
     * @param granularity
     * @param prefix
     * @return
     */
    def workerName(timestamp: Long, granularity: Granularity, prefix: String): String = {
      val truncatedTimestamp: Long = granularity.truncate(new DateTime(timestamp)).getMillis
      prefix + granularity.format(new DateTime(truncatedTimestamp))
    }

    /**
     * Deletes old snapshots after a new one is saved.
     */
    def deleteOldSnapshots(lastSnapshot : Option[SnapshotMetadata]): Unit =
      lastSnapshot.foreach { meta =>
        val criteria = SnapshotSelectionCriteria(meta.sequenceNr, meta.timestamp - 1)
        deleteSnapshots(criteria)
      }

    /**
     * Restores all active children based on latest snapshot
     *
     * @return
     */
    override def receiveRecover: Receive = {
      case SnapshotOffer(_, snapshot) => {
        snapshot.asInstanceOf[SegmentManagerState].activeWorkers.foreach { entry =>
          create(entry._1, recovery = true)
        }
      }
    }

    override def receiveCommand: Receive = {
      case SaveSnapshotSuccess(meta) => lastSnapshot = Some(meta); deleteOldSnapshots(lastSnapshot)
      case SaveSnapshotFailure(_, e) => log.error(e, "Snapshot write failed")
      case AddEvent(body, timestamp) =>
        log.debug("Received AddEvent: {}, {}", body, timestamp)
        if (rejectionPolicy.accept(timestamp)) {
          log.debug("Accepted timestamp: {}", timestamp)      
          val worker: ActorRef = getWorker(workerName(
            timestamp, dataSchema.getGranularity, workerNamePrefix))
          //watch a child to receive terminated when it hands off
          context.watch(worker)
          scheduleHandsOff(worker, dataSchema.getGranularity, strugglingWindow)

          worker ! AddEvent(body, timestamp)
        } else {
          log.debug("Not accepted timestamp: {}", timestamp)
        }

      case Terminated(_) =>
        log.debug("Received Terminated: {}, {}", sender().path.name)
        //the child should not be longer in a child list
        updateState(RemovedWorker(sender().path.name))
        saveSnapshot(state)

      case r: Any => unhandled(r)
    }

    override def persistenceId: String = self.path.name



  }/*

  def main(args: Array[String]): Unit = {

    val system = ActorSystem("Realtime")
    val a = system.actorOf(Props[SegmentManagerNode], "segment-manager")

    a ! AddEvent("fff", System.currentTimeMillis())

    /*val b = system.actorOf(Props.create(classOf[MessageGenerator], a), "generator")*/
  }*/
