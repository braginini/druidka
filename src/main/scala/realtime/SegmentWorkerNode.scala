package realtime

import akka.actor.ActorLogging
import akka.persistence._
import org.joda.time.Period
import realtime.SegmentManagerNode.{HandsOff, Persist, AddEvent}
import realtime.SegmentWorkerNode.IndexReady
import realtime.index.SegmentIndex
import scala.concurrent.duration._

/**
 * Created by mikhail on 10/08/2015.
 */
object SegmentWorkerNode {

  case object Persisted

  //successful persistence
  case object NotPersisted

  //a message indicating that the segment was closed and ready to be sent to historical nodes
  case class IndexReady(indexId: String)

  //unsuccessful persistence
}

class SegmentWorkerNode extends PersistentActor with ActorLogging {

  import context.dispatcher

  val persistDelay = new Period(0, 0, 60, 0) //10 secs

  //schedule initial self persistence
  context.system.scheduler.scheduleOnce(
    persistDelay.toStandardDuration.getMillis millis) {
    self ! Persist
  }

  /**
   * indicates whether the node is being stopped by a parent (received hands off)
   */
  var handsOff: Boolean = false

  var state: SegmentIndex = new SegmentIndex

  /**
   * The most recently saved snapshot. We store it on save and recover,
   * so that we can delete it (and any earlier snapshots) each time we
   * successfully save a new snapshot.
   */
  var lastSnapshot: Option[SnapshotMetadata] = None

  /**
   * keeping the timestamp when the last event arrived to avoid unnecessary snapshots
   */
  var lastEvent: Long = 0

  //updates the current index
  def updateState(inputRow: AddEvent): Unit = {
    state.addRow(inputRow)
  }

  /**
   * when we receive recovery we read the segment from persistent storage
   *
   * @return
   */
  override def receiveRecover = {
    case SnapshotOffer(meta, snapshot) =>
      lastSnapshot = Some(meta)
      state = snapshot.asInstanceOf[SegmentIndex]
  }


  override def receiveCommand: Receive = {
    case AddEvent(body, timestamp) =>
      log.debug("Received AddEvent: {}, {}", body, timestamp)
      lastEvent = System.currentTimeMillis()
      updateState(AddEvent(body, timestamp))
    case Persist =>
      log.debug("Received Persist")
      if (!handsOff && (lastSnapshot.isEmpty || lastEvent > lastSnapshot.get.timestamp))
        saveSnapshot(state)
      else log.debug("Nothing to persist")
    case SaveSnapshotSuccess(metadata) =>
      log.debug("Received SaveSnapshotSuccess {}", metadata)
      lastSnapshot = Some(metadata)
      deleteOldSnapshots()
      if (handsOff)
        context.parent ! IndexReady(persistenceId)
      //context.stop(self)
      else {
        //reschedule persistence
        context.system.scheduler.scheduleOnce(
          persistDelay.toStandardDuration.getMillis millis) {
          self ! Persist
        }
      }
    case SaveSnapshotFailure(metadata, reason) =>
      log.error("Received SaveSnapshotFailure {}", reason)
    case HandsOff =>
      log.debug("Received HandsOff")
      handsOff = true
      saveSnapshot(state)
  }

  override def persistenceId: String = self.path.name

  /**
   * Deletes old snapshots after a new one is saved.
   */
  def deleteOldSnapshots(stopping: Boolean = false): Unit =
    lastSnapshot.foreach { meta =>
      val criteria = SnapshotSelectionCriteria(meta.sequenceNr, meta.timestamp - 1)
      deleteSnapshots(criteria)
    }
}
