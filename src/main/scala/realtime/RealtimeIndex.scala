package realtime

import akka.actor.ActorLogging
import akka.persistence.{SaveSnapshotFailure, SaveSnapshotSuccess, PersistentActor}
import realtime.RealtimeManager.InputRow

/**
 * Created by mikhail on 10/08/2015.
 */
object RealtimeIndex {

  case class Persist() //cmd to save snapshot

  case class HandsOff() //cmd to stop handling index and load to deep storage

  case class Persisted(failed : Boolean)//msg saying whether persistence (snapshot) was successful or not

  case class HandsOffed(failed: Boolean) //msg saying whether the segment was loaded to deep storage or not

  class RealtimeIndex extends PersistentActor with ActorLogging {

    var state : IncrementalIndex = new IncrementalIndex

    var retries = 3;

    //updates the current index
    def updateState(inputRow: InputRow): Unit = {
      state.addRow(inputRow)
    }

    override def receiveRecover = {
      case r: InputRow => updateState(r)
    }

    override def receiveCommand: Receive = {
      case InputRow(body, timestamp) =>
        log.debug("Received new input row: {}, {}", body, timestamp)
        updateState(InputRow(body, timestamp))
      case Persist() =>
        log.debug("Received persist")
        retries -= 1
        saveSnapshot(state)
      case SaveSnapshotSuccess(metadata) =>
        log.debug("Saved snapshot")
        context.parent ! new Persisted(true)
      case SaveSnapshotFailure(metadata, reason) =>
        log.debug("Not persisted {}", reason)
        if (retries > 0) {
          retries -= 1
          saveSnapshot(state)
        } else {
          context.parent ! new Persisted(false)
        }
      case HandsOff() =>
        log.debug("Received hand off")
        //todo add async load to deep storage
        context.parent ! HandsOffed(true)
        context.stop(self)
    }

    override def persistenceId: String = self.path.name
  }

}
