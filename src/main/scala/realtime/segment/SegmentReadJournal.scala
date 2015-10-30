package realtime.segment

import akka.actor.ExtendedActorSystem
import akka.persistence.query.javadsl.ReadJournal

/**
 * Created by mikhail on 17/09/2015.
 */
object SegmentReadJournal {

  class SegmentReadJournal(system: ExtendedActorSystem) extends ReadJournal {

  }

}
