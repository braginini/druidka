package realtime

import realtime.RealtimeManager.InputRow

/**
 * Created by mikhail on 10/08/2015.
 */
class IncrementalIndex extends Serializable {

  def addRow(row : InputRow): Unit = {
    //todo some update
  }

  override def toString : String = "dummy-index"

}
