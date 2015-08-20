package realtime.util

import akka.actor.{Actor, ActorLogging, ActorRef}
import realtime.SegmentManagerNode.AddEvent

import scala.concurrent.duration._
import scala.util.Random

/**
 * Created by mikhail on 13/08/2015.
 */
class MessageGenerator(a: ActorRef) extends Actor with ActorLogging {

  import context.dispatcher

  val r = new Random(31)

  val alphabet = "ueiw6754aebamxaagqbk4fr9nf097j69ut29cvgf5ktm9ynf5dkd22xyxgkfqhi8mval1y93tznqjygeqmvebqcfv4z2ropzidfm"
  def randomString(alphabet: String)(n: Int): String =
    Stream.continually(r.nextInt(alphabet.size)).map(alphabet).take(n).mkString

  context.system.scheduler.schedule(400 millis, 400 millis) {
    a ! new AddEvent(randomString(alphabet)(6) + System.currentTimeMillis(), System.currentTimeMillis())
    a ! new AddEvent(randomString(alphabet)(6) + System.currentTimeMillis(), System.currentTimeMillis())
    a ! new AddEvent(randomString(alphabet)(6) + System.currentTimeMillis(), System.currentTimeMillis())
  }

  override def receive: Receive = {
    case m => log.debug("received msg {}", m)
  }
}
