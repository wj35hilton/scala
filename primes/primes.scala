import akka.actor._
import akka.routing.RoundRobinRouter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

// 
// Various prime generators.
//
// run with: 
//    scala -cp akka-actor_2.11-2.3.4.jar primes.scala
//
object Primes {
  def main(args: Array[String]) {

    // quick sanity checks
    assert(ser001(1000) == ser002(1000))
    assert(ser002(1000) == ser003(1000))
    assert(ser003(1000) == futures(1000))
    assert(futures(1000) == akkaPooled(1000))

    // average run times
    println("ser001: " + average (() => time (ser001(10000))))
    println("ser002: " + average (() => time (ser002(10000))))
    println("ser003: " + average (() => time (ser002(10000))))
    println("futures: " + average (() => time (futures(10000))))
    println("akkaPooled: " + average (() => time (akkaPooled(10000))))
  }

  //
  // borrowed utilities
  //
  def time[A](a: => A) = { val t = System.currentTimeMillis; a; System.currentTimeMillis - t}
  def average(a: () => Long) = (for(i<-1 to 100) yield a()).sum/100

  //
  // Prime testers
  //

  // simple prime tester
  def isPrime(n:Int) = !((2 until n) exists (n % _ == 0))

  // should be a bit faster, but may return non-primes
  def isProbablePrime(n:Int) = java.math.BigInteger.valueOf(n).isProbablePrime(1000)


  // 
  // Some simple (non-concurrent) prime generators 
  //

  // The first two simply walk through the list testing each value.
  // This is fairly inefficient but trivial to distribute.

  def ser001(n:Int) = (2 until n) filter (isPrime(_))

  def ser002(n:Int) = (2 until n) filter (isProbablePrime(_))

  // The following maintains a list of values for which no factors could be found and
  // uses this list for testing the current value.
  // This reduces algorithmic complexity by a factor, but is more difficult to 
  // distribute.  In particular, state (the list of primes) would need to be either
  // shared or passed to each worker.  Additionally, some care would need to be
  // taken with the 'batch' size as we don't want to test a value for which a 
  // factor exists but may not have yet been found.

  def ser003(n:Int) = {
    var primeAcc:List[Int] = List()
    for (i <- 2 to n - 1) {
      if (primeAcc.find(i % _ == 0) == None) {
        primeAcc = primeAcc :+ i
      }
    }
    primeAcc
  }




  //
  // Concurrent -- use the simpler algorithm from above and 
  // farm work out to as many available threads as possible
  //

  def futures(n:Int) = {
    val workers = for (i <- 2 to n - 1) yield future {
      if (isPrime(i)) Some(i) else None
    }
    Await.result(Future.sequence(workers), 30.seconds).flatten
  }


  //
  // Concurrent/distributed implementation of simple prime solver w/akka.
  //

  // -- akka message types
  case class Test(n: Int)
  case class Result(ip: Option[Int])
  case class Primes(primes: List[Int])
  case class Start()
 
  // -- actors -- 
  // tests a individual Int for prime-ness
  class Tester extends Actor {
    def receive = {
      case Test(n) ⇒ sender ! Result(if (isPrime(n)) Some(n) else None)
    }
  }

  // coordinates worker pool
  class Master(numToTest: Int, listener: ActorRef) extends Actor {
    val testers = context.actorOf(Props[Tester].withRouter(RoundRobinRouter(10)), name = "primesTestRouter")

    var numResults: Int = _
    var results: List[Int] = List()

    def receive = {
      case Start => for (i <- 2 until numToTest) testers ! Test(i)
      case Result(ip) => {
        numResults += 1

        if (ip.isDefined) {
          results = ip.get :: results
        }

        if (numResults == (numToTest - 2)) {
          listener ! Primes(results)
        }
      }
    }
  }

  // awaits final result
  class Listener(p:Promise[List[Int]]) extends Actor {
    def receive = {
      case Primes(primes) => {
        p.success(primes.sorted)
        context.system.shutdown()
      }
    }
  }
 
  def akkaPooled(n:Int) : List[Int] = {
    val primes = Promise[List[Int]]

    val system = ActorSystem("primeGenerator")
    val resListener = system.actorOf(Props(new Listener(primes)), name = "resListener")
    val master = system.actorOf(Props(new Master(n, resListener)), name = "master")

    master ! Start

    Await.result(primes.future, 30.seconds)
  }
}

