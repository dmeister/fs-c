package de.pc2.dedup.fschunk

import scala.actors.Actor
import scala.actors.Actor._
import scala.actors._
import scala.actors.Exit
import de.pc2.dedup.util._

trait Reporting {
  def report()
}

class Reporter(r: Reporting, reportInterval: Int) extends Actor with Log {
  def act() {
    if (reportInterval <= 0) {
      exit()
    }

    while (true) {
      receiveWithin(reportInterval) {
        case TIMEOUT =>
          r.report()
        case Quit =>
          exit()
        case msg: Any =>
          logger.error("Unknown Message" + msg)
      }
    }
  }
}

