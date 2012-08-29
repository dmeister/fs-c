package de.pc2.dedup.fschunk

import scala.actors.Actor
import scala.actors.Actor._
import scala.actors._
import scala.actors.Exit
import de.pc2.dedup.util._
import java.text.NumberFormat

/**
 * Trait of objects that support reporting.
 */
trait Reporting {
  def report()
}

class GCReporting extends Reporting with Log {
  def doReport() {
    val runtime = Runtime.getRuntime();

    val format = NumberFormat.getInstance();

    val sb = new StringBuilder();
    val maxMemory = runtime.maxMemory();
    val allocatedMemory = runtime.totalMemory();
    val freeMemory = runtime.freeMemory();

    sb.append("Memory: free: " + format.format(freeMemory / 1024));
    sb.append(" MB , allocated: " + format.format(allocatedMemory / 1024));
    sb.append(" MB , max: " + format.format(maxMemory / 1024));
    sb.append(" MB , total free: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024));
    sb.append(" MB")

    logger.info(sb)
  }

  def report() {
    doReport();
  }
}

/**
 * Actor that in certain intervals calls report on the reporting object until a Quit message is
 * received
 */
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

