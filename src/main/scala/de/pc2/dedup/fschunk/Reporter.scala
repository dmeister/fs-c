package de.pc2.dedup.fschunk

import java.text.NumberFormat
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import de.pc2.dedup.util.Log

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
    sb.append(" MB, allocated: " + format.format(allocatedMemory / 1024));
    sb.append(" MB, max: " + format.format(maxMemory / 1024));
    sb.append(" MB, total free: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024));
    sb.append(" MB")

    logger.info(sb)
  }

  def report() {
    doReport();
  }
}

class ReporterRunnable(r: Reporting) extends Runnable {
  def run() {
    r.report()
  }
}

/**
 * Actor that in certain intervals calls report on the reporting object until a Quit message is
 * received
 */
class Reporter(r: Reporting, reportInterval: Option[Int]) extends Log {
  val tp = Executors.newScheduledThreadPool(1)

  
  def start() : Reporter = {
    val interval = reportInterval match {
      case None => 60
      case Some (i) => i
    }
      tp.scheduleAtFixedRate(new ReporterRunnable(r), 1, interval, TimeUnit.SECONDS)
      this
  }
  
  def quit() {
    tp.shutdown()
  }
  
}

