package de.pc2.dedup.fschunk

import scala.actors.Actor
import scala.actors.Actor._
import scala.actors._
import scala.actors.Exit
import de.pc2.dedup.util._

class Reporter(baseActor: Actor, reportInterval: Int) extends Actor with Log {
	trapExit = true

	def act() {      
		if(reportInterval <= 0) {
			exit()
		}
		link(baseActor)

		while(true) {
			receiveWithin(reportInterval) {
			case TIMEOUT =>
				baseActor ! Report
			case Exit(actor,reason) =>
				exit()
			case msg: Any =>
				logger.error("Unknown Message" + msg)
			}
		}
	}
}
