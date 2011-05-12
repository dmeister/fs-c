package de.pc2.dedup.util

import scala.actors._

trait ActorUtil extends Actor with Log {
    val THREASHOLD = 10000
    val ITERATIONS = 60
    val SLEEPTIME = 1000

    var stepDowns = 0L

    def stepDownCheck(a: Actor) : Boolean = {
        def stepDownCheck(a: Actor, i: Int) : Boolean = {
            if(i > ITERATIONS) {
                logger.warn("Liveness warning for %s".format(a))
                return true
            }
            if(a.mailboxSize > THREASHOLD) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Stepping down (%s mailbox size %d)".format(a,a.mailboxSize))
                }
                // slow down
                receiveWithin(1000) {
                    case TIMEOUT =>

                }
                stepDownCheck(a, i + 1)
                return true		
            }
            return false
        }
        val steppedDown = stepDownCheck(a, 0)
        if(steppedDown) {
            stepDowns += 1
        }
        steppedDown
    }

    def clearMailbox() {
        receiveWithin(10) {
            case msg : Any =>
        }
    }
}