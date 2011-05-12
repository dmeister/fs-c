package de.pc2.dedup.util

import org.apache.commons.logging._

/**
 * Logging Helper trait
 * see: Blog Post by Sean Hunter, http://www.uncarved.com/blog/LogHelper.mrk
 */
trait Log {
    val loggerName = getLoggerName()
    lazy val logger = LogFactory.getLog(loggerName)
    
    def getLoggerName() : String = {
        val name = this.getClass.getName
        val i = name.indexOf("$")
        val name2 = if(i >= 0) {
            name.substring(0, i - 1)
        } else {
            name
        }
        val j = name2.lastIndexOf(".")
        val name3 = if(j >= 0) {
            name2.substring(j + 1)
        } else {
            name2
        }
        name3
    }
}
