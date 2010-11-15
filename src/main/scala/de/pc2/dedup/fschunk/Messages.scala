package de.pc2.dedup.fschunk

case object Quit 
case object Report
case class FileError(filename: String, fileSize: Long) 