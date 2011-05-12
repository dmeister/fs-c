package de.pc2.dedup.util
import java.io.File

object FileType {
    val NO_TYPE = "---"
		
    def getType(file: File) : String = {
        val i = file.getName().lastIndexOf(".")
        if(i < 0) {
            return NO_TYPE
        }
        val fileType = file.getName().substring(i + 1)
        if(fileType.length > 5) {
            return NO_TYPE
        } else {
            return fileType
        }
    }
    def getNormalizedFiletype(file: File) : String = {
        getNormalizedFiletype(getType(file)) 
    }
    def getNormalizedFiletype(filetype: String) : String = { 
        if(filetype.length() == 0) {
            return NO_TYPE
        }
        val normalizedFiletype = filetype.toLowerCase().replace(":", "-")
        if(normalizedFiletype.endsWith("~")) {
            return normalizedFiletype.substring(0, normalizedFiletype.length() - 1)
        }
        normalizedFiletype 
    }
}
