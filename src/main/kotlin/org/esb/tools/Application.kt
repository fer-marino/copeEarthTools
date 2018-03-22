package org.esb.tools

import org.gdal.gdal.gdal
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
open class Application

fun main(args: Array<String>) {
    gdal.AllRegister()
//    gdal.PushErrorHandler("CPLQuietErrorHandler")
    runApplication<Application>(*args)
}





