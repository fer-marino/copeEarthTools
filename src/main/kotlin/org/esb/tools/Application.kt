package org.esb.tools

import org.gdal.gdal.gdal
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
open class Application

fun main(args: Array<String>) {
    try {
        gdal.AllRegister()
        println(" Loaded ${gdal.GetDriverCount()} GDAL drivers")
//        gdal.PushErrorHandler("CPLQuietErrorHandler")
        runApplication<Application>(*args)
    } catch (e: UnsatisfiedLinkError) {
        println("Library path: \n\t - " + System.getProperty("java.library.path").replace(";", "\n\t - "))
        error(" ${e.message}")
    }


}
