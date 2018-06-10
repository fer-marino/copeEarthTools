package org.esb.tools

import org.esb.tools.configuration.DataHubConfiguration
import org.gdal.gdal.gdal
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableConfigurationProperties(DataHubConfiguration::class)
open class Shell

fun main(args: Array<String>) {
    try {
        gdal.AllRegister()
        println(" Loaded ${gdal.GetDriverCount()} GDAL drivers")
//        gdal.PushErrorHandler("CPLQuietErrorHandler")
        runApplication<Shell>(*args)
    } catch (e: UnsatisfiedLinkError) {
        println("Library path: \n\t - " + System.getProperty("java.library.path").replace(";", "\n\t - "))
        error(" ${e.message}")
    }


}
