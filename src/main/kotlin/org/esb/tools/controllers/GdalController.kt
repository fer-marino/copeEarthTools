package org.esb.tools.controllers

import org.gdal.gdal.gdal
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod

@ShellComponent
class GdalController {

    @ShellMethod("List loaded GDAL drivers")
    fun listGdalDrivers() {
        for (i in 0..gdal.GetDriverCount())
            println(" ** " + gdal.GetDriver(i).longName)
    }

    @ShellMethod("Disable all GDAL errors and warnings")
    fun quietMode() {
        gdal.PushErrorHandler("CPLQuietErrorHandler")
    }

    @ShellMethod("Enable gdal error reporting")
    fun enableGdalErrors() {
        gdal.PushErrorHandler("CPLQuietErrorHandler")
        TODO("Not implemented yet")
    }

}