package org.esb.tools.controllers

import filters.ClaheFilter
import org.gdal.gdal.BuildVRTOptions
import org.gdal.gdal.Dataset
import org.gdal.gdal.TranslateOptions
import org.gdal.gdal.gdal
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod

@ShellComponent
class Sentinel2Commands {

    @ShellMethod("Build a natural color mosaic")
    fun naturalColorsMosaic(extent: String, pattern: String, destinationFile: String) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
        if (matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

        val start = System.currentTimeMillis()
        // heavy on memory. Are we sure?
        val datasets = mutableListOf<Dataset>()
        matches.filter { it.isFile }.forEach {
            datasets.add(gdal.Open(it.file.absolutePath))
        }

        println(" * Found ${datasets.size} produts mathing the provided pattern. Merging...")

        val mosaic = gdal.BuildVRT("mosaic", datasets.toTypedArray(), BuildVRTOptions(gdal.ParseCommandLine(""))) // TODO add options

//        gdal.Translate(destinationFile, mosaic, TranslateOptions(gdal.ParseCommandLine(""))).delete() // TODO add options

        println(" ** Equalizing...")
        ClaheFilter(64, 255).apply(mosaic)

        gdal.Translate(destinationFile, mosaic, TranslateOptions(gdal.ParseCommandLine(""))).delete() // TODO add options


        // wrap up
        mosaic.delete()
        datasets.forEach { it.delete() }

        println(" * Mosaic completed in ${(System.currentTimeMillis() - start) / 1000} seconds")
    }


    fun naturalColorsCloudFree() {

    }


}