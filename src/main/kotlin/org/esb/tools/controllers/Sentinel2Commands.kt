package org.esb.tools.controllers

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
            try {
                val prod = gdal.Open(it.file.absolutePath + "/MTD_MSIL1C.xml")
                val datasetName = prod.GetMetadata_Dict("SUBDATASETS")["SUBDATASET_1_NAME"].toString()
                datasets.add(gdal.Open(datasetName))
                prod.delete()
            } catch (e: Exception) {
                println(" ** Error: unable to open dataset ${it.file.absoluteFile}")
            }
        }

        println(" * Found ${datasets.size} produts mathing the provided pattern. Merging...")

        val mosaic = gdal.BuildVRT("mosaic", datasets.toTypedArray(), BuildVRTOptions(gdal.ParseCommandLine("-a_srs EPSG:4326"))) // TODO add options

//        gdal.Translate(destinationFile, mosaic, TranslateOptions(gdal.ParseCommandLine(""))).delete() // TODO add options

//        println(" ** Equalizing...")

        println("Translating....")
        gdal.Translate(destinationFile, mosaic, TranslateOptions(gdal.ParseCommandLine("-a_srs EPSG:4326 -tr 10 10 -co COMPRESS=LZW"))).delete() // TODO add options


        // wrap up
        mosaic.delete()
        datasets.forEach { it.delete() }

        println(" * Mosaic completed in ${(System.currentTimeMillis() - start) / 1000} seconds")
    }


    fun naturalColorsCloudFree() {

    }


}