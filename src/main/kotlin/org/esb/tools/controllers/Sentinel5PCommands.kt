package org.esb.tools.controllers

import org.gdal.gdal.*
import org.springframework.core.io.FileSystemResource
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import java.nio.file.Files
import java.util.*

@ShellComponent
class Sentinel5PCommands {
    @ShellMethod("Merge S5P products")
    fun mergeNO2(pattern: String,
                 @ShellOption(defaultValue = "") outputOptions: String = "",
                 @ShellOption(defaultValue = "false") force: Boolean = false,
                 @ShellOption(defaultValue = "50") qualityThreshold:  Int = 50,
                 @ShellOption(defaultValue = "mosaic-no2.tif") destination: String) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")

        val start = System.currentTimeMillis()
        val products = mutableListOf<Dataset>()
        // NOTE: do not parallelize. Apparently gdal open is NOT thread safe!
        matches.filter { it.isFile && it.filename?.matches(".*L2__NO2.*\\.nc$".toRegex()) ?: false }.forEach { it ->
            // open
            val path = (it as FileSystemResource).path
            print(" * * Processing $path... ")
            val adding = gdal.Open("HDF5:\"$path\"://PRODUCT/nitrogendioxide_tropospheric_column")
            val quality = gdal.Open("HDF5:\"$path\"://PRODUCT/qa_value")

            val maskBuffer = ByteArray(quality.rasterXSize * quality.rasterYSize)
            val dataBuffer = FloatArray(adding.rasterXSize * adding.rasterYSize)

            quality.GetRasterBand(1).ReadRaster(0, 0, quality.rasterXSize, quality.rasterYSize, maskBuffer)
            adding.GetRasterBand(1).ReadRaster(0, 0, adding.rasterXSize, adding.rasterYSize, dataBuffer)

            dataBuffer.forEachIndexed { i, _ -> if (maskBuffer[i] < qualityThreshold) dataBuffer[i] = 9.969209968386869E36F }
            println(((dataBuffer.count { it != 9.969209968386869E36F } *100F) / dataBuffer.size).toString() + "% of valid pixels ")

            val copy = gdal.GetDriverByName("MEM").CreateCopy("copy", adding)
            copy.GetRasterBand(1).WriteRaster(0, 0, adding.rasterXSize, adding.rasterYSize, dataBuffer)

            quality.delete()
            adding.delete()

            // add georeferencing
            val mapA = mapOf(
                    "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                    "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                    "X_BAND" to "1", "X_DATASET" to "HDF5:$path://PRODUCT/longitude",
                    "Y_BAND" to "1", "Y_DATASET" to "HDF5:$path://PRODUCT/latitude"
            )
            copy.SetMetadata(Hashtable(mapA), "GEOLOCATION")
            val ris = gdal.Warp("$path-warp.tif", arrayOf(copy),
                    WarpOptions(gdal.ParseCommandLine("-srcnodata 9.969209968386869E36 -dstnodata 0 " +
                            "-t_srs EPSG:4326 -tr 0.02 0.02 -te -180 -85 180 85 " +
                            "-geoloc -oo COMPRESS=LZW -wo NUM_THREADS=val/ALL_CPUS -overwrite")))
            products.add(ris)
            copy.delete()
        }

        if (products.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

        println(" * Found ${products.size} products for pattern $pattern. Merging...")
        val vrt = gdal.BuildVRT("no2-vrt-merge", products.toTypedArray(),
                BuildVRTOptions(gdal.ParseCommandLine("-resolution average -srcnodata 0 -vrtnodata 0")))

        val mosaic = gdal.Warp(destination, arrayOf(vrt),
                WarpOptions(gdal.ParseCommandLine("-srcnodata 0 -dstnodata -1 -overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=val/ALL_CPUS")))

        products.forEach { it.delete() }
        vrt.delete()

//        println(gdal.GDALInfo(mosaic, InfoOptions(gdal.ParseCommandLine("-hist"))))

//        println(" ** completed in ${(System.currentTimeMillis() - start) / 1000} seconds. Applying color map...")
//        gdal.DEMProcessing("color-mosaic.tif", mosaic, "color-relief", "no2-color-map.txt",
//                DEMProcessingOptions(gdal.ParseCommandLine("-alpha -co COMPRESS=JPEG"))).delete()

        mosaic.delete()

        Files.list(matches.first().file.toPath().parent).filter { Files.isRegularFile(it) && !it.toString().endsWith("nc") }.forEach { Files.delete(it) }

        println(" ** DONE in ${(System.currentTimeMillis() - start) / 1000} seconds ")
    }

}