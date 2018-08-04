package org.esb.tools.controllers

import org.gdal.gdal.BuildVRTOptions
import org.gdal.gdal.Dataset
import org.gdal.gdal.WarpOptions
import org.gdal.gdal.gdal
import org.springframework.core.io.FileSystemResource
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import java.util.*

@ShellComponent
class Sentinel5PController {
    @ShellMethod("Merge S5P products")
    fun mergeNO2(pattern: String,
                 @ShellOption(defaultValue = "") outputOptions: String = "",
                 @ShellOption(defaultValue = "false") force: Boolean = false,
                 @ShellOption(defaultValue = "mosaic-no2.tif") destination: String) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")

        val start = System.currentTimeMillis()
        val products = mutableListOf<Dataset>()
        matches.filter { it.isFile && it.filename?.matches(".*L2__NO2.*\\.nc$".toRegex()) ?: false }.parallelStream().forEach {
            // open
            val path = (it as FileSystemResource).path
            val adding = gdal.Open("HDF5:\"$path\"://PRODUCT/nitrogendioxide_tropospheric_column")
            val quality = gdal.Open("HDF5:\"$path\"://PRODUCT/qa_value")

            val maskBuffer = ByteArray(quality.rasterXSize * quality.rasterYSize)
            val dataBuffer = FloatArray(adding.rasterXSize * adding.rasterYSize)

            quality.GetRasterBand(1).ReadRaster(0, 0, quality.rasterXSize, quality.rasterYSize, maskBuffer)
            adding.GetRasterBand(1).ReadRaster(0, 0, adding.rasterXSize, adding.rasterYSize, dataBuffer)

            dataBuffer.forEachIndexed { i, _ -> if (maskBuffer[i] < 75) dataBuffer[i] = 0F }

            adding.GetRasterBand(1).WriteRaster(0, 0, adding.rasterXSize, adding.rasterYSize, dataBuffer)

            quality.delete()

            // add georeferencing
            val mapA = mapOf(
                    "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                    "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                    "X_BAND" to "1", "X_DATASET" to "HDF5:$path://PRODUCT/longitude",
                    "Y_BAND" to "1", "Y_DATASET" to "HDF5:$path://PRODUCT/latitude"
            )
            adding.SetMetadata(Hashtable(mapA), "GEOLOCATION")
            val ris = gdal.Warp("$path-warp.tif", arrayOf(adding),
                    WarpOptions(gdal.ParseCommandLine("-srcnodata 9.969209968386869E36 -dstnodata 0 -t_srs EPSG:4326 -tr 0.02 0.02 -te -180 -85 180 85 " +
                            "-geoloc -oo COMPRESS=LZW -wo NUM_THREADS=val/ALL_CPUS -overwrite")))
            adding.GetRasterBand(1).GetMaskBand()
            // fixme synchronize add
            products.add(ris)
        }

        if (products.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

        println(" * Found ${products.size} products for pattern $pattern. Merging...")
        val vrt = gdal.BuildVRT("no2-vrt-merge", products.toTypedArray(), BuildVRTOptions(gdal.ParseCommandLine("-resolution average -srcnodata 0 -vrtnodata 0")))

        val mosaic = gdal.Warp(destination, arrayOf(vrt),
                WarpOptions(gdal.ParseCommandLine("-srcnodata 0 -dstnodata -1 -overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=val/ALL_CPUS")))

        products.forEach { it.delete() }
        vrt.delete()

//        println(gdal.GDALInfo(mosaic, InfoOptions(gdal.ParseCommandLine("-hist"))))
//
//        println(" ** completed in ${(System.currentTimeMillis() - start) / 1000} seconds. Applying color map...")
//        gdal.DEMProcessing("color-mosaic.tif", mosaic, "color-relief", "no2-color-map.txt", DEMProcessingOptions(gdal.ParseCommandLine("-alpha -co COMPRESS=JPEG"))).delete()

        mosaic.delete()

//        Files.list(matches.first().file.toPath().parent).filter { !it.endsWith("nc") }.forEach { Files.delete(it) }

        println(" ** DONE in ${(System.currentTimeMillis() - start) / 1000} seconds ")
    }

}