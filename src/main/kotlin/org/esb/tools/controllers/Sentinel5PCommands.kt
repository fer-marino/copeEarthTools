package org.esb.tools.controllers

import org.esb.tools.controllers.Sentinel5PCommands.Measurement
import org.gdal.gdal.BuildVRTOptions
import org.gdal.gdal.Dataset
import org.gdal.gdal.WarpOptions
import org.gdal.gdal.gdal
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.core.convert.converter.Converter
import org.springframework.core.io.FileSystemResource
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import org.springframework.stereotype.Component
import java.nio.file.Files
import java.util.*

@ShellComponent
class Sentinel5PCommands {
    private val measurementTypes = mapOf("no2" to Measurement("no2", "nitrogendioxide_tropospheric_column", "L2__NO2___"),
            "aer_340_380" to Measurement("aer_340_380", "aerosol_index_340_380", "L2__AER_AI"),
            "aer_354_388" to Measurement("aer_354_388", "aerosol_index_354_388", "L2__AER_AI"),
            "o3" to Measurement("o3", "ozone_total_vertical_column", "L2__O3____"),
            "co" to Measurement("co", "carbonmonoxide_total_column", "L2__CO____")
    )

    @Bean
    fun getMeasurementTypes() = measurementTypes


    @ShellMethod("Merge S5P products")
    fun s5pMerge(pattern: String,
                 @ShellOption(defaultValue = "no2") measurement: Measurement,
                 @ShellOption(defaultValue = "") outputOptions: String = "",
                 @ShellOption(defaultValue = "false") force: Boolean = false,
                 @ShellOption(defaultValue = "50") qualityThreshold: Int = 50,
                 @ShellOption(defaultValue = "mosaic.tif") destination: String) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")

        val start = System.currentTimeMillis()
        val products = mutableListOf<Dataset>()
        // NOTE: do not parallelize. Apparently gdal open is NOT thread safe!
        matches.filter { it.isFile && it.filename?.matches(".*${measurement.prodType}.*\\.nc$".toRegex()) ?: false }.forEach { it ->
            // open
            val path = (it as FileSystemResource).path
            print(" * * Processing $path... ")
            val adding = gdal.Open("HDF5:\"$path\"://PRODUCT/${measurement.varName}")
            val quality = gdal.Open("HDF5:\"$path\"://PRODUCT/qa_value")

            val maskBuffer = ByteArray(quality.rasterXSize * quality.rasterYSize)
            val dataBuffer = FloatArray(adding.rasterXSize * adding.rasterYSize)

            quality.GetRasterBand(1).ReadRaster(0, 0, quality.rasterXSize, quality.rasterYSize, maskBuffer)
            adding.GetRasterBand(1).ReadRaster(0, 0, adding.rasterXSize, adding.rasterYSize, dataBuffer)

            dataBuffer.forEachIndexed { i, _ -> if (maskBuffer[i] < qualityThreshold) dataBuffer[i] = measurement.nodata}
            println(((dataBuffer.count { it != measurement.nodata } * 100F) / dataBuffer.size).toString() + "% of valid pixels ")

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
                    WarpOptions(gdal.ParseCommandLine("-srcnodata ${measurement.nodata} -dstnodata 0 " +
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
        val vrt = gdal.BuildVRT("${measurement.name}-vrt-merge", products.toTypedArray(),
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


    data class Measurement(val name: String, val varName: String, val prodType: String, val nodata: Float = 9.969209968386869E36F)

}

@Component
class MeasurementConverter : Converter<String, Measurement> {
    @Autowired
    lateinit var measurementTypes: Map<String, Measurement>

    override fun convert(source: String): Measurement? = measurementTypes[source]

}