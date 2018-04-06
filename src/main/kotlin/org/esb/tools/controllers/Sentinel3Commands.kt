package org.esb.tools.controllers

import org.esb.tools.Utils
import org.esb.tools.Utils.Companion.monitorFile
import org.gdal.gdal.BuildVRTOptions
import org.gdal.gdal.DEMProcessingOptions
import org.gdal.gdal.InfoOptions
import org.gdal.gdal.TranslateOptions
import org.gdal.gdal.WarpOptions
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ucar.ma2.ArrayDouble
import ucar.ma2.ArrayFloat
import ucar.ma2.ArrayShort
import ucar.ma2.DataType
import ucar.nc2.Dimension
import ucar.nc2.NetcdfFileWriter
import ucar.nc2.dataset.NetcdfDataset
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

@ShellComponent
class Sentinel3Commands {

    @ShellMethod("Convert and merge multiple OCN products")
    fun lstMerge(pattern: String,
                 @ShellOption(defaultValue = "-projwin 5 50 24 35") outputOptions: String = "",
                 @ShellOption(defaultValue = "false")force: Boolean = false) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
        if (matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

        val ascending = mutableListOf<String>()
        val descending = mutableListOf<String>()

        matches.filter { it.isFile }.forEach {
            rebuildLST(it.file.absolutePath, force)
            if ( Utils.isAscending(it.file.absolutePath) )
                ascending.add(it.file.absolutePath + "/lst_warp_rebuild.tif")
            else
                descending.add(it.file.absolutePath + "/lst_warp_rebuild.tif")
        }

        ascending.sort()
        descending.sort()
        print(" * Merging...")
        val start = System.currentTimeMillis()
        val asc = gdal.BuildVRT("mergea", Vector(ascending), BuildVRTOptions( gdal.ParseCommandLine("-resolution average")) )
        val desc = gdal.BuildVRT("merged", Vector(descending), BuildVRTOptions( gdal.ParseCommandLine("-resolution average")) )

        val da = gdal.Translate("ascending.tif", asc, TranslateOptions( gdal.ParseCommandLine(outputOptions) ) )
        val dd = gdal.Translate("descending.tif", desc, TranslateOptions( gdal.ParseCommandLine(outputOptions) ) )

        monitorFile("ascending.tif", 160000)
        monitorFile("descending.tif", 160000)

        gdal.Warp("ascending-warp.tif", arrayOf(da), WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=3")))
        monitorFile("ascending-warp.tif", 190000)

        gdal.Warp("descending-warp.tif", arrayOf(dd), WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=3")))
        monitorFile("descending-warp.tif", 190000)

        desc.delete()
        asc.delete()

        desc.delete()
        asc.delete()

        println(" done in ${System.currentTimeMillis() - start} msec")
    }

    @ShellMethod("Convert and merge multiple OGVI products")
//    fun ogviMerge(pattern: String, @ShellOption(defaultValue = "-projwin 5 50 24 35") outputOptions: String = "") {
//    fun ogviMerge(pattern: String, outputOptions: String = "", shpFile: String) {
    fun ogviMerge(pattern: String, @ShellOption(defaultValue = "") outputOptions: String = "") {

        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
                .map { it.file.absolutePath + "/ogvi_warp_rebuild.tif" }.sorted()
        if (matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }


        print(" * Merging...")
        val dsVrt = gdal.BuildVRT("merged", Vector(matches), BuildVRTOptions( gdal.ParseCommandLine("-resolution average")) )
        val dd = gdal.Translate("ogvi.tif", dsVrt, TranslateOptions( gdal.ParseCommandLine(outputOptions) ) )

        monitorFile("ogvi.tif", 60000)

        gdal.Warp("ogvi-warp.tif", arrayOf(dd), WarpOptions(gdal.ParseCommandLine("-co COMPRESS=LZW -s_srs EPSG:4326  -crop_to_cutline -cutline  it_10km.shp")))

        monitorFile("ogvi-warp.tif", 90000)

        postprocess("ogvi-warp.tif", 1.0, 1)

        dsVrt.delete()
        println("complete")
    }

    @ShellMethod("Convert LST products")
    fun rebuildLST(prodName: String, force: Boolean = false) {
        if(!force && Files.exists(Paths.get(prodName, "lst_warp_rebuild.tif")) && Files.size(Paths.get(prodName, "lst_warp_rebuild.tif")) > 50000) return

        print(" * Converting $prodName... ")
        val lstFile = NetcdfDataset.openDataset("$prodName/LST_in.nc")
        val geodeticFile = NetcdfDataset.openDataset("$prodName/geodetic_in.nc")
        val flags = NetcdfDataset.openDataset("$prodName/flags_in.nc")

        val lstData = lstFile.findVariable("LST").read() as ArrayFloat.D2
        val latData = geodeticFile.findVariable("latitude_in").read() as ArrayDouble.D2
        val lonData = geodeticFile.findVariable("longitude_in").read() as ArrayDouble.D2
        val confidenceIn = flags.findVariable("confidence_in").read() as ArrayShort.D2

        val shape = lstData.shape

        // convert float to short

        var cloud = 0

        for (y in 0 until lstData.shape[0])
            for (x in 0 until lstData.shape[1])
                when {
                    !(x in 30..lstData.shape[1]-30 || y in 30..lstData.shape[0]-30) -> lstData[y, x] = 0f // stay away from borders
                    lstData[y, x].isNaN() -> lstData[y, x] = 0f // no data
                    DataType.unsignedShortToInt(confidenceIn[y, x]) and 16384 == 16384 -> {
                        lstData[y, x] = 0f
                        cloud++
                    }
                }

        print("cloudy pixels ${(cloud.toDouble() / (shape[0] * shape[1])*100).format(2)}%... ")

        val dimensions = lstFile.findVariable("LST").dimensions

        val writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted.nc")

        val newDimensions = mutableListOf<Dimension>(
            writer.addDimension(null, dimensions[0].fullName, dimensions[0].length),
            writer.addDimension(null, dimensions[1].fullName, dimensions[1].length)
        )

        // populate
        val lstn = writer.addVariable(null, "surface_temperature", DataType.FLOAT, newDimensions)
        lstn.addAll(lstFile.findVariable("LST").attributes)

        val lat = writer.addVariable(null, "lat", DataType.DOUBLE, newDimensions)
        lat.addAll(geodeticFile.findVariable("latitude_in").attributes)

        val lon = writer.addVariable(null, "lon", DataType.DOUBLE, newDimensions)
        lon.addAll(geodeticFile.findVariable("longitude_in").attributes)

//        writer.addGroupAttribute(null, Attribute("Conventions", "CF-1.0"))

        // create the file
        try {
            writer.create()
            writer.write(lstn, lstData)
            writer.write(lat, latData)
            writer.write(lon, lonData)
        } catch (e: IOException) {
            print("ERROR creating file $prodName/reformatted.nc: ${e.message}")
        }

        writer.close()
        lstFile.close()
        geodeticFile.close()
        flags.close()

        val wgs84 = SpatialReference()
        wgs84.ImportFromEPSG(4326)

        val lst = gdal.Open("NETCDF:$prodName/reformatted.nc:surface_temperature")
        val map = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted.nc:lat"
        )

        lst.SetMetadata(Hashtable(map), "GEOLOCATION")

        val ris = gdal.Warp("$prodName/lst_warp_rebuild.tif", arrayOf(lst), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata 0")))
        monitorFile("$prodName/lst_warp_rebuild.tif", 60000)
        lst.delete()
        ris.delete()
        println("done")
    }

    @ShellMethod("Convert OGVI products")
//    fun rebuildOGVI(prodName: String, shpFile: String) {
    fun rebuildOGVI(prodName: String) {
        if (Files.exists(Paths.get(prodName, "ogvi_warp_rebuild.tif")) && Files.size(Paths.get(prodName, "ogvi_warp_rebuild.tif")) > 50000) return

        print(" * Converting $prodName... ")
        val ogviFile = NetcdfDataset.openDataset("$prodName/ogvi.nc")
        val geodeticFile = NetcdfDataset.openDataset("$prodName/geo_coordinates.nc")
//        val flags = NetcdfDataset.openDataset("$prodName/flags_in.nc")

        val ogviData = ogviFile.findVariable("OGVI").read() as ArrayFloat.D2
        val latData = geodeticFile.findVariable("latitude").read() as ArrayDouble.D2
        val lonData = geodeticFile.findVariable("longitude").read() as ArrayDouble.D2
//        val confidenceIn = flags.findVariable("confidence_in").read() as ArrayShort.D2

        val shape = ogviData.shape

        // convert float to short
        val ogviDataConv = ArrayShort.D2(shape[0], shape[1])
//        var cloud = 0

        for (y in 0 until ogviData.shape[0])
            for (x in 0 until ogviData.shape[1])
                when {
                    !(x in 30..ogviData.shape[1]-30 || y in 30..ogviData.shape[0]-30) -> ogviDataConv[y, x] = -32767 // stay away from borders
                    ogviData[y, x].isNaN() -> ogviDataConv[y, x] = -32767 // no data
//                    DataType.unsignedShortToInt(confidenceIn[y, x]) and 16384 == 16384 -> {
//                        ogviDataConv[y, x] = -32767
//                        cloud++
//                    }
                    else -> ogviDataConv[y, x] = ogviData[y, x].toShort()
                }

//        print("cloudy pixels ${(cloud.toDouble() / (shape[0] * shape[1])*100).format(2)}%... ")

        val dimensions = ogviFile.findVariable("OGVI").dimensions

        val writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted.nc")

        val newDimensions = mutableListOf<Dimension>(
                writer.addDimension(null, dimensions[0].fullName, dimensions[0].length),
                writer.addDimension(null, dimensions[1].fullName, dimensions[1].length)
        )

        // populate
        val ogvin = writer.addVariable(null, "olci_global_vegetation_index", DataType.SHORT, newDimensions)
        ogvin.addAll(ogviFile.findVariable("OGVI").attributes)

        val lat = writer.addVariable(null, "lat", DataType.DOUBLE, newDimensions)
        lat.addAll(geodeticFile.findVariable("latitude").attributes)

        val lon = writer.addVariable(null, "lon", DataType.DOUBLE, newDimensions)
        lon.addAll(geodeticFile.findVariable("longitude").attributes)

//        writer.addGroupAttribute(null, Attribute("Conventions", "CF-1.0"))

        // create the file
        try {
            writer.create()
            writer.write(ogvin, ogviDataConv)
            writer.write(lat, latData)
            writer.write(lon, lonData)
        } catch (e: IOException) {
            print("ERROR creating file $prodName/reformatted.nc: ${e.message}")
        }

        writer.close()
        ogviFile.close()
        geodeticFile.close()
//        flags.close()

        val wgs84 = SpatialReference()
        wgs84.ImportFromEPSG(4326)

        val ogvi = gdal.Open("NETCDF:$prodName/reformatted.nc:olci_global_vegetation_index")
        val map = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted.nc:lat"
        )

        ogvi.SetMetadata(Hashtable(map), "GEOLOCATION")

//        gdal.Warp("$prodName/ogvi_warp_rebuild.tif", arrayOf(ogvi), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW -cutline $shpFile")))
        gdal.Warp("$prodName/ogvi_warp_rebuild.tif", arrayOf(ogvi), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW")))
        val out = Paths.get("$prodName/ogvi_warp_rebuild.tif")
        while (true) {
            if (Files.notExists(out)) {
                Thread.sleep(500)
                continue
            }

            if (Files.size(out) < 50_000) {
                Thread.sleep(500)
                continue
            }

            break
        }
        ogvi.delete()
        println("done")
    }

    @ShellMethod("gdal info")
    fun info(prodName: String) {
        println(gdal.GDALInfo(gdal.Open(prodName), InfoOptions(gdal.ParseCommandLine("-hist -stats"))))
    }

    @ShellMethod("postprocess generated geotiff")
    fun postprocess(prod: String, maxSearchDistance: Double = 2.5, smothingIterations: Int = 3) {
        print(" * Post processing started for $prod...")
        val inds = gdal.Open(prod, gdalconstConstants.GA_Update)
        inds.GetRasterBand(1)
        gdal.FillNodata(inds.GetRasterBand(1), inds.GetRasterBand(1), maxSearchDistance, smothingIterations)
        gdal.DEMProcessing("color-$prod", inds, "color-relief", "color-table.txt", DEMProcessingOptions(gdal.ParseCommandLine("-alpha -co COMPRESS=JPEG")))
        monitorFile("color-$prod", 200000)
        println("done")
    }

    fun Double.format(digits: Int) = java.lang.String.format("%.${digits}f", this)
}
