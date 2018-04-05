package org.esb.tools.controllers

import org.esb.tools.Utils
import org.esb.tools.Utils.Companion.monitorFile
import org.gdal.gdal.BuildVRTOptions
import org.gdal.gdal.DEMProcessingOptions
import org.gdal.gdal.InfoOptions
import org.gdal.gdal.TermProgressCallback
import org.gdal.gdal.TranslateOptions
import org.gdal.gdal.WarpOptions
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference
import org.springframework.beans.factory.annotation.Value
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

    @Value("\${shapeFile:Europe_coastline_poly.shp}") lateinit var shapeFile: String
    @Value("\${shapeFile:Lim_Biogeografico.shp}") lateinit var shapeAmazon: String

    @ShellMethod("Convert and merge multiple OCN products")
    fun lstMerge(pattern: String, @ShellOption(defaultValue = "-projwin 5 50 24 35") outputOptions: String = "") {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
        if (matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

        val ascending = mutableListOf<String>()
        val descending = mutableListOf<String>()

        matches.filter { it.isFile }.forEach {
            rebuildLST(it.file.absolutePath)
            if ( Utils.isAscending(it.file.absolutePath) )
                ascending.add(it.file.absolutePath + "/lst_warp_rebuild.tif")
            else
                descending.add(it.file.absolutePath + "/lst_warp_rebuild.tif")
        }

        ascending.sort()
        descending.sort()
        print(" * Merging...")
        val asc = gdal.BuildVRT("mergea", Vector(ascending), BuildVRTOptions( gdal.ParseCommandLine("-resolution average")) )
        val desc = gdal.BuildVRT("merged", Vector(descending), BuildVRTOptions( gdal.ParseCommandLine("-resolution average")) )

        val da = gdal.Translate("ascending.tif", asc, TranslateOptions( gdal.ParseCommandLine(outputOptions) ) )
        val dd = gdal.Translate("descending.tif", desc, TranslateOptions( gdal.ParseCommandLine(outputOptions) ) )

        monitorFile("ascending.tif", 60000)
        monitorFile("descending.tif", 60000)

        gdal.Warp("ascending-warp.tif", arrayOf(da), WarpOptions(gdal.ParseCommandLine("-co COMPRESS=LZW -s_srs EPSG:4326  -crop_to_cutline -cutline  it_10km.shp")))
        gdal.Warp("descending-warp.tif", arrayOf(dd), WarpOptions(gdal.ParseCommandLine("-co COMPRESS=LZW -s_srs EPSG:4326  -crop_to_cutline -cutline  it_10km.shp")))

        monitorFile("ascending-warp.tif", 90000)
        monitorFile("descending-warp.tif", 90000)

        postprocess("ascending-warp.tif", 1.0, 1)
        postprocess("descending-warp.tif", 1.0, 1)

        desc.delete()
        asc.delete()

        desc.delete()
        asc.delete()
        println("done")
    }

    @ShellMethod("Convert and merge multiple OGVI products")
//    fun ogviMerge(pattern: String, @ShellOption(defaultValue = "-projwin 5 50 24 35") outputOptions: String = "") {
//    fun ogviMerge(pattern: String, outputOptions: String = "", shpFile: String) {
    fun ogviMerge(pattern: String, shpFile: String, @ShellOption(defaultValue = "") outputOptions: String = "") {

        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
        if (matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

        val ascending = mutableListOf<String>()
        val descending = mutableListOf<String>()

        matches.filter { it.isFile }.forEach {
            rebuildOGVI(it.file.absolutePath)
            if ( Utils.isAscending(it.file.absolutePath) )
                ascending.add(it.file.absolutePath + "/ogvi_warp_rebuild.tif")
            else
                descending.add(it.file.absolutePath + "/ogvi_warp_rebuild.tif")
        }

        ascending.sort()
        descending.sort()
        print(" * Merging...")

//        try {
//            val asc = gdal.BuildVRT("mergea", Vector(ascending), BuildVRTOptions(gdal.ParseCommandLine("-resolution average")))
//            val da = gdal.Translate("ascending.tif", asc, TranslateOptions(gdal.ParseCommandLine(outputOptions)))
//            monitorFile("ascending.tif", 60000)
//            gdal.Warp("ascending-warp.tif", arrayOf(da), WarpOptions(gdal.ParseCommandLine("-co COMPRESS=LZW -s_srs EPSG:4326  -crop_to_cutline -cutline  $shpFile")))
//            monitorFile("ascending-warp.tif", 90000)
//            postprocess("ascending-warp.tif", 1.0, 1)
//            asc.delete()
//        } catch (e: IOException) {
//            print("ERROR merging ascending: ${e.message}")
//        }

        try {
            val desc = gdal.BuildVRT("merged", Vector(descending), BuildVRTOptions(gdal.ParseCommandLine("-resolution average")))
            val dd = gdal.Translate("descending.tif", desc, TranslateOptions(gdal.ParseCommandLine("-a_nodata 9.969209968386869E36")))
//            val dd = gdal.Translate("descending.tif", desc, TranslateOptions(gdal.ParseCommandLine("-ot Float32 -a_nodata -128.49803 -scale 0 255 0 1")))
            monitorFile("descending.tif", 60000)
            gdal.Warp("descending-warp.tif", arrayOf(dd), WarpOptions(gdal.ParseCommandLine("-co COMPRESS=LZW -s_srs EPSG:4326 -tr 0.02 0.02 -crop_to_cutline -cutline $shpFile")))
            monitorFile("descending-warp.tif", 90000)
            postprocess("descending-warp.tif", 1.0, 1)
            desc.delete()
        } catch (e: IOException) {
            print("ERROR merging descending: ${e.message}")
        }

        println("done")
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
        val lstDataConv = ArrayShort.D2(shape[0], shape[1])
        var cloud = 0

        for (y in 0 until lstData.shape[0])
            for (x in 0 until lstData.shape[1])
                when {
                    !(x in 30..lstData.shape[1]-30 || y in 30..lstData.shape[0]-30) -> lstDataConv[y, x] = 0//-32767 // stay away from borders
                    lstData[y, x].isNaN() -> lstDataConv[y, x] = 0//-32767 // no data
                    DataType.unsignedShortToInt(confidenceIn[y, x]) and 16384 == 16384 -> {
                        lstDataConv[y, x] = 0//-32767
                        cloud++
                    }
                    else -> lstDataConv[y, x] = lstData[y, x].toShort()
                }

        print("cloudy pixels ${(cloud.toDouble() / (shape[0] * shape[1])*100).format(2)}%... ")

        val dimensions = lstFile.findVariable("LST").dimensions

        val writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted.nc")

        val newDimensions = mutableListOf<Dimension>(
                writer.addDimension(null, dimensions[0].fullName, dimensions[0].length),
                writer.addDimension(null, dimensions[1].fullName, dimensions[1].length)
        )

        // populate
        val lstn = writer.addVariable(null, "surface_temperature", DataType.SHORT, newDimensions)
        lstn.addAll(lstFile.findVariable("LST").attributes)

        val lat = writer.addVariable(null, "lat", DataType.DOUBLE, newDimensions)
        lat.addAll(geodeticFile.findVariable("latitude_in").attributes)

        val lon = writer.addVariable(null, "lon", DataType.DOUBLE, newDimensions)
        lon.addAll(geodeticFile.findVariable("longitude_in").attributes)

//        writer.addGroupAttribute(null, Attribute("Conventions", "CF-1.0"))

        // create the file
        try {
            writer.create()
            writer.write(lstn, lstDataConv)
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

        gdal.Warp("$prodName/lst_warp_rebuild.tif", arrayOf(lst), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW")))
        monitorFile("$prodName/lst_warp_rebuild.tif", 60000)
        lst.delete()
        println("done")
    }

    @ShellMethod("Convert OGVI products")
//    fun rebuildOGVI(prodName: String, shpFile: String) {
    fun rebuildOGVI(prodName: String, force: Boolean = false) {
        if(!force && Files.exists(Paths.get(prodName, "ogvi_warp_rebuild.tif")) && Files.size(Paths.get(prodName, "ogvi_warp_rebuild.tif")) > 50000) return

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
//        val ogviDataConv = ArrayShort.D2(shape[0], shape[1])
        val ogviDataConv = ArrayFloat.D2(shape[0], shape[1])

//        var cloud = 0

        val nodata = 200
        for (y in 0 until ogviData.shape[0])
            for (x in 0 until ogviData.shape[1])
                when {
                    !(x in 30..ogviData.shape[1]-30 || y in 30..ogviData.shape[0]-30) || ogviDataConv[y, x] > 200  -> ogviDataConv[y, x] = nodata.toFloat() // stay away from borders
                    ogviData[y, x].isNaN() -> ogviDataConv[y, x] = nodata.toFloat() // no data
//                    !(x in 30..ogviData.shape[1]-30 || y in 30..ogviData.shape[0]-30) -> ogviDataConv[y, x] = -32767 // stay away from borders
//                    ogviData[y, x].isNaN() -> ogviDataConv[y, x] = -32767 // no data
//                    DataType.unsignedShortToInt(confidenceIn[y, x]) and 16384 == 16384 -> {
//                        ogviDataConv[y, x] = 0//-32767
//                        cloud++
//                    }
//                    else -> ogviDataConv[y, x] = (ogviData[y, x]*255).toShort()
                    else -> ogviDataConv[y, x] = ogviData[y, x].toFloat()
                }

//        print("cloudy pixels ${(cloud.toDouble() / (shape[0] * shape[1])*100).format(2)}%... ")

        val dimensions = ogviFile.findVariable("OGVI").dimensions

        val writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted.nc")

        val newDimensions = mutableListOf<Dimension>(
                writer.addDimension(null, dimensions[0].fullName, dimensions[0].length),
                writer.addDimension(null, dimensions[1].fullName, dimensions[1].length)
        )

        // populate
//        val ogvin = writer.addVariable(null, "olci_global_vegetation_index", DataType.SHORT, newDimensions)
        val ogvin = writer.addVariable(null, "olci_global_vegetation_index", DataType.FLOAT, newDimensions)
        ogvin.addAll(ogviFile.findVariable("OGVI").attributes)

        val lat = writer.addVariable(null, "lat", DataType.DOUBLE, newDimensions)
        lat.addAll(geodeticFile.findVariable("latitude").attributes)

        val lon = writer.addVariable(null, "lon", DataType.DOUBLE, newDimensions)
        lon.addAll(geodeticFile.findVariable("longitude").attributes)

//        writer.addGroupAttribute(null, Attribute("Conventions", "CF-1.0"))

        // create the file
        try {
            writer.create()
            writer.write(ogvin, ogviData)
//            writer.write(ogvin, ogviDataConv)
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

        gdal.Warp("$prodName/ogvi_warp_rebuild.tif", arrayOf(ogvi), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW")))
        monitorFile("$prodName/ogvi_warp_rebuild.tif", 60000)
        ogvi.delete()
        println("done")
    }

    @ShellMethod("gdal info")
    fun info(prodName: String) {
        println(gdal.GDALInfo(gdal.Open(prodName), InfoOptions(gdal.ParseCommandLine("-hist -stats"))))
    }

    @ShellMethod("postprocess generated geotiff")
    fun postprocess(prod: String, maxSearchDistance: Double = 2.5, smothingIterations: Int = 3) {
        print(" * Post processing started...")
        val inds = gdal.Open(prod, gdalconstConstants.GA_Update)
        inds.GetRasterBand(1).SetNoDataValue(0.0)
        gdal.FillNodata(inds.GetRasterBand(1), inds.GetRasterBand(1), maxSearchDistance, smothingIterations, null, TermProgressCallback())
        gdal.DEMProcessing("color-$prod", inds, "color-relief", "color-table.txt", DEMProcessingOptions(gdal.ParseCommandLine("-co COMPRESS=JPEG")))
        monitorFile("color-$prod", 90000)
        println("done")
    }

    fun Double.format(digits: Int) = java.lang.String.format("%.${digits}f", this)
}
