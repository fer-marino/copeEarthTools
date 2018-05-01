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
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ucar.ma2.ArrayDouble
import ucar.ma2.ArrayFloat
import ucar.ma2.ArrayShort
import ucar.ma2.DataType
import ucar.nc2.Attribute
import ucar.nc2.Dimension
import ucar.nc2.NetcdfFileWriter
import ucar.nc2.dataset.NetcdfDataset
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

@ShellComponent
class Sentinel3Commands {

    @Value("\${gdalTimeout:160000}")
    private var timeout: Int = 160000

    @ShellMethod("Convert and merge multiple OCN products")
    fun lstMerge(pattern: String,
                 @ShellOption(defaultValue = "-projwin 5 50 24 35") outputOptions: String = "",
                 @ShellOption(defaultValue = "false") force: Boolean = false) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
        if (matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

        matches.first().file.parentFile

        val ascending = mutableListOf<String>()
        val descending = mutableListOf<String>()

        matches.filter { it.isFile }.forEach {
            rebuildLST(it.file.absolutePath, force)
            if (Utils.isAscending(it.file.absolutePath))
                ascending.add(it.file.absolutePath + "/lst_warp_rebuild.tif")
            else
                descending.add(it.file.absolutePath + "/lst_warp_rebuild.tif")
        }

        ascending.sort()
        descending.sort()
        print(" * Merging...")
        val start = System.currentTimeMillis()
        Files.deleteIfExists(Paths.get("mergea"))
        Files.deleteIfExists(Paths.get("merged"))
        var asc = gdal.BuildVRT("mergea", Vector(ascending), BuildVRTOptions(gdal.ParseCommandLine("-resolution average")))
        var desc = gdal.BuildVRT("merged", Vector(descending), BuildVRTOptions(gdal.ParseCommandLine("-resolution average")))

        gdal.Translate("mergea.vrt", asc, TranslateOptions(gdal.ParseCommandLine("-of VRT")))
        gdal.Translate("merged.vrt", desc, TranslateOptions(gdal.ParseCommandLine("-of VRT")))

        var t = Files.readAllLines(Paths.get("mergea.vrt"))
        for(i in 0..t.size) {
            if(t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i+1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i+2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i+3, "    <PixelFunctionCode><![CDATA[")
                t.add(i+4, "import numpy as np")
                t.add(i+5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i+6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), out = out_ar)")
                t.add(i+7, "]]>")
                t.add(i+8, "    </PixelFunctionCode>")
            }
        }
        Files.write(Paths.get("mergea.vrt"), t)

        t = Files.readAllLines(Paths.get("merged.vrt"))
        for(i in 0..t.size) {
            if(t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i+1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i+2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i+3, "    <PixelFunctionCode><![CDATA[")
                t.add(i+4, "import numpy as np")
                t.add(i+5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i+6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), out = out_ar)")
                t.add(i+7, "]]>")
                t.add(i+8, "    </PixelFunctionCode>")
            }
        }
        Files.write(Paths.get("merged.vrt"), t)

        asc.delete()
        desc.delete()
        asc = gdal.Open("mergea.vrt")
        desc = gdal.Open("merged.vrt")


        val da = gdal.Translate("${matches.first().file.parentFile}/ascending.tif", asc, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        val dd = gdal.Translate("${matches.first().file.parentFile}/descending.tif", desc, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))

        monitorFile("${matches.first().file.parentFile}/ascending.tif", timeout)
        monitorFile("${matches.first().file.parentFile}/descending.tif", timeout)

        gdal.Warp("${matches.first().file.parentFile}/ascending-warp.tif", arrayOf(da), WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=3")))
        monitorFile("${matches.first().file.parentFile}/ascending-warp.tif", timeout)

        gdal.Warp("${matches.first().file.parentFile}/descending-warp.tif", arrayOf(dd), WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=3")))
        monitorFile("${matches.first().file.parentFile}/descending-warp.tif", timeout)

        desc.delete()
        asc.delete()

        desc.delete()
        asc.delete()

        println(" done in ${System.currentTimeMillis() - start} msec")
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
            if (Utils.isAscending(it.file.absolutePath))
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
            monitorFile("descending.tif", timeout)
            gdal.Warp("descending-warp.tif", arrayOf(dd), WarpOptions(gdal.ParseCommandLine("-co COMPRESS=LZW -s_srs EPSG:4326 -tr 0.02 0.02 -crop_to_cutline -cutline $shpFile")))
            monitorFile("descending-warp.tif", timeout)
            postprocess("descending-warp.tif", 1.0, 1)
            desc.delete()
        } catch (e: IOException) {
            print("ERROR merging descending: ${e.message}")
        }

        println("done")
    }

    @ShellMethod("Convert LST products")
    fun rebuildLST(prodName: String, force: Boolean = false) {
        if (!force && Files.exists(Paths.get(prodName, "lst_warp_rebuild.tif")) && Files.size(Paths.get(prodName, "lst_warp_rebuild.tif")) > 50000) return

        print(" * Converting $prodName... ")
        val lstFile = NetcdfDataset.openDataset("$prodName/LST_in.nc")
        val geodeticFile = NetcdfDataset.openDataset("$prodName/geodetic_in.nc")
        val flags = NetcdfDataset.openDataset("$prodName/flags_in.nc")

        val lstData = lstFile.findVariable("LST").read() as ArrayFloat.D2
        val latData = geodeticFile.findVariable("latitude_in").read() as ArrayDouble.D2
        val lonData = geodeticFile.findVariable("longitude_in").read() as ArrayDouble.D2
        val confidenceIn = flags.findVariable("confidence_in").read() as ArrayShort.D2

        val shape = lstData.shape

        var cloud = 0
        for (y in 0 until lstData.shape[0])
            for (x in 0 until lstData.shape[1])
                when {
                    !(x in 30..lstData.shape[1] - 30 || y in 30..lstData.shape[0] - 30) -> lstData[y, x] = Float.NaN  // stay away from borders
                    DataType.unsignedShortToInt(confidenceIn[y, x]) and 16384 == 16384 -> {
                        lstData[y, x] = Float.NaN
                        cloud++
                    }
                }

        print("cloudy pixels ${(cloud.toDouble() / (shape[0] * shape[1]) * 100).format(2)}%... ")

        val dimensions = lstFile.findVariable("LST").dimensions

        val writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted.nc")

        val newDimensions = mutableListOf<Dimension>(
                writer.addDimension(null, dimensions[0].fullName, dimensions[0].length),
                writer.addDimension(null, dimensions[1].fullName, dimensions[1].length)
        )

        // populate
        val lstn = writer.addVariable(null, "surface_temperature", DataType.FLOAT, newDimensions)
        lstn.addAll(lstFile.findVariable("LST").attributes)
        lstn.addAttribute(Attribute("valid_range", "200, 350"))
        lstn.addAttribute(Attribute("_FillValue", "nan"))

        val lat = writer.addVariable(null, "lat", DataType.DOUBLE, newDimensions)
        lat.addAll(geodeticFile.findVariable("latitude_in").attributes)

        val lon = writer.addVariable(null, "lon", DataType.DOUBLE, newDimensions)
        lon.addAll(geodeticFile.findVariable("longitude_in").attributes)

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

        val ris = gdal.Warp("$prodName/lst_warp_rebuild.tif", arrayOf(lst), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata nan")))
        monitorFile("$prodName/lst_warp_rebuild.tif", timeout)
        lst.delete()
        ris.delete()
        println("done")
    }

    @ShellMethod("Convert OGVI products")
//    fun rebuildOGVI(prodName: String, shpFile: String) {
    fun rebuildOGVI(prodName: String, force: Boolean = false) {
        if (!force && Files.exists(Paths.get(prodName, "ogvi_warp_rebuild.tif")) && Files.size(Paths.get(prodName, "ogvi_warp_rebuild.tif")) > 50000) return

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
                    !(x in 30..ogviData.shape[1] - 30 || y in 30..ogviData.shape[0] - 30) || ogviDataConv[y, x] > 200 -> ogviDataConv[y, x] = nodata.toFloat() // stay away from borders
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
        monitorFile("$prodName/ogvi_warp_rebuild.tif", timeout)
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
        monitorFile("color-$prod", timeout)
        println("done")
    }

    fun Double.format(digits: Int) = java.lang.String.format("%.${digits}f", this)
}
