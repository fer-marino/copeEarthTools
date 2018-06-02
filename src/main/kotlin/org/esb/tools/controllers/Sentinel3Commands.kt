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

    @ShellMethod("Convert and merge multiple LST products")
    fun lstMerge(pattern: String,
                 //@ShellOption(defaultValue = "-projwin 5 50 24 35") outputOptions: String = "",
                 @ShellOption(defaultValue = "") outputOptions: String = "",
                 @ShellOption(defaultValue = "false") force: Boolean = false) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
        if (matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

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


        val da = gdal.Translate("ascending.tif", asc, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        val dd = gdal.Translate("descending.tif", desc, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))

        monitorFile("ascending.tif", 1600000)
        monitorFile("descending.tif", 1600000)

        gdal.Warp("ascending-warp.tif", arrayOf(da), WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=3")))
        monitorFile("ascending-warp.tif", 1900000)

        gdal.Warp("descending-warp.tif", arrayOf(dd), WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=3")))
        monitorFile("descending-warp.tif", 1900000)

        desc.delete()
        asc.delete()

        desc.delete()
        asc.delete()

        println(" done in ${System.currentTimeMillis() - start} msec")
    }

    @ShellMethod("Convert and merge multiple OLCI L2 products")
    fun olciMerge(pattern: String,
                  @ShellOption(defaultValue = "") outputOptions: String = "",
                  @ShellOption(defaultValue = "false") force: Boolean = false) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
        if (matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

        //val ascending = mutableListOf<String>()
        val descendingOGVI = mutableListOf<String>()
        val descendingIWV = mutableListOf<String>()
        val descendingOTCI = mutableListOf<String>()

        matches.filter { it.isFile }.forEach {
            rebuildOLCI(it.file.absolutePath, force)
//            if (Utils.isAscending(it.file.absolutePath))
//                //ascending.add(it.file.absolutePath + "/lst_warp_rebuild.tif")
//            else
//            descendingOGVI.add(it.file.absolutePath + "/ogvi_warp_rebuild.tif")
//            descendingIWV.add(it.file.absolutePath + "/iwv_warp_rebuild.tif")
//            descendingOTCI.add(it.file.absolutePath + "/otci_warp_rebuild.tif")
            descendingOGVI.add(it.file.absolutePath + "/ogvi_lzw_rebuild.tif")
            descendingIWV.add(it.file.absolutePath + "/iwv_lzw_rebuild.tif")
            descendingOTCI.add(it.file.absolutePath + "/otci_lzw_rebuild.tif")
        }

        //ascending.sort()
        descendingOGVI.sort()
        descendingIWV.sort()
        descendingOTCI.sort()

        print(" * Merging...")
        val start = System.currentTimeMillis()
        //Files.deleteIfExists(Paths.get("mergea"))
        Files.deleteIfExists(Paths.get("merged"))
        //var asc = gdal.BuildVRT("mergea", Vector(ascending), BuildVRTOptions(gdal.ParseCommandLine("-resolution average")))
//        var descOGVI = gdal.BuildVRT("merged", Vector(descendingOGVI), BuildVRTOptions(gdal.ParseCommandLine("-resolution average")))
//        var descOTCI = gdal.BuildVRT("merged", Vector(descendingOTCI), BuildVRTOptions(gdal.ParseCommandLine("-resolution average")))
//        var descIWV = gdal.BuildVRT("merged", Vector(descendingIWV), BuildVRTOptions(gdal.ParseCommandLine("-resolution average")))

        var descOGVI = gdal.BuildVRT("merged", Vector(descendingOGVI), BuildVRTOptions(gdal.ParseCommandLine("")))
        var descOTCI = gdal.BuildVRT("merged", Vector(descendingOTCI), BuildVRTOptions(gdal.ParseCommandLine("")))
        var descIWV = gdal.BuildVRT("merged", Vector(descendingIWV), BuildVRTOptions(gdal.ParseCommandLine("")))

        gdal.Translate("merged_ogvi.vrt", descOGVI, TranslateOptions(gdal.ParseCommandLine("-of VRT")))
        gdal.Translate("merged_otci.vrt", descOTCI, TranslateOptions(gdal.ParseCommandLine("-of VRT")))
        gdal.Translate("merged_iwv.vrt", descIWV, TranslateOptions(gdal.ParseCommandLine("-of VRT")))

        var t = Files.readAllLines(Paths.get("merged_ogvi.vrt"))
        for(i in 0..t.size) {
            if(t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i+1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i+2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i+3, "    <PixelFunctionCode><![CDATA[")
                t.add(i+4, "import numpy as np")
                t.add(i+5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i+6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), decimals=5, out = out_ar)")
                t.add(i+7, "]]>")
                t.add(i+8, "    </PixelFunctionCode>")
            }
        }
        Files.write(Paths.get("merged_ogvi.vrt"), t)

        t = Files.readAllLines(Paths.get("merged_otci.vrt"))
        for(i in 0..t.size) {
            if(t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i+1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i+2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i+3, "    <PixelFunctionCode><![CDATA[")
                t.add(i+4, "import numpy as np")
                t.add(i+5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i+6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), decimals=5, out = out_ar)")
                t.add(i+7, "]]>")
                t.add(i+8, "    </PixelFunctionCode>")
            }
        }
        Files.write(Paths.get("merged_otci.vrt"), t)

        t = Files.readAllLines(Paths.get("merged_iwv.vrt"))
        for(i in 0..t.size) {
            if(t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i+1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i+2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i+3, "    <PixelFunctionCode><![CDATA[")
                t.add(i+4, "import numpy as np")
                t.add(i+5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i+6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), decimals=5, out = out_ar)")
                t.add(i+7, "]]>")
                t.add(i+8, "    </PixelFunctionCode>")
            }
        }
        Files.write(Paths.get("merged_iwv.vrt"), t)

        descOGVI.delete()
        descOTCI.delete()
        descIWV.delete()

        descOGVI = gdal.Open("merged_ogvi.vrt")
        descOTCI = gdal.Open("merged_otci.vrt")
        descIWV = gdal.Open("merged_iwv.vrt")

        /* MOSAIC */
        val ddOGVI = gdal.Translate("descending_ogvi.tif", descOGVI, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        monitorFile("descending_ogvi.tif", 1600000)

        val ddOTCI = gdal.Translate("descending_otci.tif", descOTCI, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        monitorFile("descending_otci.tif", 1600000)

        val ddIWVI = gdal.Translate("descending_iwv.tif", descIWV, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        monitorFile("descending_iwv.tif", 1600000)

        /* COMPRESSION */
        val commandOGVI = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 descending_ogvi.tif descending_ogvi_lzw.tif"
        Runtime.getRuntime().exec(commandOGVI)

        val commandOTCI = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 descending_otci.tif descending_otci_lzw.tif"
        Runtime.getRuntime().exec(commandOTCI)

        val commandIWV = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 descending_iwv.tif descending_iwv_lzw.tif"
        Runtime.getRuntime().exec(commandIWV)

//        gdal.Translate("descending_ogvi-warp.tif", ddOGVI, TranslateOptions(gdal.ParseCommandLine(" -co COMPRESS=LZW -a_srs EPSG:4326 ")))
//        monitorFile("descending_ogvi-warp.tif", 190000)
//
//        gdal.Translate("descending_otci-warp.tif", descOTCI, TranslateOptions(gdal.ParseCommandLine(" -co COMPRESS=LZW -a_srs EPSG:4326 ")))
//        monitorFile("descending_otci-warp.tif", 190000)
//
//        gdal.Translate("descending_iwv-warp.tif", descIWV, TranslateOptions(gdal.ParseCommandLine(" -co COMPRESS=LZW -a_srs EPSG:4326 ")))
//        monitorFile("descending_iwv-warp.tif", 190000)

//        gdal.Warp("ascending-warp.tif", arrayOf(da), WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=3")))
//        monitorFile("ascending-warp.tif", 1900000)
//
//        gdal.Warp("descending-warp.tif", arrayOf(ddOGVI), WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=3")))
//        monitorFile("descending-warp.tif", 1900000)

        descOGVI.delete()
        descOTCI.delete()
        descIWV.delete()

        println(" done in ${System.currentTimeMillis() - start} msec")
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
        monitorFile("$prodName/lst_warp_rebuild.tif", 600000)
        lst.delete()
        ris.delete()
        println("done")
    }

    @ShellMethod("Convert OGVI products")
//    fun rebuildOLCI(prodName: String, shpFile: String) {
    fun rebuildOLCI(prodName: String, force: Boolean = false) {
      if (!force
                && Files.exists(Paths.get(prodName, "ogvi_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "ogvi_lzw_rebuild.tif")) > 50000
                && Files.exists(Paths.get(prodName, "otci_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "otci_lzw_rebuild.tif")) > 50000
               && Files.exists(Paths.get(prodName, "iwv_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "iwv_lzw_rebuild.tif")) > 50000
                ) return
//        if (!force
//                && Files.exists(Paths.get(prodName, "ogvi_warp_rebuild.tif"))
//                && Files.size(Paths.get(prodName, "ogvi_warp_rebuild.tif")) > 50000
//                && Files.exists(Paths.get(prodName, "otci_warp_rebuild.tif"))
//                && Files.size(Paths.get(prodName, "otci_warp_rebuild.tif")) > 50000
//                && Files.exists(Paths.get(prodName, "iwv_warp_rebuild.tif"))
//                && Files.size(Paths.get(prodName, "iwv_warp_rebuild.tif")) > 50000
//                ) return
        print(" * Converting $prodName... ")
        val ogviFile = NetcdfDataset.openDataset("$prodName/ogvi.nc")
        val otciFile = NetcdfDataset.openDataset("$prodName/otci.nc")
        val iwvFile = NetcdfDataset.openDataset("$prodName/iwv.nc")

        val geodeticFile = NetcdfDataset.openDataset("$prodName/geo_coordinates.nc")

        val ogviData = ogviFile.findVariable("OGVI").read() as ArrayFloat.D2
        val otciData = otciFile.findVariable("OTCI").read() as ArrayFloat.D2
        val iwvData = iwvFile.findVariable("IWV").read() as ArrayFloat.D2

        val latData = geodeticFile.findVariable("latitude").read() as ArrayDouble.D2
        val lonData = geodeticFile.findVariable("longitude").read() as ArrayDouble.D2

        val dimensions = ogviFile.findVariable("OGVI").dimensions

        val writerOGVI = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_ogvi.nc")
        val writerOTCI = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_otci.nc")
        val writerIWV = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_iwv.nc")

        val newDimensionsOGVI = mutableListOf<Dimension>(
                writerOGVI.addDimension(null, dimensions[0].fullName, dimensions[0].length),
                writerOGVI.addDimension(null, dimensions[1].fullName, dimensions[1].length)
        )

        val newDimensionsOTCI = mutableListOf<Dimension>(
                writerOTCI.addDimension(null, dimensions[0].fullName, dimensions[0].length),
                writerOTCI.addDimension(null, dimensions[1].fullName, dimensions[1].length)
        )

        val newDimensionsIWV = mutableListOf<Dimension>(
                writerIWV.addDimension(null, dimensions[0].fullName, dimensions[0].length),
                writerIWV.addDimension(null, dimensions[1].fullName, dimensions[1].length)
        )

        // populate
        val ogvi = writerOGVI.addVariable(null, "vegetation_index", DataType.FLOAT, newDimensionsOGVI)
        val otci = writerOTCI.addVariable(null, "chlorophyll_index", DataType.FLOAT, newDimensionsOTCI)
        val iwv = writerIWV.addVariable(null, "integrated_water_vapour", DataType.FLOAT, newDimensionsIWV)

        ogvi.addAll(ogviFile.findVariable("OGVI").attributes)
//        ogvi.addAttribute(Attribute("valid_range", "0, 1"))
        ogvi.addAttribute(Attribute("_FillValue", "nan"))

        otci.addAll(otciFile.findVariable("OTCI").attributes)
//        ogvi.addAttribute(Attribute("valid_range", "0, 1"))
        otci.addAttribute(Attribute("_FillValue", "nan"))

        iwv.addAll(iwvFile.findVariable("IWV").attributes)
//        ogvi.addAttribute(Attribute("valid_range", "0, 1"))
        iwv.addAttribute(Attribute("_FillValue", "nan"))

        val latOGVI = writerOGVI.addVariable(null, "lat", DataType.DOUBLE, newDimensionsOGVI)
        latOGVI.addAll(geodeticFile.findVariable("latitude").attributes)

        val lonOGVI = writerOGVI.addVariable(null, "lon", DataType.DOUBLE, newDimensionsOGVI)
        lonOGVI.addAll(geodeticFile.findVariable("longitude").attributes)

        // create the file
        try {
            writerOGVI.create()
            writerOGVI.write(ogvi, ogviData)
            writerOGVI.write(latOGVI, latData)
            writerOGVI.write(lonOGVI, lonData)
        } catch (e: IOException) {
            print("ERROR creating file $prodName/reformatted_ogvi.nc: ${e.message}")
        }

        val latOTCI = writerOTCI.addVariable(null, "lat", DataType.DOUBLE, newDimensionsOTCI)
        latOTCI.addAll(geodeticFile.findVariable("latitude").attributes)

        val lonOTCI = writerOTCI.addVariable(null, "lon", DataType.DOUBLE, newDimensionsOTCI)
        lonOTCI.addAll(geodeticFile.findVariable("longitude").attributes)

        try {
            writerOTCI.create()
            writerOTCI.write(otci, otciData)
            writerOTCI.write(latOTCI, latData)
            writerOTCI.write(lonOTCI, lonData)
        } catch (e: IOException) {
            print("ERROR creating file $prodName/reformatted_otci.nc: ${e.message}")
        }

        val latIWV = writerIWV.addVariable(null, "lat", DataType.DOUBLE, newDimensionsIWV)
        latIWV.addAll(geodeticFile.findVariable("latitude").attributes)

        val lonIWV = writerIWV.addVariable(null, "lon", DataType.DOUBLE, newDimensionsIWV)
        lonIWV.addAll(geodeticFile.findVariable("longitude").attributes)

        try {
            writerIWV.create()
            writerIWV.write(iwv, iwvData)
            writerIWV.write(latIWV, latData)
            writerIWV.write(lonIWV, lonData)
        } catch (e: IOException) {
            print("ERROR creating file $prodName/reformatted_iwv.nc: ${e.message}")
        }

        writerOGVI.close()
        ogviFile.close()
        writerOTCI.close()
        otciFile.close()
        writerIWV.close()
        iwvFile.close()
        geodeticFile.close()

        val wgs84 = SpatialReference()
        wgs84.ImportFromEPSG(4326)

        val ogviDTS = gdal.Open("NETCDF:$prodName/reformatted_ogvi.nc:vegetation_index")
        val otciDTS = gdal.Open("NETCDF:$prodName/reformatted_otci.nc:chlorophyll_index")
        val iwvDTS = gdal.Open("NETCDF:$prodName/reformatted_iwv.nc:integrated_water_vapour")

        val mapOGVI = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_ogvi.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_ogvi.nc:lat"
        )

        val mapOTCI = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_otci.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_otci.nc:lat"
        )

        val mapIWV = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_iwv.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_iwv.nc:lat"
        )

        ogviDTS.SetMetadata(Hashtable(mapOGVI), "GEOLOCATION")
        otciDTS.SetMetadata(Hashtable(mapOTCI), "GEOLOCATION")
        iwvDTS.SetMetadata(Hashtable(mapIWV), "GEOLOCATION")

        val risOGVI = gdal.Warp("$prodName/ogvi_warp_rebuild.tif", arrayOf(ogviDTS), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata nan")))
//        val risOGVI = gdal.Warp("$prodName/ogvi_warp_rebuild.tif", arrayOf(ogviDTS), WarpOptions(gdal.ParseCommandLine("-geoloc -srcnodata 0 -dstnodata nan")))
        monitorFile("$prodName/ogvi_warp_rebuild.tif", 60000)

        val risOTCI = gdal.Warp("$prodName/otci_warp_rebuild.tif", arrayOf(otciDTS), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata nan")))
        monitorFile("$prodName/otci_warp_rebuild.tif", 60000)

        val risIWV = gdal.Warp("$prodName/iwv_warp_rebuild.tif", arrayOf(iwvDTS), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata nan")))
        monitorFile("$prodName/iwv_warp_rebuild.tif", 60000)

        var commandOGVI = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 $prodName/ogvi_warp_rebuild.tif $prodName/ogvi_lzw_rebuild.tif"
        println(commandOGVI)
        Runtime.getRuntime().exec(commandOGVI)

        var commandOTCI = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 $prodName/otci_warp_rebuild.tif $prodName/otci_lzw_rebuild.tif"
        println(commandOTCI)
        Runtime.getRuntime().exec(commandOTCI)

        var commandIWV = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 $prodName/iwv_warp_rebuild.tif $prodName/iwv_lzw_rebuild.tif"
        println(commandIWV)
        Runtime.getRuntime().exec(commandIWV)

        ogviDTS.delete()
        risOGVI.delete()

        otciDTS.delete()
        risOTCI.delete()

        iwvDTS.delete()
        risIWV.delete()

        var delete ="rm -rf $prodName/reformatted_otci.nc"
        println(delete)
        Runtime.getRuntime().exec(delete)
        delete ="rm -rf $prodName/reformatted_ogvi.nc"
        println(delete)
        Runtime.getRuntime().exec(delete)
        delete ="rm -rf $prodName/reformatted_iwv.nc"
        println(delete)
        Runtime.getRuntime().exec(delete)
        delete ="rm -rf $prodName/otci_warp_rebuild.tif"
        println(delete)
        Runtime.getRuntime().exec(delete)
        delete ="rm -rf $prodName/ogvi_warp_rebuild.tif"
        println(delete)
        Runtime.getRuntime().exec(delete)
        delete ="rm -rf $prodName/iwv_warp_rebuild.tif"
        println(delete)
        Runtime.getRuntime().exec(delete)

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
        monitorFile("color-$prod", 2000000)
        println("done")
    }

    fun Double.format(digits: Int) = java.lang.String.format("%.${digits}f", this)
}
