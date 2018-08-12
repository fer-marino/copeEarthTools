package org.esb.tools.controllers

import org.esb.tools.Utils.Companion.monitorFile
import org.gdal.gdal.*
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ucar.ma2.*
import ucar.nc2.Attribute
import ucar.nc2.Dimension
import ucar.nc2.NetcdfFileWriter
import ucar.nc2.Variable
import ucar.nc2.dataset.NetcdfDataset
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

@ShellComponent
class Sentinel3Commands {

    @Value("\${gdalTimeout:16000000}")
    private var timeout: Int = 16000000

    @ShellMethod("Convert and merge multiple LST products")
    fun lstMerge(pattern: String,
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
            ascending.add(it.file.absolutePath + "/ascending.tif")
            descending.add(it.file.absolutePath + "/descending.tif")
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
        for (i in 0..t.size) {
            if (t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i + 1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i + 2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i + 3, "    <PixelFunctionCode><![CDATA[")
                t.add(i + 4, "import numpy as np")
                t.add(i + 5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i + 6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), out = out_ar)")
                t.add(i + 7, "]]>")
                t.add(i + 8, "    </PixelFunctionCode>")
            }
        }
//        Files.write(Paths.get("mergea.vrt"), t)

        t = Files.readAllLines(Paths.get("merged.vrt"))
        for (i in 0..t.size) {
            if (t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i + 1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i + 2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i + 3, "    <PixelFunctionCode><![CDATA[")
                t.add(i + 4, "import numpy as np")
                t.add(i + 5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i + 6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), out = out_ar)")
                t.add(i + 7, "]]>")
                t.add(i + 8, "    </PixelFunctionCode>")
            }
        }
//        Files.write(Paths.get("merged.vrt"), t)

        asc.delete()
        desc.delete()
        asc = gdal.Open("mergea.vrt")
        desc = gdal.Open("merged.vrt")


        val da = gdal.Translate("ascending.tif", asc, TranslateOptions(gdal.ParseCommandLine(outputOptions)))
        val dd = gdal.Translate("descending.tif", desc, TranslateOptions(gdal.ParseCommandLine(outputOptions)))

        gdal.Warp("${matches.first().file.parentFile}/ascending-warp.tif", arrayOf(da),
                WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=3"))).delete()

        gdal.Warp("${matches.first().file.parentFile}/descending-warp.tif", arrayOf(dd),
                WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:4326 -wo NUM_THREADS=3"))).delete()

        da.delete()
        dd.delete()

        desc.delete()
        asc.delete()

        desc.delete()
        asc.delete()

        println(" done in ${System.currentTimeMillis() - start} msec")
    }

    @ShellMethod("Converte and merge multiple SST products")
    fun sstMerge(pattern: String,
            //@ShellOption(defaultValue = " ") outputOptions: String = "",
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
            rebuildSST(it.file.absolutePath, force)
            ascending.add(it.file.absolutePath + "/ascending.tif")
            descending.add(it.file.absolutePath + "/descending.tif")
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
        for (i in 0..t.size) {
            if (t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i + 1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i + 2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i + 3, "    <PixelFunctionCode><![CDATA[")
                t.add(i + 4, "import numpy as np")
                t.add(i + 5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i + 6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), out = out_ar)")
                t.add(i + 7, "]]>")
                t.add(i + 8, "    </PixelFunctionCode>")
            }
        }
//        Files.write(Paths.get("mergea.vrt"), t)

        t = Files.readAllLines(Paths.get("merged.vrt"))
        for (i in 0..t.size) {
            if (t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i + 1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i + 2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i + 3, "    <PixelFunctionCode><![CDATA[")
                t.add(i + 4, "import numpy as np")
                t.add(i + 5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i + 6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), out = out_ar)")
                t.add(i + 7, "]]>")
                t.add(i + 8, "    </PixelFunctionCode>")
            }
        }
//        Files.write(Paths.get("merged.vrt"), t)

        asc.delete()
        desc.delete()
        asc = gdal.Open("mergea.vrt")
        desc = gdal.Open("merged.vrt")


        val da = gdal.Translate("ascending.tif", asc, TranslateOptions(gdal.ParseCommandLine(outputOptions)))
        val dd = gdal.Translate("descending.tif", desc, TranslateOptions(gdal.ParseCommandLine(outputOptions)))

//        gdal.Warp("${matches.first().file.parentFile}/ascending-warp.tif", arrayOf(da),
//                WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:3857 -wo NUM_THREADS=3"))).delete()
//
//        gdal.Warp("${matches.first().file.parentFile}/descending-warp.tif", arrayOf(dd),
//                WarpOptions(gdal.ParseCommandLine("-overwrite -wm 3000 -co COMPRESS=LZW -s_srs EPSG:3857 -wo NUM_THREADS=3"))).delete()

        val commanddesc = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending.tif descending_sst_lzw.tif"
        Runtime.getRuntime().exec(commanddesc)
        val commandasc = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  ascending.tif descending_sst_lzw.tif"
        Runtime.getRuntime().exec(commandasc)

        da.delete()
        dd.delete()

        desc.delete()
        asc.delete()

        desc.delete()
        asc.delete()

        println(" done in ${System.currentTimeMillis() - start} msec")
    }

    @ShellMethod("Convert and merge multiple OLCI L2 marine products")
    fun olciMergeMarine(pattern: String,
                        @ShellOption(defaultValue = "") outputOptions: String = "",
                        @ShellOption(defaultValue = "false") force: Boolean = false) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
        if (matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }


        val descendingCHL = mutableListOf<String>()
        val descendingTSM = mutableListOf<String>()
        val descendingKD9 = mutableListOf<String>()
        val descendingT86 = mutableListOf<String>()
        val descendingA86 = mutableListOf<String>()
        val descendingCHN = mutableListOf<String>()
        val descendingADG = mutableListOf<String>()
        val descendingPAR = mutableListOf<String>()
        val descendingIWV = mutableListOf<String>()

        matches.filter { it.isFile }.forEach {
            rebuildOLCIMarine(it.file.absolutePath, force)
            descendingCHL.add(it.file.absolutePath + "/chl_lzw_rebuild.tif")
            descendingTSM.add(it.file.absolutePath + "/tsm_lzw_rebuild.tif")
            descendingKD9.add(it.file.absolutePath + "/kd9_lzw_rebuild.tif")
            descendingT86.add(it.file.absolutePath + "/t86_lzw_rebuild.tif")
            descendingA86.add(it.file.absolutePath + "/a86_lzw_rebuild.tif")
            descendingCHN.add(it.file.absolutePath + "/chn_lzw_rebuild.tif")
            descendingADG.add(it.file.absolutePath + "/adg_lzw_rebuild.tif")
            descendingPAR.add(it.file.absolutePath + "/par_lzw_rebuild.tif")
        }

        descendingCHL.sort()
        descendingTSM.sort()
        descendingKD9.sort()
        descendingT86.sort()
        descendingA86.sort()
        descendingCHN.sort()
        descendingADG.sort()
        descendingPAR.sort()

        print(" * Merging...")
        val start = System.currentTimeMillis()

        Files.deleteIfExists(Paths.get("merged_chl"))
        Files.deleteIfExists(Paths.get("merged_tsm"))
        Files.deleteIfExists(Paths.get("merged_kd9"))
        Files.deleteIfExists(Paths.get("merged_a86"))
        Files.deleteIfExists(Paths.get("merged_t86"))
        Files.deleteIfExists(Paths.get("merged_chn"))
        Files.deleteIfExists(Paths.get("merged_adg"))
        Files.deleteIfExists(Paths.get("merged_par"))

        /* Build VRT*/
        var descCHL = gdal.BuildVRT("merged_chl", Vector(descendingCHL), BuildVRTOptions(gdal.ParseCommandLine("")))
        var descTSM = gdal.BuildVRT("merged_tsm", Vector(descendingTSM), BuildVRTOptions(gdal.ParseCommandLine("")))
        var descKD9 = gdal.BuildVRT("merged_kd9", Vector(descendingKD9), BuildVRTOptions(gdal.ParseCommandLine("")))
        var descT86 = gdal.BuildVRT("merged_t86", Vector(descendingT86), BuildVRTOptions(gdal.ParseCommandLine("")))
        var descA86 = gdal.BuildVRT("merged_a86", Vector(descendingA86), BuildVRTOptions(gdal.ParseCommandLine("")))
        var descCHN = gdal.BuildVRT("merged_chn", Vector(descendingCHN), BuildVRTOptions(gdal.ParseCommandLine("")))
        var descADG = gdal.BuildVRT("merged_adg", Vector(descendingADG), BuildVRTOptions(gdal.ParseCommandLine("")))
        var descPAR = gdal.BuildVRT("merged_par", Vector(descendingPAR), BuildVRTOptions(gdal.ParseCommandLine("")))

        gdal.Translate("merged_chl.vrt", descCHL, TranslateOptions(gdal.ParseCommandLine("-of VRT")))
        gdal.Translate("merged_tsm.vrt", descTSM, TranslateOptions(gdal.ParseCommandLine("-of VRT")))
        gdal.Translate("merged_kd9.vrt", descKD9, TranslateOptions(gdal.ParseCommandLine("-of VRT")))
        gdal.Translate("merged_t86.vrt", descT86, TranslateOptions(gdal.ParseCommandLine("-of VRT")))
        gdal.Translate("merged_a86.vrt", descA86, TranslateOptions(gdal.ParseCommandLine("-of VRT")))
        gdal.Translate("merged_chn.vrt", descCHN, TranslateOptions(gdal.ParseCommandLine("-of VRT")))
        gdal.Translate("merged_adg.vrt", descADG, TranslateOptions(gdal.ParseCommandLine("-of VRT")))
        gdal.Translate("merged_par.vrt", descPAR, TranslateOptions(gdal.ParseCommandLine("-of VRT")))

        val pyLog = "np.round_(np.log10(np.nanmean(np.power(10,in_ar,dtype = 'float32'), axis = 0, dtype = 'float32'),dtype = 'float32'), decimals=5, out = out_ar)"
        val pyLin = "np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), decimals=5, out = out_ar)"

        createVRT("chl", pyLog)
        createVRT("tsm", pyLog)
        createVRT("kd9", pyLog)
        createVRT("t86", pyLin)
        createVRT("a86", pyLin)
        createVRT("chn", pyLog)
        createVRT("adg", pyLog)
        createVRT("par", pyLin)

        descCHL.delete()
        descTSM.delete()
        descKD9.delete()
        descT86.delete()
        descA86.delete()
        descCHN.delete()
        descADG.delete()
        descPAR.delete()

        descCHL = gdal.Open("merged_chl.vrt")
        descTSM = gdal.Open("merged_tsm.vrt")
        descKD9 = gdal.Open("merged_kd9.vrt")
        descT86 = gdal.Open("merged_t86.vrt")
        descA86 = gdal.Open("merged_a86.vrt")
        descCHN = gdal.Open("merged_chn.vrt")
        descADG = gdal.Open("merged_adg.vrt")
        descPAR = gdal.Open("merged_par.vrt")

        /* MOSAIC */
        val ddCHL = gdal.Translate("descending_chl.tif", descCHL, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        val ddTSM = gdal.Translate("descending_tsm.tif", descTSM, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        val ddKD9 = gdal.Translate("descending_kd9.tif", descKD9, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        val ddT86 = gdal.Translate("descending_t86.tif", descT86, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        val ddA86 = gdal.Translate("descending_a86.tif", descA86, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        val ddCHN = gdal.Translate("descending_chn.tif", descCHN, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        val ddADG = gdal.Translate("descending_adg.tif", descADG, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
        val ddPAR = gdal.Translate("descending_par.tif", descPAR, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))

        /* COMPRESSION */
        val commandCHL = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending_chl.tif descending_chl_lzw.tif"
        Runtime.getRuntime().exec(commandCHL)
        val commandTSM = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending_tsm.tif descending_tsm_lzw.tif"
        Runtime.getRuntime().exec(commandTSM)
        val commandKD9 = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending_kd9.tif descending_kd9_lzw.tif"
        Runtime.getRuntime().exec(commandKD9)
        val commandT86 = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending_t86.tif descending_t86_lzw.tif"
        Runtime.getRuntime().exec(commandT86)
        val commandA86 = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending_a86.tif descending_a86_lzw.tif"
        Runtime.getRuntime().exec(commandA86)
        val commandCHN = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending_chn.tif descending_chn_lzw.tif"
        Runtime.getRuntime().exec(commandCHN)
        val commandADG = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending_adg.tif descending_adg_lzw.tif"
        Runtime.getRuntime().exec(commandADG)
        val commandPAR = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending_par.tif descending_par_lzw.tif"
        Runtime.getRuntime().exec(commandPAR)

        /*GDAL WARP resampling*/


        descCHL.delete()
        descTSM.delete()
        descKD9.delete()
        descT86.delete()
        descA86.delete()
        descCHN.delete()
        descADG.delete()
        descPAR.delete()

        println(" done in ${System.currentTimeMillis() - start} msec")
    }

    @ShellMethod("Convert and merge multiple OLCI L2 land products")
    fun olciMergeLand(pattern: String,
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
            rebuildOLCILand(it.file.absolutePath, force)
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
        for (i in 0..t.size) {
            if (t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i + 1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i + 2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i + 3, "    <PixelFunctionCode><![CDATA[")
                t.add(i + 4, "import numpy as np")
                t.add(i + 5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i + 6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), decimals=5, out = out_ar)")
                t.add(i + 7, "]]>")
                t.add(i + 8, "    </PixelFunctionCode>")
            }
        }
        Files.write(Paths.get("merged_ogvi.vrt"), t)

        t = Files.readAllLines(Paths.get("merged_otci.vrt"))
        for (i in 0..t.size) {
            if (t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i + 1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i + 2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i + 3, "    <PixelFunctionCode><![CDATA[")
                t.add(i + 4, "import numpy as np")
                t.add(i + 5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i + 6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), decimals=5, out = out_ar)")
                t.add(i + 7, "]]>")
                t.add(i + 8, "    </PixelFunctionCode>")
            }
        }
        Files.write(Paths.get("merged_otci.vrt"), t)

        t = Files.readAllLines(Paths.get("merged_iwv.vrt"))
        for (i in 0..t.size) {
            if (t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i + 1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i + 2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i + 3, "    <PixelFunctionCode><![CDATA[")
                t.add(i + 4, "import numpy as np")
                t.add(i + 5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
                t.add(i + 6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), decimals=5, out = out_ar)")
                t.add(i + 7, "]]>")
                t.add(i + 8, "    </PixelFunctionCode>")
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
//        monitorFile("descending_ogvi.tif", 1600000)

        val ddOTCI = gdal.Translate("descending_otci.tif", descOTCI, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
//        monitorFile("descending_otci.tif", 1600000)

        val ddIWVI = gdal.Translate("descending_iwv.tif", descIWV, TranslateOptions(gdal.ParseCommandLine("$outputOptions")))
//        monitorFile("descending_iwv.tif", 1600000)

        /* COMPRESSION */
        val commandOGVI = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending_ogvi.tif descending_ogvi_lzw.tif"
        Runtime.getRuntime().exec(commandOGVI)

        val commandOTCI = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending_otci.tif descending_otci_lzw.tif"
        Runtime.getRuntime().exec(commandOTCI)

        val commandIWV = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 -projwin -179.9 89.9 179.9 -89.9  descending_iwv.tif descending_iwv_lzw.tif"
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
        if (!force && Files.exists(Paths.get(prodName, "ascending.tif")) && Files.size(Paths.get(prodName, "ascending.tif")) > 50000) return

        print(" * Converting $prodName... ")
        Files.deleteIfExists(Paths.get("$prodName/reformatted_ascending.nc"))
        Files.deleteIfExists(Paths.get("$prodName/reformatted_descending.nc"))
        Files.deleteIfExists(Paths.get("$prodName/ascending.tif"))
        Files.deleteIfExists(Paths.get("$prodName/descending.tif"))

        val lstFile = NetcdfDataset.openDataset("$prodName/LST_in.nc")
        val geodeticFile = NetcdfDataset.openDataset("$prodName/geodetic_in.nc")
        val flags = NetcdfDataset.openDataset("$prodName/flags_in.nc")

        val lstData = lstFile.findVariable("LST").read() as ArrayFloat.D2
        val latData = geodeticFile.findVariable("latitude_in").read() as ArrayDouble.D2
        val lonData = geodeticFile.findVariable("longitude_in").read() as ArrayDouble.D2
        val confidenceIn = flags.findVariable("confidence_in").read() as ArrayShort.D2


        val shape = lstData.shape
        val lstAscending = ArrayFloat.D2(shape[0], shape[1])
        val lstDescending = ArrayFloat.D2(shape[0], shape[1])

        var cloud = 0
        for (y in 0 until lstData.shape[0])
            for (x in 0 until lstData.shape[1])
                when {
                    DataType.unsignedShortToInt(confidenceIn[y, x]) and 16384 == 16384 -> {
                        lstData[y, x] = Float.NaN
                        cloud++
                    }
                    !(x in 30..lstData.shape[1] - 30 || y in 30..lstData.shape[0] - 30) -> lstData[y, x] = Float.NaN  // stay away from borders
                    else -> {
                        if (latData[y, x] > 85 || latData[y, x] < -85) {
                            lstAscending[y, x] = lstData[y, x]
                            lstDescending[y, x] = lstData[y, x]
                        } else if (latData[y, x] - latData[Math.max(0, y - 2), x] >= 0)
                            lstAscending[y, x] = lstData[y, x]      // ascending
                        else if (latData[y, x] - latData[Math.max(0, y - 2), x] <= 0)
                            lstDescending[y, x] = lstData[y, x]    // descending
                    }
                }

        print("cloudy pixels ${(cloud.toDouble() / (shape[0] * shape[1]) * 100).format(2)}%... ")

        val dimensions = lstFile.findVariable("LST").dimensions

        val writerAscending = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_ascending.nc")
        val writerDescending = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_descending.nc")

        val dimensionAscending = mutableListOf<Dimension>(
                writerAscending.addDimension(null, dimensions[0].fullName, dimensions[0].length),
                writerAscending.addDimension(null, dimensions[1].fullName, dimensions[1].length)
        )
        val dimensionDescending = mutableListOf<Dimension>(
                writerDescending.addDimension(null, dimensions[0].fullName, dimensions[0].length),
                writerDescending.addDimension(null, dimensions[1].fullName, dimensions[1].length)
        )

        // populate
        val lstAscVar = writerAscending.addVariable(null, "surface_temperature", DataType.FLOAT, dimensionAscending)
        lstAscVar.addAll(lstFile.findVariable("LST").attributes)
        lstAscVar.addAttribute(Attribute("valid_range", "200, 350"))
        lstAscVar.addAttribute(Attribute("_FillValue", "nan"))

        val lstDescVar = writerDescending.addVariable(null, "surface_temperature", DataType.FLOAT, dimensionDescending)
        lstDescVar.addAll(lstFile.findVariable("LST").attributes)
        lstDescVar.addAttribute(Attribute("valid_range", "200, 350"))
        lstDescVar.addAttribute(Attribute("_FillValue", "nan"))

        val latAVar = writerAscending.addVariable(null, "lat", DataType.DOUBLE, dimensionAscending)
        latAVar.addAll(geodeticFile.findVariable("latitude_in").attributes)

        val latDVar = writerDescending.addVariable(null, "lat", DataType.DOUBLE, dimensionDescending)
        latDVar.addAll(geodeticFile.findVariable("latitude_in").attributes)

        val lonAVar = writerAscending.addVariable(null, "lon", DataType.DOUBLE, dimensionAscending)
        lonAVar.addAll(geodeticFile.findVariable("longitude_in").attributes)

        val lonDVar = writerDescending.addVariable(null, "lon", DataType.DOUBLE, dimensionDescending)
        lonDVar.addAll(geodeticFile.findVariable("longitude_in").attributes)

        // create the file
        try {
            writerAscending.create()
            writerAscending.write(lstAscVar, lstAscending)
            writerAscending.write(latAVar, latData)
            writerAscending.write(lonAVar, lonData)

            writerDescending.create()
            writerDescending.write(lstDescVar, lstDescending)
            writerDescending.write(latDVar, latData)
            writerDescending.write(lonDVar, lonData)
        } catch (e: IOException) {
            print("ERROR creating file $prodName/reformatted_xxx.nc: ${e.message}")
            throw e
        } finally {
            writerAscending.close()
            writerDescending.close()
            lstFile.close()
            geodeticFile.close()
            flags.close()
        }

        print(" warping... ")
        val wgs84 = SpatialReference()
        wgs84.ImportFromEPSG(4326)

        val lstAscdataset = gdal.Open("NETCDF:$prodName/reformatted_ascending.nc:surface_temperature")
        val mapA = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_ascending.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_ascending.nc:lat"
        )

        lstAscdataset.SetMetadata(Hashtable(mapA), "GEOLOCATION")

        var ris = gdal.Warp("$prodName/ascending.tif", arrayOf(lstAscdataset), WarpOptions(gdal.ParseCommandLine("-t_srs EPSG:4326 -tr 0.012349251965289 0.012349251965289 -geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata nan")))
        monitorFile("$prodName/ascending.tif", timeout)
        lstAscdataset.delete()
        ris.delete()
        print(" ascending complete. Starting descending...")

        val lstDescdataset = gdal.Open("NETCDF:$prodName/reformatted_descending.nc:surface_temperature")
        val mapD = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_descending.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_descending.nc:lat"
        )

        lstDescdataset.SetMetadata(Hashtable(mapD), "GEOLOCATION")

        ris = gdal.Warp("$prodName/descending.tif", arrayOf(lstDescdataset), WarpOptions(gdal.ParseCommandLine("-t_srs EPSG:3857 -tr 0.012349251965289 0.012349251965289 -geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata nan")))
        monitorFile("$prodName/descending.tif", 600000)
        lstDescdataset.delete()
        ris.delete()
        println("done")
    }

    @ShellMethod("Convert SST products")
    fun rebuildSST(prodName: String, force: Boolean = false) {
        if (!force && Files.exists(Paths.get(prodName, "ascending.tif")) && Files.size(Paths.get(prodName, "ascending.tif")) > 50000) return

        print(" * Converting $prodName... ")
        Files.deleteIfExists(Paths.get("$prodName/reformatted_ascending.nc"))
        Files.deleteIfExists(Paths.get("$prodName/reformatted_descending.nc"))
        Files.deleteIfExists(Paths.get("$prodName/ascending.tif"))
        Files.deleteIfExists(Paths.get("$prodName/descending.tif"))

        val pattern = "SLSTRA"
        var prod = ""
        var file = File("$prodName/").listFiles()
        for (item in file)
            if (item.toString().contains(pattern))
                prod = item.toString()
//
//        val prod = PathMatchingResourcePatternResolver().getResources("file:$pattern")
//        if (matches.isEmpty()) {
//            println(" * No product matches the pattern '$pattern'")
//            return
//        }
        println("processing " + prod)
        val sstFile = NetcdfDataset.openDataset(prod)
//        val sstFile = NetcdfDataset.openDataset("$prodName/"+prod)
//        val sstFile = NetcdfDataset.openDataset("$prodName/geodetic_in.nc")
//        val flags = NetcdfDataset.openDataset("$prodName/flags_in.nc")

        val sstData = sstFile.findVariable("sea_surface_temperature").read().reduce(0) as ArrayDouble.D2
//        println(sstData.shape[0])
//        println(sstData.shape[1])
//        println(sstData.shape[2])

        val latData = sstFile.findVariable("lat").read() as ArrayFloat.D2
        val lonData = sstFile.findVariable("lon").read() as ArrayFloat.D2
//        val confidenceIn = flags.findVariable("confidence_in").read() as ArrayShort.D2


        val shape = sstData.shape
        val sstAscending = ArrayFloat.D2(shape[0], shape[1])
        val sstDescending = ArrayFloat.D2(shape[0], shape[1])

        var cloud = 0
        for (y in 0 until sstData.shape[0])
            for (x in 0 until sstData.shape[1])
//                when {
//                    DataType.unsignedShortToInt(confidenceIn[y, x]) and 16384 == 16384 -> {
//                        lstData[y, x] = Float.NaN
//                        cloud++
//                    }
//                    !(x in 30..lstData.shape[1] - 30 || y in 30..lstData.shape[0] - 30) -> lstData[y, x] = Float.NaN  // stay away from borders
//                    else -> {
                if (latData[y, x] > 85 || latData[y, x] < -85) {
                    sstAscending[y, x] = sstData[y, x].toFloat()
                    sstDescending[y, x] = sstData[y, x].toFloat()
                } else if (latData[y, x] - latData[Math.max(0, y - 2), x] >= 0)
                    sstAscending[y, x] = sstData[y, x].toFloat()      // ascending
                else if (latData[y, x] - latData[Math.max(0, y - 2), x] <= 0)
                    sstDescending[y, x] = sstData[y, x].toFloat()    // descending
//                    }
//                }

//        print("cloudy pixels ${(cloud.toDouble() / (shape[0] * shape[1]) * 100).format(2)}%... ")

        val dimensions = sstFile.findVariable("sea_surface_temperature").dimensions

        val writerAscending = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_ascending.nc")
        val writerDescending = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_descending.nc")

        val dimensionAscending = mutableListOf<Dimension>(
                writerAscending.addDimension(null, dimensions[1].fullName, dimensions[1].length),
                writerAscending.addDimension(null, dimensions[2].fullName, dimensions[2].length)
        )
        val dimensionDescending = mutableListOf<Dimension>(
                writerDescending.addDimension(null, dimensions[1].fullName, dimensions[1].length),
                writerDescending.addDimension(null, dimensions[2].fullName, dimensions[2].length)
        )

        // populate
        val sstAscVar = writerAscending.addVariable(null, "surface_temperature", DataType.FLOAT, dimensionAscending)
        sstAscVar.addAll(sstFile.findVariable("sea_surface_temperature").attributes)
        sstAscVar.addAttribute(Attribute("valid_range", "200, 350"))
        sstAscVar.addAttribute(Attribute("_FillValue", "nan"))

        val sstDescVar = writerDescending.addVariable(null, "surface_temperature", DataType.FLOAT, dimensionDescending)
        sstDescVar.addAll(sstFile.findVariable("sea_surface_temperature").attributes)
        sstDescVar.addAttribute(Attribute("valid_range", "200, 350"))
        sstDescVar.addAttribute(Attribute("_FillValue", "nan"))

        val latAVar = writerAscending.addVariable(null, "lat", DataType.DOUBLE, dimensionAscending)
        latAVar.addAll(sstFile.findVariable("lat").attributes)

        val latDVar = writerDescending.addVariable(null, "lat", DataType.DOUBLE, dimensionDescending)
        latDVar.addAll(sstFile.findVariable("lat").attributes)

        val lonAVar = writerAscending.addVariable(null, "lon", DataType.DOUBLE, dimensionAscending)
        lonAVar.addAll(sstFile.findVariable("lon").attributes)

        val lonDVar = writerDescending.addVariable(null, "lon", DataType.DOUBLE, dimensionDescending)
        lonDVar.addAll(sstFile.findVariable("lon").attributes)

        // create the file
        try {
            writerAscending.create()
            writerAscending.write(sstAscVar, sstAscending)
            writerAscending.write(latAVar, latData)
            writerAscending.write(lonAVar, lonData)

            writerDescending.create()
            writerDescending.write(sstDescVar, sstDescending)
            writerDescending.write(latDVar, latData)
            writerDescending.write(lonDVar, lonData)
        } catch (e: IOException) {
            print("ERROR creating file $prodName/reformatted_xxx.nc: ${e.message}")
            throw e
        } finally {
            writerAscending.close()
            writerDescending.close()
            sstFile.close()
//            sstFile.close()
//            flags.close()
        }

        print(" warping... ")
        val wgs84 = SpatialReference()
        wgs84.ImportFromEPSG(4326)

        val sstAscdataset = gdal.Open("NETCDF:$prodName/reformatted_ascending.nc:surface_temperature")
        val mapA = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_ascending.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_ascending.nc:lat"
        )

//        sstAscdataset.SetMetadata(Hashtable(mapA), "GEOLOCATION")

        var ris = gdal.Warp("$prodName/ascending.tif", arrayOf(sstAscdataset), WarpOptions(gdal.ParseCommandLine("-t_srs EPSG:4326 -tr 0.012349251965289 0.012349251965289 -geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata nan")))
//        monitorFile("$prodName/ascending.tif", timeout)
        sstAscdataset.delete()
        ris.delete()
        print(" ascending complete. Starting descending...")

        val sstDescdataset = gdal.Open("NETCDF:$prodName/reformatted_descending.nc:surface_temperature")
        val mapD = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_descending.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_descending.nc:lat"
        )

        sstDescdataset.SetMetadata(Hashtable(mapD), "GEOLOCATION")

        ris = gdal.Warp("$prodName/descending.tif", arrayOf(sstDescdataset), WarpOptions(gdal.ParseCommandLine("-t_srs EPSG:3857 -tr 0.012349251965289 0.012349251965289 -geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata nan")))
//        monitorFile("$prodName/descending.tif", 600000)
        sstDescdataset.delete()
        ris.delete()
        println("done")

        System.gc()
    }

    @ShellMethod("Convert OLCI land products")
//    fun rebuildOLCI(prodName: String, shpFile: String) {
    fun rebuildOLCILand(prodName: String, force: Boolean = false) {
        if (!force
                && Files.exists(Paths.get(prodName, "ogvi_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "ogvi_lzw_rebuild.tif")) > 50000
                && Files.exists(Paths.get(prodName, "otci_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "otci_lzw_rebuild.tif")) > 50000
                && Files.exists(Paths.get(prodName, "iwv_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "iwv_lzw_rebuild.tif")) > 50000
        ) return
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
//        monitorFile("$prodName/ogvi_warp_rebuild.tif", 60000)
        ogviDTS.delete()
        risOGVI.delete()

        val risOTCI = gdal.Warp("$prodName/otci_warp_rebuild.tif", arrayOf(otciDTS), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata nan")))
//        monitorFile("$prodName/otci_warp_rebuild.tif", 60000)
        otciDTS.delete()
        risOTCI.delete()

        val risIWV = gdal.Warp("$prodName/iwv_warp_rebuild.tif", arrayOf(iwvDTS), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata nan")))
//        monitorFile("$prodName/iwv_warp_rebuild.tif", 60000)
        iwvDTS.delete()
        risIWV.delete()

        var commandOGVI = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 $prodName/ogvi_warp_rebuild.tif $prodName/ogvi_lzw_rebuild.tif"
        println(commandOGVI)
        Runtime.getRuntime().exec(commandOGVI)

        var commandOTCI = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 $prodName/otci_warp_rebuild.tif $prodName/otci_lzw_rebuild.tif"
        println(commandOTCI)
        Runtime.getRuntime().exec(commandOTCI)

        var commandIWV = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 $prodName/iwv_warp_rebuild.tif $prodName/iwv_lzw_rebuild.tif"
        println(commandIWV)
        Runtime.getRuntime().exec(commandIWV)


        var delete = "rm -rf $prodName/reformatted_otci.nc"
        println(delete)
        Runtime.getRuntime().exec(delete)
        delete = "rm -rf $prodName/reformatted_ogvi.nc"
        println(delete)
        Runtime.getRuntime().exec(delete)
        delete = "rm -rf $prodName/reformatted_iwv.nc"
        println(delete)
        Runtime.getRuntime().exec(delete)
        delete = "rm -rf $prodName/otci_warp_rebuild.tif"
        println(delete)
        Runtime.getRuntime().exec(delete)
        delete = "rm -rf $prodName/ogvi_warp_rebuild.tif"
        println(delete)
        Runtime.getRuntime().exec(delete)
        delete = "rm -rf $prodName/iwv_warp_rebuild.tif"
        println(delete)
        Runtime.getRuntime().exec(delete)

        println("done")

    }

    @ShellMethod("Convert OLCI marine products")
    fun rebuildOLCIMarine(prodName: String, force: Boolean = false) {
        if (!force
                && Files.exists(Paths.get(prodName, "chl_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "chl_lzw_rebuild.tif")) > 50000
                && Files.exists(Paths.get(prodName, "tsm_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "tsm_lzw_rebuild.tif")) > 50000
                && Files.exists(Paths.get(prodName, "kd9_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "kd9_lzw_rebuild.tif")) > 50000
                && Files.exists(Paths.get(prodName, "t86_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "t86_lzw_rebuild.tif")) > 50000
                && Files.exists(Paths.get(prodName, "chn_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "chn_lzw_rebuild.tif")) > 50000
                && Files.exists(Paths.get(prodName, "adg_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "adg_lzw_rebuild.tif")) > 50000
                && Files.exists(Paths.get(prodName, "par_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "par_lzw_rebuild.tif")) > 50000
                && Files.exists(Paths.get(prodName, "a86_lzw_rebuild.tif"))
                && Files.size(Paths.get(prodName, "a86_lzw_rebuild.tif")) > 50000
        ) return

        print(" * Converting $prodName... ")

        val chlFile = NetcdfDataset.openDataset("$prodName/chl_oc4me.nc")
        val tsmFile = NetcdfDataset.openDataset("$prodName/tsm_nn.nc")
        val kd9File = NetcdfDataset.openDataset("$prodName/trsp.nc")
        val aerFile = NetcdfDataset.openDataset("$prodName/w_aer.nc") //t86 e a86
        val chnFile = NetcdfDataset.openDataset("$prodName/chl_nn.nc")
        val adgFile = NetcdfDataset.openDataset("$prodName/iop_nn.nc")
        val parFile = NetcdfDataset.openDataset("$prodName/par.nc")
        val flags = NetcdfDataset.openDataset("$prodName/wqsf.nc")
        val geodeticFile = NetcdfDataset.openDataset("$prodName/geo_coordinates.nc")

        val chlData = chlFile.findVariable("CHL_OC4ME").read() as ArrayFloat.D2
        val tsmData = tsmFile.findVariable("TSM_NN").read() as ArrayFloat.D2
        val kd9Data = kd9File.findVariable("KD490_M07").read() as ArrayFloat.D2
        val t86Data = aerFile.findVariable("T865").read() as ArrayFloat.D2
        val a86Data = aerFile.findVariable("A865").read() as ArrayFloat.D2
        val chnData = chnFile.findVariable("CHL_NN").read() as ArrayFloat.D2
        val adgData = adgFile.findVariable("ADG443_NN").read() as ArrayFloat.D2
        val parData = parFile.findVariable("PAR").read() as ArrayFloat.D2

        val flag = flags.findVariable("WQSF").read() as ArrayLong.D2

        val latData = geodeticFile.findVariable("latitude").read() as ArrayDouble.D2
        val lonData = geodeticFile.findVariable("longitude").read() as ArrayDouble.D2

        val dimensions = chlFile.findVariable("CHL_OC4ME").dimensions

        maskSunglint(chlData, flag)
        maskSunglint(tsmData, flag)
        maskSunglint(kd9Data, flag)
        maskSunglint(t86Data, flag)
        maskSunglint(a86Data, flag)
        maskSunglint(chnData, flag)
        maskSunglint(adgData, flag)
        maskSunglint(parData, flag)

        val writerCHL = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_chl.nc")
        val writerTSM = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_tsm.nc")
        val writerKD9 = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_kd9.nc")
        val writerT86 = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_t86.nc")
        val writerA86 = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_a86.nc")
        val writerCHN = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_chn.nc")
        val writerADG = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_adg.nc")
        val writerPAR = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted_par.nc")

        val newDimensionsCHL = getDimension(writerCHL, dimensions)
        val newDimensionsTSM = getDimension(writerTSM, dimensions)
        val newDimensionsKD9 = getDimension(writerKD9, dimensions)
        val newDimensionsT86 = getDimension(writerT86, dimensions)
        val newDimensionsA86 = getDimension(writerA86, dimensions)
        val newDimensionsCHN = getDimension(writerCHN, dimensions)
        val newDimensionsADG = getDimension(writerADG, dimensions)
        val newDimensionsPAR = getDimension(writerPAR, dimensions)

        // populate
        val chl = writerCHL.addVariable(null, "chl", DataType.FLOAT, newDimensionsCHL)
        val tsm = writerTSM.addVariable(null, "tsm", DataType.FLOAT, newDimensionsTSM)
        val kd9 = writerKD9.addVariable(null, "kd9", DataType.FLOAT, newDimensionsKD9)
        val t86 = writerT86.addVariable(null, "t86", DataType.FLOAT, newDimensionsT86)
        val a86 = writerA86.addVariable(null, "a86", DataType.FLOAT, newDimensionsA86)
        val chn = writerCHN.addVariable(null, "chn", DataType.FLOAT, newDimensionsCHN)
        val adg = writerADG.addVariable(null, "adg", DataType.FLOAT, newDimensionsADG)
        val par = writerPAR.addVariable(null, "par", DataType.FLOAT, newDimensionsPAR)

        chl.addAll(chlFile.findVariable("CHL_OC4ME").attributes)
        chl.addAttribute(Attribute("_FillValue", "nan"))

        tsm.addAll(tsmFile.findVariable("TSM_NN").attributes)
        tsm.addAttribute(Attribute("_FillValue", "nan"))

        kd9.addAll(kd9File.findVariable("KD490_M07").attributes)
        kd9.addAttribute(Attribute("_FillValue", "nan"))

        t86.addAll(aerFile.findVariable("T865").attributes)
        t86.addAttribute(Attribute("_FillValue", "nan"))

        a86.addAll(aerFile.findVariable("A865").attributes)
        a86.addAttribute(Attribute("_FillValue", "nan"))

        chn.addAll(chnFile.findVariable("CHL_NN").attributes)
        chn.addAttribute(Attribute("_FillValue", "nan"))

        adg.addAll(adgFile.findVariable("ADG443_NN").attributes)
        adg.addAttribute(Attribute("_FillValue", "nan"))

        par.addAll(parFile.findVariable("PAR").attributes)
        par.addAttribute(Attribute("_FillValue", "nan"))

        // create the file
        createNetcdf(writerCHL, chl, chlData, geodeticFile, newDimensionsCHL, latData, lonData)
        createNetcdf(writerTSM, tsm, tsmData, geodeticFile, newDimensionsTSM, latData, lonData)
        createNetcdf(writerKD9, kd9, kd9Data, geodeticFile, newDimensionsKD9, latData, lonData)
        createNetcdf(writerT86, t86, t86Data, geodeticFile, newDimensionsT86, latData, lonData)
        createNetcdf(writerA86, a86, a86Data, geodeticFile, newDimensionsA86, latData, lonData)
        createNetcdf(writerCHN, chn, chnData, geodeticFile, newDimensionsCHN, latData, lonData)
        createNetcdf(writerADG, adg, adgData, geodeticFile, newDimensionsADG, latData, lonData)
        createNetcdf(writerPAR, par, parData, geodeticFile, newDimensionsPAR, latData, lonData)

        flags.close()
        geodeticFile.close()

        val wgs84 = SpatialReference()
        wgs84.ImportFromEPSG(4326)

        val chlDTS = gdal.Open("NETCDF:$prodName/reformatted_chl.nc:chl")
        val tsmDTS = gdal.Open("NETCDF:$prodName/reformatted_tsm.nc:tsm")
        val kd9DTS = gdal.Open("NETCDF:$prodName/reformatted_kd9.nc:kd9")
        val t86DTS = gdal.Open("NETCDF:$prodName/reformatted_t86.nc:t86")
        val a86DTS = gdal.Open("NETCDF:$prodName/reformatted_a86.nc:a86")
        val chnDTS = gdal.Open("NETCDF:$prodName/reformatted_chn.nc:chn")
        val adgDTS = gdal.Open("NETCDF:$prodName/reformatted_adg.nc:adg")
        val parDTS = gdal.Open("NETCDF:$prodName/reformatted_par.nc:par")

        val mapCHL = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_chl.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_chl.nc:lat"
        )

        val mapTSM = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_tsm.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_tsm.nc:lat"
        )

        val mapKD9 = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_kd9.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_kd9.nc:lat"
        )

        val mapT86 = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_t86.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_t86.nc:lat"
        )

        val mapA86 = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_a86.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_a86.nc:lat"
        )

        val mapCHN = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_chn.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_chn.nc:lat"
        )

        val mapADG = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_adg.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_adg.nc:lat"
        )

        val mapPAR = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName/reformatted_par.nc:lon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName/reformatted_par.nc:lat"
        )

        chlDTS.SetMetadata(Hashtable(mapCHL), "GEOLOCATION")
        tsmDTS.SetMetadata(Hashtable(mapTSM), "GEOLOCATION")
        kd9DTS.SetMetadata(Hashtable(mapKD9), "GEOLOCATION")
        t86DTS.SetMetadata(Hashtable(mapT86), "GEOLOCATION")
        a86DTS.SetMetadata(Hashtable(mapA86), "GEOLOCATION")
        chnDTS.SetMetadata(Hashtable(mapCHN), "GEOLOCATION")
        adgDTS.SetMetadata(Hashtable(mapADG), "GEOLOCATION")
        parDTS.SetMetadata(Hashtable(mapPAR), "GEOLOCATION")

        createTiff(prodName, "chl", chlDTS)
        createTiff(prodName, "tsm", tsmDTS)
        createTiff(prodName, "kd9", kd9DTS)
        createTiff(prodName, "t86", t86DTS)
        createTiff(prodName, "a86", a86DTS)
        createTiff(prodName, "chn", chnDTS)
        createTiff(prodName, "adg", adgDTS)
        createTiff(prodName, "par", parDTS)

        Files.delete(Paths.get("$prodName/reformatted_chl.nc"))
        Files.delete(Paths.get("$prodName/reformatted_tsm.nc"))
        Files.delete(Paths.get("$prodName/reformatted_kd9.nc"))
        Files.delete(Paths.get("$prodName/reformatted_t86.nc"))
        Files.delete(Paths.get("$prodName/reformatted_a86.nc"))
        Files.delete(Paths.get("$prodName/reformatted_chn.nc"))
        Files.delete(Paths.get("$prodName/reformatted_adg.nc"))
        Files.delete(Paths.get("$prodName/reformatted_par.nc"))

        Files.delete(Paths.get("$prodName/chl_warp_rebuild.tif"))
        Files.delete(Paths.get("$prodName/tsm_warp_rebuild.tif"))
        Files.delete(Paths.get("$prodName/kd9_warp_rebuild.tif"))
        Files.delete(Paths.get("$prodName/t86_warp_rebuild.tif"))
        Files.delete(Paths.get("$prodName/a86_warp_rebuild.tif"))
        Files.delete(Paths.get("$prodName/chn_warp_rebuild.tif"))
        Files.delete(Paths.get("$prodName/adg_warp_rebuild.tif"))
        Files.delete(Paths.get("$prodName/par_warp_rebuild.tif"))

        System.gc()

        println("done")

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

    fun logToLinear(array: ArrayFloat.D2): ArrayFloat.D2 {
        val converted = ArrayFloat.D2(array.shape[0], array.shape[1])
        for (y in 0 until array.shape[0])
            for (x in 0 until array.shape[1])
                converted[y, x] = Math.pow(10.0, array[y, x].toDouble()).toFloat()
        return converted
    }

    fun maskSunglint(input: ArrayFloat.D2, flags: ArrayLong.D2) {
        for (y in 0 until input.shape[0])
            for (x in 0 until input.shape[1])
                when {
                    (flags[y, x].toInt() and 2048 == 2048) or (flags[y, x].toInt() and 4096 == 4096) -> {
                        input[y, x] = Float.NaN
                    }
                }
    }

    fun getDimension(writer: NetcdfFileWriter, dim: MutableList<Dimension>): MutableList<Dimension> {
        val newDimensions = mutableListOf<Dimension>(
                writer.addDimension(null, dim[0].fullName, dim[0].length),
                writer.addDimension(null, dim[1].fullName, dim[1].length)
        )
        return newDimensions
    }

    fun createNetcdf(writer: NetcdfFileWriter, varbl: Variable, data: ArrayFloat.D2, geodetic: NetcdfDataset, dim: MutableList<Dimension>, ltData: ArrayDouble.D2, lnData: ArrayDouble.D2) {

        val lt = writer.addVariable(null, "lat", DataType.DOUBLE, dim)
        lt.addAll(geodetic.findVariable("latitude").attributes)

        val ln = writer.addVariable(null, "lon", DataType.DOUBLE, dim)
        ln.addAll(geodetic.findVariable("longitude").attributes)

        try {
            writer.create()
            writer.write(varbl, data)
            writer.write(lt, ltData)
            writer.write(ln, lnData)
        } catch (e: IOException) {
            print("ERROR creating reformatted file: ${e.message}")
            throw e
        } finally {
            writer.close()
            writer.close()
        }

    }

    fun createTiff(prodName: String, outputPattern: String, dts: Dataset) {

        val ris = gdal.Warp("$prodName/" + outputPattern + "_warp_rebuild.tif", arrayOf(dts), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW -srcnodata 0 -dstnodata nan")))
        dts.delete()
        ris.delete()

        var command = "gdal_translate -co COMPRESS=LZW -a_srs EPSG:4326 $prodName/" + outputPattern + "_warp_rebuild.tif $prodName/" + outputPattern + "_lzw_rebuild.tif"
        println(command)
        Runtime.getRuntime().exec(command)

        System.gc()

    }

    fun createVRT(outputPattern: String, pythonFunction: String) {

        var t = Files.readAllLines(Paths.get("merged_" + outputPattern + ".vrt"))
        for (i in 0..t.size) {
            if (t[i].contains("<VRTRasterBand")) {
                t[i] = t[i].replace("<VRTRasterBand", "<VRTRasterBand subClass=\"VRTDerivedRasterBand\"")
                t.add(i + 1, "    <PixelFunctionType>add</PixelFunctionType>")
                t.add(i + 2, "    <PixelFunctionLanguage>Python</PixelFunctionLanguage>")
                t.add(i + 3, "    <PixelFunctionCode><![CDATA[")
                t.add(i + 4, "import numpy as np")
                t.add(i + 5, "def add(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):")
//                t.add(i+6, "    np.round_(np.nanmean(in_ar, axis = 0, dtype = 'float32'), decimals=5, out = out_ar)")
//                t.add(i+6, "    np.round_(np.nanmean(np.power(10,in_ar,dtype = 'float32'),dtype = 'float32', axis = 0), decimals=5, out = out_ar)")
//                t.add(i+6, "    np.round_(np.log10(np.nanmean(np.power(10,in_ar,dtype = 'float32'), axis = 0, dtype = 'float32'),dtype = 'float32'), decimals=5, out = out_ar)")
                t.add(i + 6, "    " + pythonFunction)
                t.add(i + 7, "]]>")
                t.add(i + 8, "    </PixelFunctionCode>")
            }
        }
        Files.write(Paths.get("merged_" + outputPattern + ".vrt"), t)

    }

    fun Double.format(digits: Int) = java.lang.String.format("%.${digits}f", this)
}
