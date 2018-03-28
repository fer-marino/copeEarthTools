package org.esb.tools.controllers

import org.esb.tools.Utils
import org.gdal.gdal.*
import org.gdal.osr.SpatialReference
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

@ShellComponent
class Sentinel1Commands {

    @ShellMethod("Convert and merge multiple OCN products")
    fun ocnMerge(pattern: String, @ShellOption(defaultValue = "-projwin 17 41.5 21.5 39.5") outputOptions:String) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
        if(matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

        val uList = mutableListOf<Dataset>()
        val vList = mutableListOf<Dataset>()
        matches.filter { it.isFile }
                .map { prod -> ocnToAsciiGrid(prod.file.absolutePath, volatile =  true) }
                .forEach { uList.add(it.first); vList.add(it.second) }

        var t = gdal.BuildVRT("merge", uList.toTypedArray(), BuildVRTOptions( gdal.ParseCommandLine("-resolution average")) )
        gdal.Translate("U10.asc", t, TranslateOptions( gdal.ParseCommandLine("-of AAIGrid -co force_cellsize=true $outputOptions") ) )
        uList.forEach { it.delete(); it.GetFileList().forEach { Files.deleteIfExists(Paths.get(it.toString())) } }
        t.delete()

        t = gdal.BuildVRT("merge", vList.toTypedArray(), BuildVRTOptions( gdal.ParseCommandLine("-r cubicspline -resolution average")) )
        gdal.Translate("V10.asc", t, TranslateOptions( gdal.ParseCommandLine("-of AAIGrid -co force_cellsize=true $outputOptions") ) )
        vList.forEach { it.delete(); it.GetFileList().forEach { Files.deleteIfExists(Paths.get(it.toString())) } }
        t.delete()
        println(" * done")
    }

    @ShellMethod("Convert and merge multiple OCN products")
    fun ocnMergeGeotiff(pattern: String, @ShellOption(defaultValue = "-projwin 7.5 44 21.5 35") outputOptions:String) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
        if(matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

        val uListA = mutableListOf<Dataset>(); val uListD = mutableListOf<Dataset>()
        val vListA = mutableListOf<Dataset>(); val vListD = mutableListOf<Dataset>()
        matches.filter { it.isFile }.forEach {
            val t = ocnToAsciiGrid(it.file.absolutePath, volatile =  false)
            if(Utils.isAscending(it.file.absolutePath)) {
                uListA.add(t.first); vListA.add(t.second)
            } else {
                uListD.add(t.first); vListD.add(t.second)
            }
        }

        if(uListA.isNotEmpty()) {
            val umergea = gdal.BuildVRT("umergea", uListA.toTypedArray(), BuildVRTOptions(gdal.ParseCommandLine("-r cubicspline -resolution average")))
            val vmergea = gdal.BuildVRT("vmergea", vListA.toTypedArray(), BuildVRTOptions(gdal.ParseCommandLine("-r cubicspline -resolution average")))
            val tA = gdal.BuildVRT("mergea", arrayOf(umergea, vmergea), BuildVRTOptions(gdal.ParseCommandLine("-separate")))
            println(" * Merging ${uListA.size} ascending products...")
            Thread.sleep(500)
            gdal.Translate("winds-ascending.tif", tA, TranslateOptions(gdal.ParseCommandLine("-of gtiff -oo COMPRESS=LZW $outputOptions")), TermProgressCallback())
            Thread.sleep(500)
            tA.delete()
            umergea.delete()
            vmergea.delete()
        }
        if(uListD.isNotEmpty()) {
            val umerged = gdal.BuildVRT("umerged", uListD.toTypedArray(), BuildVRTOptions(gdal.ParseCommandLine("-r cubicspline -resolution average")))
            val vmerged = gdal.BuildVRT("vmerged", vListD.toTypedArray(), BuildVRTOptions(gdal.ParseCommandLine("-r cubicspline -resolution average")))
            val tD = gdal.BuildVRT("merged", arrayOf(umerged, vmerged), BuildVRTOptions(gdal.ParseCommandLine("-separate")))
            println(" * Merging ${uListD.size} descending products...")
            Thread.sleep(500)
            gdal.Translate("winds-decending.tif", tD, TranslateOptions(gdal.ParseCommandLine("-of gtiff -oo COMPRESS=LZW $outputOptions")), TermProgressCallback())
            Thread.sleep(500)
            tD.delete()
            umerged.delete()
            vmerged.delete()
        }
        if(uListA.isEmpty() && uListD.isEmpty())
            println(" * Nothing to merge")

        println(" * done")
    }

    @ShellMethod("Convert OCN files to ASCII Grid")
    fun ocnToAsciiGrid(prodName: String,
                       @ShellOption(defaultValue = "AAIGrid") outputFormat: String = "GTiff",
                       @ShellOption(defaultValue = "false") volatile: Boolean = false): Pair<Dataset, Dataset> {
        println(" * Converting $prodName...")
        val wgs84 = SpatialReference()
        wgs84.ImportFromEPSG(4326)

        val prodFile = Files.list(Paths.get(prodName, "measurement")).findFirst()

        if(!prodFile.isPresent) throw IllegalArgumentException("no measurement file found. Corrupted product?")

        val direction = gdal.Open("NETCDF:${prodFile.get()}:owiWindDirection")
        val speed = gdal.Open("NETCDF:${prodFile.get()}:owiWindSpeed")

        val map = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:${prodFile.get()}:owiLon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:${prodFile.get()}:owiLat"
        )
        direction.SetMetadata(Hashtable(map), "GEOLOCATION")
        speed.SetMetadata(Hashtable(map), "GEOLOCATION")

        val directionWarp = gdal.AutoCreateWarpedVRT(direction, wgs84.ExportToWkt())
        val speedWarp = gdal.AutoCreateWarpedVRT(speed, wgs84.ExportToWkt())

        val uFilename = if(volatile) Files.createTempFile("U-", ".tif").toString() else "$prodName/U10.tif"
        val u = gdal.GetDriverByName("MEM").CreateCopy(uFilename, directionWarp)
        val vFilename = if(volatile) Files.createTempFile("V-", ".tif").toString() else "$prodName/V10.tif"
        val v = gdal.GetDriverByName("MEM").CreateCopy(vFilename, directionWarp)

        val no_data = Array(1, {0.0})
        speed.GetRasterBand(1).GetNoDataValue(no_data)
        val xSize = directionWarp.rasterXSize
        val ySize = directionWarp.rasterYSize
        val directionArray = FloatArray(xSize * ySize)
        val speedArray = FloatArray(xSize * ySize)
        val uArray = FloatArray(xSize * ySize)
        val vArray = FloatArray(xSize * ySize)

        directionWarp.GetRasterBand(1).ReadRaster(0, 0, xSize, ySize, directionArray)
        speedWarp.GetRasterBand(1).ReadRaster(0, 0, xSize, ySize, speedArray)

        for(y in 0 until ySize)
            for(x in 0 until xSize) {
                val i = y * xSize + x
                if(speedArray[i] == 0f || speedArray[i] == no_data[0].toFloat()) {
                    uArray[i] = 0f
                    vArray[i] = 0f
                } else
                    Utils.polarToRectangular(speedArray[i].toDouble(), directionArray[i].toDouble()).apply {
                        uArray[i] = first.toFloat()
                        vArray[i] = second.toFloat()
                    }
            }

        u.GetRasterBand(1).WriteRaster(0, 0, xSize, ySize, uArray)
        v.GetRasterBand(1).WriteRaster(0, 0, xSize, ySize, vArray)
        u.GetRasterBand(1).SetNoDataValue(.0)
        v.GetRasterBand(1).SetNoDataValue(.0)

        val translateOptions = if(outputFormat.toLowerCase().equals("aaigrid"))
            TranslateOptions(gdal.ParseCommandLine("-of AAIGrid -co force_cellsize=true"))
        else
            TranslateOptions(gdal.ParseCommandLine("-of $outputFormat"))

        gdal.Translate(uFilename, u, translateOptions)
        gdal.Translate(vFilename, v, translateOptions)

        directionWarp.delete()
        speedWarp.delete()
        direction.delete()
        speed.delete()

        return u to v
    }

}