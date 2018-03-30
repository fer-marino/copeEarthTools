package org.esb.tools.controllers

import org.esb.tools.Utils
import org.gdal.gdal.BuildVRTOptions
import org.gdal.gdal.InfoOptions
import org.gdal.gdal.TranslateOptions
import org.gdal.gdal.WarpOptions
import org.gdal.gdal.gdal
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

        gdal.Translate("ascending.tif", asc, TranslateOptions( gdal.ParseCommandLine(outputOptions) ) )
        gdal.Translate("descending.tif", desc, TranslateOptions( gdal.ParseCommandLine(outputOptions) ) )

        while (true) {
            if (Files.notExists(Paths.get("ascending.tif"))) {
                Thread.sleep(500)
                continue
            }

            if (Files.size(Paths.get("ascending.tif")) < 50_000) {
                Thread.sleep(500)
                continue
            }

            break
        }

        desc.delete()
        asc.delete()
        println("done")
    }

    @ShellMethod("Convert LST products")
    fun rebuildLST(prodName: String) {
        if(Files.exists(Paths.get(prodName, "lst_warp_rebuild.tif")) && Files.size(Paths.get(prodName, "lst_warp_rebuild.tif")) > 50000) return

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
                    !(x in 30..lstData.shape[1]-30 || y in 30..lstData.shape[0]-30) -> lstDataConv[y, x] = -32767 // stay away from borders
                    lstData[y, x].isNaN() -> lstDataConv[y, x] = -32767 // no data
                    DataType.unsignedShortToInt(confidenceIn[y, x]) and 16384 == 16384 -> {
                        lstDataConv[y, x] = -32767
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
        val out = Paths.get("$prodName/lst_warp_rebuild.tif")
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
        lst.delete()
        println("done")
    }

    @ShellMethod("gdal info")
    fun info(prodName: String) {
        println(gdal.GDALInfo(gdal.Open(prodName), InfoOptions(gdal.ParseCommandLine("-hist -stats"))))
    }

    fun Double.format(digits: Int) = java.lang.String.format("%.${digits}f", this)
}
