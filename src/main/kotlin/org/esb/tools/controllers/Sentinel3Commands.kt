package org.esb.tools.controllers

import org.gdal.gdal.InfoOptions
import org.gdal.gdal.ProgressCallback
import org.gdal.gdal.WarpOptions
import org.gdal.gdal.gdal
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
import java.util.*


@ShellComponent
class Sentinel3Commands {

    @ShellMethod("Convert and merge multiple OCN products")
    fun lstProcessAndMerge(pattern: String) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$pattern")
        if(matches.isEmpty()) {
            println(" * No product matches the pattern '$pattern'")
            return
        }

        matches.filter { it.isFile }
                .map { prod -> rebuildLST(prod.file.absolutePath) }
    }

    @ShellMethod("Convert LST products")
    fun rebuildLST(prodName: String, @ShellOption(defaultValue = "-projwin 17 41.5 21.5 39.5") outputOptions: String = "") {
        println(" * Converting $prodName...")
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

        for(y in 0 until lstData.shape[0])
            for(x in 0 until lstData.shape[1]) {
                if (lstData[y, x].isNaN())
                    lstDataConv[y, x] = 0
                else if(DataType.unsignedShortToInt(confidenceIn[y, x]) and 16384 == 16384) {
                    lstDataConv[y, x] = 0; cloud++
                } else
                    lstDataConv[y, x] = lstData[y, x].toShort()
            }

        println("cloudy pixels $cloud of ${shape[0] * shape[1]}")

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
//        lst.GetRasterBand(1).SetNoDataValue(0.0)

        gdal.Warp("$prodName/lst_warp_rebuild.tif", arrayOf(lst), WarpOptions(gdal.ParseCommandLine("-geoloc -oo COMPRESS=LZW")), EsbGdalCallback())

        lst.delete()
    }

    @ShellMethod("gdal info")
    fun info(prodName: String) {
        println(gdal.GDALInfo(gdal.Open(prodName), InfoOptions(gdal.ParseCommandLine("-hist -stats"))))
    }

    class EsbGdalCallback: ProgressCallback() {
        override fun run(dfComplete: Double, pszMessage: String?): Int {

            print("\r * ${dfComplete.format(2)} $pszMessage ")
            if(dfComplete == 1.0) println()
            return super.run(dfComplete, pszMessage)
        }
    }

}

fun Double.format(digits: Int) = java.lang.String.format("%.${digits}f", this)