package org.esb.tools.controllers

import org.gdal.gdal.TranslateOptions
import org.gdal.gdal.WarpOptions
import org.gdal.gdal.gdal
import org.gdal.osr.SpatialReference
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ucar.ma2.ArrayDouble
import ucar.ma2.ArrayFloat
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
                .map { prod -> lstToGeotiff(prod.file.absolutePath) }
    }


    @ShellMethod("Convert LST products")
    fun lstToGeotiff(prodName: String, @ShellOption(defaultValue = "-projwin 17 41.5 21.5 39.5") outputOptions: String = "colScaled.txt") {
        println(" * Converting $prodName...")
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
        lst.GetRasterBand(1).SetNoDataValue(-1.0)

        val warp = gdal.AutoCreateWarpedVRT(lst, wgs84.ExportToWkt())
        println(gdal.GDALInfo(warp, null))
        gdal.Translate("$prodName/lst.tif", warp, TranslateOptions( gdal.ParseCommandLine("-oo GTIFF_HONOUR_NEGATIVE_SCALEY=YES")) )

        lst.delete()
    }

    @ShellMethod("Convert LST products")
    fun rebuildLST(prodName: String ) {
        val lstFile = NetcdfDataset.openDataset("$prodName/LST_in.nc")
        val geodeticFile = NetcdfDataset.openDataset("$prodName/geodetic_in.nc")

        val lstData = lstFile.findVariable("LST").read() as ArrayFloat.D2
        val latData = geodeticFile.findVariable("latitude_in").read() as ArrayDouble.D2
        val lonData = geodeticFile.findVariable("longitude_in").read() as ArrayDouble.D2

        for(y in 0 until lstData.shape[0])
            for(x in 0 until lstData.shape[1])
                if(lstData[y, x].isNaN()) lstData[y, x] = -1f

        val dimensions = lstFile.findVariable("LST").dimensions

        val writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "$prodName/reformatted.nc")

        val newDimensions = mutableListOf<Dimension>(
            writer.addDimension(null, dimensions[0].fullName, dimensions[0].length),
            writer.addDimension(null, dimensions[1].fullName, dimensions[1].length)
        )

        // populate
//        val dimensionsList = listOf(latDimension, lonDimension)
        val lst = writer.addVariable(null, "surface_temperature", DataType.FLOAT, newDimensions)
        lst.addAll(lstFile.findVariable("LST").attributes)
//        lst.addAttribute(Attribute("coordinates", "lon lat"))
//        lst.addAttribute(Attribute("grid_mapping", "crs"))



        val lat = writer.addVariable(null, "lat", DataType.DOUBLE, newDimensions)
        lat.addAll(geodeticFile.findVariable("latitude_in").attributes)

        val lon = writer.addVariable(null, "lon", DataType.DOUBLE, newDimensions)
        lon.addAll(geodeticFile.findVariable("longitude_in").attributes)

//        writer.addGroupAttribute(null, Attribute("Conventions", "CF-1.0"))

        // create the file
        try {
            writer.create()
            writer.write(lst, lstData)
            writer.write(lat, latData)
            writer.write(lon, lonData)
        } catch (e: IOException) {
            print("ERROR creating file $prodName/reformatted.nc: ${e.message}")
        }

        writer.close()
        lstFile.close()
        geodeticFile.close()
    }

    @ShellMethod("Convert LFR products")
    fun ogviConvert(prodName: String ) {
        println(" * Converting $prodName...")
        val wgs84 = SpatialReference()
        wgs84.ImportFromEPSG(4326)

        val ogvi = gdal.Open("HDF5:$prodName/ogvi.nc://OGVI")
        val map = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "HDF5:$prodName/geo_coordinates.nc://longitude",
                "Y_BAND" to "1", "Y_DATASET" to "HDF5:$prodName/geo_coordinates.nc://latitude"
        )

        ogvi.SetMetadata(Hashtable(map), "GEOLOCATION")

        gdal.Warp("$prodName/ogvi.tif", arrayOf(ogvi), WarpOptions(gdal.ParseCommandLine("-geoloc")))

        println(" * Complete")
    }

    @ShellMethod("Convert LST products")
    fun lstConvert(prodName: String ) {
        println(" * Converting $prodName...")
        val wgs84 = SpatialReference()
        wgs84.ImportFromEPSG(4326)

        val lst = gdal.Open("HDF5:$prodName/LST_in.nc://LST")
        val map = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "HDF5:$prodName/geodetic_in.nc://longitude_in",
                "Y_BAND" to "1", "Y_DATASET" to "HDF5:$prodName/geodetic_in.nc://latitude_in"
        )

        lst.SetMetadata(Hashtable(map), "GEOLOCATION")

        gdal.Warp("$prodName/lst.tif", arrayOf(lst), WarpOptions(gdal.ParseCommandLine("-geoloc")))

        println(" * Complete")
    }

    @ShellMethod("gdal info")
    fun info(prodName: String) {
        println(gdal.GDALInfo(gdal.Open(prodName), null))
    }

}