package org.esb.tools.controllers

import org.esb.tools.Utils
import org.gdal.gdal.TranslateOptions
import org.gdal.gdal.gdal
import org.gdal.osr.SpatialReference
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import java.util.*

@ShellComponent
class Sentinel1Commands {

    @ShellMethod("Convert OCN files to ASCII Grid")
    fun ocnToAsciiGrid(prodName: String) {
        val start = System.currentTimeMillis()

        val wgs84 = SpatialReference()
        wgs84.ImportFromEPSG(4326)

        val direction = gdal.Open("NETCDF:$prodName:owiWindDirection")
        val speed = gdal.Open("NETCDF:$prodName:owiWindSpeed")

        val map = mapOf(
                "LINE_OFFSET" to "1", "LINE_STEP" to "1",
                "PIXEL_OFFSET" to "1", "PIXEL_STEP" to "1",
                "X_BAND" to "1", "X_DATASET" to "NETCDF:$prodName:owiLon",
                "Y_BAND" to "1", "Y_DATASET" to "NETCDF:$prodName:owiLat"
        )
        direction.SetMetadata(Hashtable(map), "GEOLOCATION")
        speed.SetMetadata(Hashtable(map), "GEOLOCATION")

        val directionWarp = gdal.AutoCreateWarpedVRT(direction, wgs84.ExportToWkt())
        val speedWarp = gdal.AutoCreateWarpedVRT(speed, wgs84.ExportToWkt())

        val u = gdal.GetDriverByName("MEM").CreateCopy("U", directionWarp)
        val v = gdal.GetDriverByName("MEM").CreateCopy("V", directionWarp)

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
                // TODO handle no_data
                val i = y * xSize + x
                val vect = Utils.polarToRectangular(speedArray[i].toDouble(), directionArray[i].toDouble())
                uArray[i] = vect.first.toFloat()
                vArray[i] = vect.second.toFloat()
            }

        u.GetRasterBand(1).WriteRaster(0, 0, xSize, ySize, uArray)
        v.GetRasterBand(1).WriteRaster(0, 0, xSize, ySize, vArray)

        gdal.Translate("U10.asc", u, TranslateOptions( gdal.ParseCommandLine("-of AAIGrid -co force_cellsize=true") ) )
        gdal.Translate("V10.asc", v, TranslateOptions( gdal.ParseCommandLine("-of AAIGrid -co force_cellsize=true") ) )

        u.delete()
        u.delete()
        directionWarp.delete()
        speedWarp.delete()
        direction.delete()
        speed.delete()

        println(" * * completed in ${System.currentTimeMillis() - start} msec")
    }

}