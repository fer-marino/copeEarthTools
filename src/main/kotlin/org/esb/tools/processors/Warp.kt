package org.esb.tools.processors

import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import org.osgeo.proj4j.BasicCoordinateTransform
import org.osgeo.proj4j.ProjCoordinate
import org.osgeo.proj4j.Registry
import org.osgeo.proj4j.parser.Proj4Parser
import ucar.ma2.ArrayFloat

class Warp {
    companion object {
        data class WarpOutput (var lat: ArrayFloat.D2, var lon: ArrayFloat.D2, var data: ArrayFloat.D2)

        private val projParser = Proj4Parser(Registry())
        private val transform by lazy {
            BasicCoordinateTransform(projParser.parse("WGS84", arrayOf("+proj=longlat", "+ellps=WGS84", "+datum=WGS84", "+units=degrees", "+no_defs")),
                projParser.parse("GOOGLE", arrayOf("+proj=merc", "+lon_0=0", "+k=1", "+x_0=0", "+y_0=0", "+a=6378137", "+b=6378137", "+towgs84=0,0,0,0,0,0,0", "+units=m", "+no_defs"))
            )
        }
        private val transformReverse by lazy {
            BasicCoordinateTransform(projParser.parse("GOOGLE", arrayOf("+proj=merc", "+lon_0=0", "+k=1", "+x_0=0", "+y_0=0", "+a=6378137", "+b=6378137", "+towgs84=0,0,0,0,0,0,0", "+units=m", "+no_defs")),
                    projParser.parse("WGS84", arrayOf("+proj=longlat", "+ellps=WGS84", "+datum=WGS84", "+units=degrees", "+no_defs"))
            )
        }

        fun warp(lat: Float, lon: Float): ProjCoordinate {
            return transform.transform(ProjCoordinate(lon.toDouble(), lat.toDouble()), ProjCoordinate())
        }

        fun warpReverse(x: Double, y: Double): ProjCoordinate {
            return transformReverse.transform(ProjCoordinate(x, y), ProjCoordinate())
        }

        fun warp(lat: ArrayFloat.D2, lon: ArrayFloat.D2, data: ArrayFloat.D2, pixelSize: Double, interpolate: ((ArrayFloat.D2) -> ArrayFloat.D2)? = null): WarpOutput {
            val statsLat = SummaryStatistics()
            val statsLon = SummaryStatistics()

            for(i in 0 until data.shape[0])
                for(j in 0 until data.shape[1])
                    warp(lat[i, j], lon[i, j]).apply {
                        statsLat.addValue(y)
                        statsLon.addValue(x)
                    }

            // recalculate output dimensions, assuming square pixels
            val xSize = ((statsLon.max - statsLon.min) / pixelSize).toInt() +1
            val ySize = ((statsLat.max - statsLat.min) / pixelSize).toInt() +1
            val out = ArrayFloat.D2(ySize, xSize)
            val outLat = ArrayFloat.D2(ySize, xSize)
            val outLon = ArrayFloat.D2(ySize, xSize)

            // init to NaN
            for(i in 0 until out.shape[0])
                for(j in 0 until out.shape[1]) {
                    out[i, j] = Float.NaN
                    warpReverse(statsLon.min + j*pixelSize, statsLat.min + i*pixelSize).apply {
                        outLat[i, j] = y.toFloat()
                        outLon[i, j] = x.toFloat()
                    }
                }

            val d = ySize / (statsLat.max - statsLat.min)
            // warp
            for(i in 0 until data.shape[0])
                for(j in 0 until data.shape[1]) {
                    val coord = warp(lat[i, j], lon[i, j])
                    val tLat = ((coord.y - statsLat.min) / pixelSize).toInt()
                    val tLon = ((coord.x - statsLon.min) / pixelSize).toInt()

                    out[tLat, tLon] = data[i, j]
                }

            val interpolated = interpolate?.invoke(out)

            return if(interpolated != null)
                WarpOutput(outLat, outLon, interpolated)
            else
                WarpOutput(outLat, outLon, out)
        }

        fun interpolateBilinear(input: ArrayFloat.D2): ArrayFloat.D2 {
            val out = input.copy() as ArrayFloat.D2
            for(i in 0 until input.shape[0])
                for(j in 0 until input.shape[1]) {
                    if(input[i, j].isNaN()) {
                        // scan neighbourhood
                        val t = SummaryStatistics()

                        if(j-1 >= 0) {
                            if(i-1 >= 0 && !input[i-1, j-1].isNaN())
                                t.addValue(input[i-1, j-1].toDouble())
                            if(!input[i, j-1].isNaN())
                                t.addValue(input[i, j-1].toDouble())
                            if(i+1 <= input.shape[0]-1 && !input[i+1, j-1].isNaN())
                                t.addValue(input[i+1, j-1].toDouble())
                        }

                        if(i-1 >= 0 && !input[i-1, j].isNaN())
                            t.addValue(input[i-1, j].toDouble())
                        if(i+1 <= input.shape[0]-1 && !input[i+1, j].isNaN())
                            t.addValue(input[i+1, j].toDouble())

                        if(j+1 <= input.shape[1]-1) {
                            if(i-1 >= 0 && !input[i-1, j+1].isNaN())
                                t.addValue(input[i-1, j+1].toDouble())
                            if(!input[i, j+1].isNaN())
                                t.addValue(input[i, j+1].toDouble())
                            if(i+1 <= input.shape[0]-1 && !input[i+1, j+1].isNaN())
                                t.addValue(input[i+1, j+1].toDouble())
                        }

                        if(t.n >= 4)
                            out[i, j] = t.mean.toFloat()
                    }

                }
            return out
        }
    }
}