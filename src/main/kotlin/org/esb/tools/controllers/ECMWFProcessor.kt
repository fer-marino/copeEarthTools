package org.esb.tools.controllers

import imageTracer.GeoJsonUtils
import imageTracer.ImageTracer
import imageTracer.ImageTracer.imagedataToGeoJson
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ucar.ma2.ArrayFloat
import ucar.nc2.dataset.NetcdfDataset
import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import javax.imageio.ImageIO


@ShellComponent
class ECMWFProcessor {

    @ShellMethod("Load all Wind ")
    fun loadECMWF(@ShellOption(defaultValue = "wrfout_d03_2017-07-13_12_00_00") file: String,
                    @ShellOption(defaultValue = "test.json") jsonFile: String) {
        val start = System.currentTimeMillis()

        val dataset = NetcdfDataset.openDataset(file)
        val landMask = dataset.findVariable("LANDMASK").read() as ArrayFloat.D3
        val lat = dataset.findVariable("XLAT").read() as ArrayFloat.D3
        val lon = dataset.findVariable("XLONG").read() as ArrayFloat.D3
//        val windU = dataset.findVariable("U10").read() as ArrayFloat.D3
//        var windV = dataset.findVariable("V10").read() as ArrayFloat.D3
        val image = buildImage(landMask, outputFile = "test.png")

        val data = ImageTracer.loadImageData(image)
        val palette = ImageTracer.getPalette(image, options)
        val svg = ImageTracer.imagedataToSVG(data, options, palette)
        val geo = imagedataToGeoJson(data, options, palette, object: GeoJsonUtils.GeoCoder {
            override fun getLat(x: Double, y: Double): Float {
                val xx = Math.min(lat.shape[1].toDouble()-1, x).toInt()
                val yy = Math.min(lat.shape[2].toDouble()-1, y).toInt()
                return lat[0, xx, yy]
            }
            override fun getLon(x: Double, y: Double): Float {
                val xx = Math.min(lon.shape[1].toDouble()-1, x).toInt()
                val yy = Math.min(lon.shape[2].toDouble()-1, y).toInt()
                return lon[0, xx, yy]
            }
        })

        Files.write(Paths.get("test.svg"), svg.toByteArray())
        Files.write(Paths.get("testJson.json"), geo.toByteArray())

        dataset.close()
        println(" * Imported products in ${(System.currentTimeMillis() - start)/1000} seconds")
    }

    @ShellMethod("Convert NetCDF variable to ascii grid format ")
    fun toAsciiGrid(varName: String, @ShellOption(defaultValue = "XLAT") latVarName: String,
                    @ShellOption(defaultValue = "XLONG") lonVarName: String,
                    @ShellOption(defaultValue = "wrfout_d03_2017-07-13_12_00_00") inputFile: String ) {
        val dataset = NetcdfDataset.openDataset(inputFile)
        val lat = dataset.findVariable(latVarName).read() as ArrayFloat.D3
        val lon = dataset.findVariable(lonVarName).read() as ArrayFloat.D3
        val findVariable = dataset.findVariable(varName)
        val variable = findVariable.read() as ArrayFloat.D3

        File("$varName.asc").printWriter().use { out ->
            val shape = findVariable.shape
            val cellSize = (lon[0, 0, shape[1]-1] - lon[0, 0, 0])/(shape[1]-1)
            out.write("ncols        ${shape[1]}\n" +
                    "nrows        ${shape[2]}\n" +
                    "xllcorner    ${lon[0, 0, 0]}\n" +
                    "yllcorner    ${lat[0, 0, shape[2]-1]}\n" +
                    "cellsize     $cellSize\n" +
                    "NODATA_value ${Double.MAX_VALUE}\n")

            for(i in 0 until shape[1]) {
                for (j in 0 until shape[2]) {
                    out.write("${variable[10, i, j]} ")
                }
                out.write("\n")
            }
        }
        dataset.close()

        println(" * * completed")
    }

    private fun buildImage(aIn: ArrayFloat.D3, scaleFactor: Float = 255f, outputFile: String? = null, nan: Float = 0f): BufferedImage {
        val width = aIn.shape[2]
        val height = aIn.shape[1]
        val im = BufferedImage(width, height, BufferedImage.TYPE_4BYTE_ABGR)
        for(w in 0 until width -1)
            for(h in 0 until height -1) {
                if(aIn[0, h, w] != nan) {
                    val toInt = Math.min(255, (aIn[0, h, w] * scaleFactor).toInt())
                    val c = Color(toInt, toInt, toInt)
                    im.setRGB(w, h, c.rgb)
                } else
                    im.setRGB(w, h, Color(0, 0, 0, 1).rgb)
            }

        if(outputFile != null)
            ImageIO.write(im, "png", Paths.get("test.png").toFile());

        return im
    }

    private final val options = HashMap<String, Float>(15)

    // https://github.com/jankovicsandras/imagetracerjava
    init {
        options["numberofcolors"] = 32f
        options["scale"] = 4f
        options["ltres"] = 1f
        options["qtres"] = 1f
        options["pathomit"] = 8f
        options["colorsampling"] = 1f
        options["mincolorratio"] = 0.02f
        options["colorquantcycles"] = 3f
        options["simplifytolerance"] = 0f
        options["roundcoords"] = 3f
        options["lcpr"] = 0f
        options["qcpr"] = 0f
        options["qcpr"] = 1f
        options["viewbox"] = 0f
        options["blurradius"] = 0f
        options["blurdelta"] = 20f
    }

}