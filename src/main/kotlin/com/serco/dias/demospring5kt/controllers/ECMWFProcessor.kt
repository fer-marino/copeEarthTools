package com.serco.dias.demospring5kt.controllers

import imageTracer.ImageTracer
import imageTracer.Quantize
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ucar.ma2.ArrayFloat
import ucar.nc2.dataset.NetcdfDataset
import java.awt.Color
import java.awt.image.BufferedImage
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.HashMap
import javax.imageio.ImageIO


@ShellComponent
class ECMWFProcessor {

    @ShellMethod("Load all Wind ")
    fun loadECMWF(@ShellOption(defaultValue = "wrfout_d03_2017-07-13_12_00_00") file: String,
                    @ShellOption(defaultValue = "test.json") jsonFile: String) {
        val start = System.currentTimeMillis()

        val dataset = NetcdfDataset.openDataset(file)
        val landMask = dataset.findVariable("LANDMASK").read() as ArrayFloat.D3
//        val windU = dataset.findVariable("U10").read() as ArrayFloat.D3
//        var windV = dataset.findVariable("V10").read() as ArrayFloat.D3
        val image = buildImage(landMask)


        val data = ImageTracer.loadImageData(image)
        val palette = ImageTracer.getPalette(image, options)
        var vector = ImageTracer.imagedataToTracedata(data, options, palette)
        val svg = ImageTracer.imagedataToSVG(data, options, palette)

        Files.write(Paths.get("test.svg"), svg.toByteArray())

        dataset.close()
        println(" * Imported products in ${(System.currentTimeMillis() - start)/1000} seconds")

    }

    private fun buildImage(aIn: ArrayFloat.D3, scaleFactor: Float = 255f, outputFile: String? = null): BufferedImage {
        val width = aIn.shape[2]
        val height = aIn.shape[1]
        val im = BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR)
        for(w in 0 until width -1)
            for(h in 0 until height-1) {
                val toInt = Math.min(255, (aIn[0, height-h-1, w] * scaleFactor).toInt())
                val c = Color(toInt, toInt, toInt)
                im.setRGB(w, h, c.rgb)
            }

        if(outputFile != null)
            ImageIO.write(im, "png", Paths.get("test.png").toFile());

        return im
    }

    final val options = HashMap<String, Float>(15)

    init {
        options["numberofcolors"] = 128f
        options["scale"] = 4f
        options["ltres"] = 1f
        options["qtres"] = 1f
        options["pathomit"] = 8f
        options["colorsampling"] = 1f
        options["mincolorratio"] = 0.02f
        options["colorquantcycles"] = 3f
        options["simplifytolerance"] = 0f
        options["roundcoords"] = 1f
        options["lcpr"] = 0f
        options["qcpr"] = 0f
        options["qcpr"] = 1f
        options["viewbox"] = 0f
        options["blurradius"] = 0f
        options["blurdelta"] = 20f
    }

}