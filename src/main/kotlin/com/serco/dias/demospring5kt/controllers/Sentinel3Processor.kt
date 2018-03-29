package com.serco.dias.demospring5kt.controllers

import imageTracer.GeoJsonUtils
import imageTracer.ImageTracer
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ucar.ma2.ArrayByte
import ucar.ma2.ArrayDouble
import ucar.ma2.ArrayFloat
import ucar.ma2.ArrayInt
import ucar.nc2.dataset.NetcdfDataset
import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import javax.imageio.ImageIO

@ShellComponent("Sentinel 3 Processor")
class Sentinel3Processor {

    @ShellMethod("Load biome")
    fun loadBIOME(@ShellOption(defaultValue = "C:\\Users\\Salvatore Tarchini\\Desktop\\Data-Mining\\S3A_SL_2_LST____20171024T084607_20171024T084907_20171024T105110_0179_023_335_2340_SVL_O_NR_002.SEN3") file: String) {
        val start = System.currentTimeMillis()
        val directory = file.substringBeforeLast(File.separator)
//        println(directory)

        val lst_ancillary = NetcdfDataset.openDataset(file + File.separator + "LST_ancillary_ds.nc")
        val coordinates = NetcdfDataset.openDataset(file + File.separator + "geodetic_in.nc")
        val instrumentData = NetcdfDataset.openDataset(file + File.separator + "indices_in.nc")
//        val flags = NetcdfDataset.openDataset(file+File.separator+"LST_ancillary_ds.nc")

//        //val landMask = dataset.findVariable("LANDMASK").read() as ArrayFloat.D2
        val lat = coordinates.findVariable("latitude_in").read() as ArrayDouble.D2
        val lon = coordinates.findVariable("longitude_in").read() as ArrayDouble.D2
        val biome = lst_ancillary.findVariable("biome").read() as ArrayByte.D2
        val validation = lst_ancillary.findVariable("validation").read() as ArrayByte.D1

        val shape = instrumentData.findVariable("detector_in").getShape()
//        for (i in 0..(shape[0]-1)) {
//            print(i)
//            print("-")
//            println(validation.get(i).toInt() and 2)
//        }
//        exitProcess(1)

        val image = buildImage(biome, 29.toFloat(), outputFile = directory + File.separator + "biome.png")
        val data = ImageTracer.loadImageData(image)
        // data è un vettore data.heigth*data.width*4 poichè i canali sono 4 (red, green, blue, alpha)

        val palette = ImageTracer.getPalette(image, options)
        val svg = ImageTracer.imagedataToSVG(data, options, palette)
        Files.write(Paths.get(directory + File.separator + "biome.svg"), svg.toByteArray())

        val geo = ImageTracer.imagedataToGeoJson(data, options, palette, object : GeoJsonUtils.GeoCoder {
            override fun getLat(x: Double, y: Double): Float {
                val xx = Math.min(lat.shape[0].toDouble() - 1, x).toInt()
                val yy = Math.min(lat.shape[1].toDouble() - 1, y).toInt()
                return lat[xx, yy].toFloat()
            }

            override fun getLon(x: Double, y: Double): Float {
                val xx = Math.min(lon.shape[0].toDouble() - 1, x).toInt()
                val yy = Math.min(lon.shape[1].toDouble() - 1, y).toInt()
                return lon[xx, yy].toFloat()
            }
        })

        Files.write(Paths.get(directory + File.separator + "biome.json"), geo.toByteArray())

        for (mask in 0..29) {
            val biomeClass = biomeMask(biome, mask.toDouble(), shape)
            val image = buildImage(biomeClass, 255.toFloat(), outputFile = directory + File.separator + "biome_" + mask + ".png")
            val data = ImageTracer.loadImageData(image)
            val palette = ImageTracer.getPalette(image, options)
            val svg = ImageTracer.imagedataToSVG(data, options, palette)
            Files.write(Paths.get(directory + File.separator + "biome_" + mask + ".svg"), svg.toByteArray())

            val geo = ImageTracer.imagedataToGeoJson(data, options, palette, object : GeoJsonUtils.GeoCoder {
                override fun getLat(x: Double, y: Double): Float {
                    val xx = Math.min(lat.shape[0].toDouble() - 1, x).toInt()
                    val yy = Math.min(lat.shape[1].toDouble() - 1, y).toInt()
                    return lat[xx, yy].toFloat()
                }

                override fun getLon(x: Double, y: Double): Float {
                    val xx = Math.min(lon.shape[0].toDouble() - 1, x).toInt()
                    val yy = Math.min(lon.shape[1].toDouble() - 1, y).toInt()
                    return lon[xx, yy].toFloat()
                }
            })

            Files.write(Paths.get(directory + File.separator + "biome_" + mask + ".json"), geo.toByteArray())
        }

        coordinates.close()
        lst_ancillary.close()

        println(" * Imported products in ${(System.currentTimeMillis() - start) / 1000} seconds")
    }

    @ShellMethod("Load OGVI")
    fun loadOGVI(@ShellOption(defaultValue = "C:\\Users\\Salvatore Tarchini\\Desktop\\OGVI\\S3A_OL_2_LFR____20180217T083838_20180217T084138_20180218T132021_0179_028_064_2340_LN1_O_NT_002.SEN3") file: String) {
        val start = System.currentTimeMillis()
        val directory = file.substringBeforeLast(File.separator)
//        println(directory)

        val dataset_ogvi = NetcdfDataset.openDataset(file + File.separator + "ogvi.nc")
        val coordinates = NetcdfDataset.openDataset(file + File.separator + "geo_coordinates.nc")
        val instrumentData = NetcdfDataset.openDataset(file + File.separator + "instrument_data.nc")

        //val landMask = dataset.findVariable("LANDMASK").read() as ArrayFloat.D2
        val lat = coordinates.findVariable("latitude").read() as ArrayDouble.D2
        val lon = coordinates.findVariable("longitude").read() as ArrayDouble.D2
        val ogvi = dataset_ogvi.findVariable("OGVI").read() as ArrayFloat.D2
        val shape = instrumentData.findVariable("detector_index").getShape()

//        println(shape[0])
//        println(shape[1])

//        val latF = ArrayFloat.D2(shape[0],shape[1])
//        val lonF = ArrayFloat.D2(shape[0],shape[1])
//        val ima = latF.index
//        for (i in 0..(shape[0]-1)) {
//            for (j in 0..(shape[0]-1)){
//                latF.set(ima.set(i,j),lat[i,j].toFloat())
//                lonF.set(ima.set(i,j),lon[i,j].toFloat())
//            }
//        }

//        val image = buildImage(ogvi, outputFile = directory+File.separator+"ogvi.png")
        val image = buildImage(ogvi)

        val data = ImageTracer.loadImageData(image)
        val palette = ImageTracer.getPalette(image, options)
//        var vector = ImageTracer.imagedataToTracedata(data, options, palette)
        val svg = ImageTracer.imagedataToSVG(data, options, palette)
        Files.write(Paths.get(directory + File.separator + "ogvi.svg"), svg.toByteArray())

//        println(data.height)
//        println(data.width)
        val geo = ImageTracer.imagedataToGeoJson(data, options, palette, object : GeoJsonUtils.GeoCoder {
            override fun getLat(x: Double, y: Double): Float {
//                print(shape[0])
//                print(lat)
//                println(lat.shape[0])
//                println(x)
//                println(y)
//                println(Math.min(lat.shape[0].toDouble() - 1, x))
                //                System.out.print(lat.shape[0].toDouble())
                val xx = Math.min(lat.shape[0].toDouble() - 1, x).toInt()
                val yy = Math.min(lat.shape[1].toDouble() - 1, y).toInt()
//                val xx = Math.min(lat.shape[0] - 1, x).toInt()
//                val yy = Math.min(lat.shape[1] - 1, y).toInt()
                //return lat[0, xx, yy]
//                return latF[xx, yy]
//                println(xx)
                return lat[xx, yy].toFloat()
            }

            override fun getLon(x: Double, y: Double): Float {
                val xx = Math.min(lon.shape[0].toDouble() - 1, x).toInt()
                val yy = Math.min(lon.shape[1].toDouble() - 1, y).toInt()
                //return lon[0, xx, yy]
//                return lonF[xx, yy]
                return lon[xx, yy].toFloat()
            }
        })

        Files.write(Paths.get(directory + File.separator + "ogviJson.json"), geo.toByteArray())

        coordinates.close()
        dataset_ogvi.close()

        println(" * Imported products in ${(System.currentTimeMillis() - start) / 1000} seconds")
    }

    private fun buildImage(aIn: ArrayByte.D2, scaleFactor: Float, outputFile: String? = null, nan: Float = 0f): BufferedImage {
        val width = aIn.shape[1]
        val height = aIn.shape[0]
        val im = BufferedImage(width, height, BufferedImage.TYPE_4BYTE_ABGR)
        for (w in 0 until width - 1)
            for (h in 0 until height - 1) {
//                if(aIn[h, w] != nan) {
                    val toInt = Math.min(255, (aIn[h, w] * scaleFactor).toInt())
//                    println(aIn[h, w])
//                    println(aIn[h, w].toInt())
//                    val toInt = aIn[h, w].toInt()
                    val c = Color(toInt, toInt, toInt)
                    im.setRGB(w, h, c.rgb)
//                } else
//                    im.setRGB(w, h, Color(0, 0, 0, 1).rgb)
            }

        if (outputFile != null)
            ImageIO.write(im, "png", Paths.get(outputFile).toFile())

        return im
    }

    private fun buildImage(aIn: ArrayInt.D2, scaleFactor: Float = 255f, outputFile: String? = null, nan: Float = 0f): BufferedImage {
        val width = aIn.shape[1]
        val height = aIn.shape[0]
        val im = BufferedImage(width, height, BufferedImage.TYPE_4BYTE_ABGR)
        for (w in 0 until width - 1)
            for (h in 0 until height - 1) {
//                if(aIn[h, w] != nan) {
                val toInt = Math.min(255, (aIn[h, w] * scaleFactor).toInt())
//                    println(aIn[h, w])
//                    println(aIn[h, w].toInt())
//                    val toInt = aIn[h, w].toInt()
                val c = Color(toInt, toInt, toInt)
                im.setRGB(w, h, c.rgb)
//                } else
//                    im.setRGB(w, h, Color(0, 0, 0, 1).rgb)
            }

        if (outputFile != null)
            ImageIO.write(im, "png", Paths.get(outputFile).toFile())

        return im
    }

    private fun buildImage(aIn: ArrayFloat.D2, scaleFactor: Float = 255f, outputFile: String? = null, nan: Float = 0f): BufferedImage {
        val width = aIn.shape[1]
        val height = aIn.shape[0]
        val im = BufferedImage(width, height, BufferedImage.TYPE_4BYTE_ABGR)
        for (w in 0 until width - 1)
            for (h in 0 until height - 1) {
                if (aIn[h, w] != nan) {
                    val toInt = Math.min(255, (aIn[h, w] * scaleFactor).toInt())
                    val c = Color(toInt, toInt, toInt)
//                    println(toInt)
                    im.setRGB(w, h, c.rgb)
                } else
                    im.setRGB(w, h, Color(0, 0, 0, 1).rgb)
            }

        if (outputFile != null)
            ImageIO.write(im, "png", Paths.get(outputFile).toFile())

        return im
    }

    private fun biomeMask(aIn: ArrayByte.D2, maskValue: Double, shp: IntArray): ArrayByte.D2 {
        val biomeMaskVar = ArrayByte.D2(shp[0], shp[1])
        val ima = biomeMaskVar.index

        for (i in 0..(shp[0] - 1)) {
            for (j in 0..(shp[0] - 1)) {
                if (aIn.get(i, j).toDouble().equals(maskValue)) {
                    biomeMaskVar.set(ima.set(i, j), 1)
                } else {
                    biomeMaskVar.set(ima.set(i, j), 0)
                }
            }
        }
        return biomeMaskVar
    }

    private final val options = HashMap<String, Float>(15)

    // https://github.com/jankovicsandras/imagetracerjava
    init {
//        options["numberofcolors"] = 10f
//        options["scale"] = 1f
//        options["ltres"] = 10f
//        options["qtres"] = 10f
//        options["pathomit"] = 32f
//        options["colorsampling"] = 1f
//        options["mincolorratio"] = 0.02f
//        options["colorquantcycles"] = 3f
//        options["simplifytolerance"] = 0f
//        options["roundcoords"] = 3f
//        options["lcpr"] = 0f
//        options["qcpr"] = 0f
//        options["viewbox"] = 0f
//        options["blurradius"] = 5f
//        options["blurdelta"] = 50f
//        options["desc"] = 1f
        options["numberofcolors"] = 10f
        options["scale"] = 1f
        options["ltres"] = 1f
        options["qtres"] = 1f
        options["pathomit"] = 16f
        options["colorsampling"] = 1f
        options["mincolorratio"] = 0.02f
        options["colorquantcycles"] = 3f
        options["simplifytolerance"] = 0f
        options["roundcoords"] = 3f
        options["lcpr"] = 0f
        options["qcpr"] = 0f
        options["viewbox"] = 0f
        options["blurradius"] = 5f
        options["blurdelta"] = 50f
        options["desc"] = 1f
    }
}