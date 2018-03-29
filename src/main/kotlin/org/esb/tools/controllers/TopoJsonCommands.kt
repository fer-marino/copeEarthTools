package org.esb.tools.controllers

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ucar.ma2.ArrayDouble
import ucar.nc2.dataset.NetcdfDataset
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

@ShellComponent
class TopoJsonCommands {
    private val mapper = ObjectMapper()

    @ShellMethod("Load all SRAL ")
    fun loadAllSral(
        @ShellOption(defaultValue = "uncompressed") folder: String,
        @ShellOption(defaultValue = ".*") filter: Regex,
        @ShellOption(defaultValue = "test.json") jsonFile: String
    ) {
        val path = Paths.get(folder)

        val start = System.currentTimeMillis()
        var c = 0
        Files.walk(path.toAbsolutePath()).filter { Files.isDirectory(it) && it.toString().contains("SR_2_LAN") && it.toString().matches(filter) }
                .forEach { addSralLand(it.toAbsolutePath().toString(), jsonFile); c++ }
        println(" * Imported $c products in ${(System.currentTimeMillis() - start) / 1000} seconds")
    }

    @ShellMethod("Add SRAL point to Topojson file")
    fun addSralLand(product: String, jsonFile: String) {
        print(" * Importing $product...")
        val json: Array<Any> = mapper.readValue(File(jsonFile), Array<Any>::class.java)
        val v_wind = json.first { ((it as Map<*, *>)["header"] as Map<*, *>)["parameterNumberName"] == "V-component_of_wind" } as Map<*, *>
        val u_wind = json.first { ((it as Map<*, *>)["header"] as Map<*, *>)["parameterNumberName"] == "U-component_of_wind" } as Map<*, *>

        NetcdfDataset.openDataset("$product/standard_measurement.nc").use {
            // winds
            val wind_u_01: ArrayDouble.D1 = it.findVariable("wind_speed_mod_u_01").read() as ArrayDouble.D1
            val wind_v_01: ArrayDouble.D1 = it.findVariable("wind_speed_mod_v_01").read() as ArrayDouble.D1
            val lat_01: ArrayDouble.D1 = it.findVariable("lat_01").read() as ArrayDouble.D1
            val lon_01: ArrayDouble.D1 = it.findVariable("lon_01").read() as ArrayDouble.D1
            val size = it.findVariable("wind_speed_mod_u_01").size

            for (t in 0 until size - 1) {
                val lat_i: Int = (((lat_01[t.toInt()] + 90) * Math.sqrt((65160 / 4).toDouble())) / 180).toInt() - 1
                val lon_i: Int = (((lon_01[t.toInt()]) * Math.sqrt((65160 / 4).toDouble()) * 4) / 360).toInt()

                if (v_wind["data"] is ArrayList<*> && u_wind["data"] is ArrayList<*>) {
                    (v_wind["data"] as ArrayList<Double>)[lat_i * lon_i] = wind_v_01[t.toInt()]
                    (u_wind["data"] as ArrayList<Double>)[lat_i * lon_i] = wind_u_01[t.toInt()]
                }
            }

            it.close()
        }

        mapper.writeValue(File(jsonFile), arrayOf(v_wind, u_wind))
        println(" done")
    }
}
