package com.serco.dias.demospring5kt

import com.serco.dias.demospring5kt.controllers.TopoJsonCommands
import org.junit.Test

class KotlinJunitTest {

    @Test
    fun topoNetcdf() {
        val commands = TopoJsonCommands()
        commands.addSralLand("uncompressed/S3A_SR_2_LAN____20170929T133328_20170929T142357_20171025T030343_3029_022_366______LN3_O_NT_002.SEN3",
                "C:\\Users\\fernando marino\\WebstormProjects\\earth\\public\\data\\weather\\current\\current-wind-surface-level-gfs-1.0.json")
    }

}