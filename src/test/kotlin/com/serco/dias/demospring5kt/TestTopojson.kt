package com.serco.dias.demospring5kt

import com.serco.dias.demospring5kt.controllers.ECMWFProcessor
import com.serco.dias.demospring5kt.controllers.TopoJsonCommands
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.shell.Input
import org.springframework.shell.Shell
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.springframework.shell.result.DefaultResultHandler



@RunWith(SpringJUnit4ClassRunner::class)
@SpringBootTest
class KotlinJunitTest {
    @Autowired lateinit var shell: Shell

  //  @Test
    fun topoNetcdf() {
        val commands = TopoJsonCommands()
        commands.addSralLand("uncompressed/S3A_SR_2_LAN____20170929T133328_20170929T142357_20171025T030343_3029_022_366______LN3_O_NT_002.SEN3",
                "C:\\Users\\fernando marino\\WebstormProjects\\earth\\public\\data\\weather\\current\\current-wind-surface-level-gfs-1.0.json")
    }

    @Test
    fun testEcmwf() {
        val file = "e:\\wrfout_d03_2017-07-13_12_00_00"
        val result = shell.evaluate( {"load-ecmwf"} )

        val resulthandler = DefaultResultHandler()
        resulthandler.handleResult(result)
    }

}