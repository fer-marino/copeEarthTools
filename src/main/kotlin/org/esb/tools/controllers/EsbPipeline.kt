package org.esb.tools.controllers

import org.apache.commons.io.FileUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

@ShellComponent
class EsbPipeline {
    @Autowired
    lateinit var sentinel3Commands: Sentinel3Commands
    @Autowired
    lateinit var sentinel1Commands: Sentinel1Commands
    @Autowired
    lateinit var sentinel5Commands: Sentinel5PCommands
    @Autowired
    lateinit var dataAccessCommands: DataAccessCommands

    private var workingDir = System.getProperty("user.dir")

    @ShellMethod("Shows the current working dir")
    fun workingDir() {
        println(" ** Working dir is: $workingDir")
    }

    @ShellMethod("Set the working dir")
    fun setWorkingDir(workingDir: String) {
        this.workingDir = workingDir
    }

    @ShellMethod("Process sentinel 5 product")
    fun sen5(year: Int, month: Int) {
        var startDate = LocalDateTime.of(year, month, 1, 0, 0)
        val stopDate = LocalDateTime.of(year, month + 1, 1, 0, 0)
        val filter = "( beginPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] " +
                "AND endPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] ) " +
                "AND (producttype:L2__NO2*)"

        dataAccessCommands.selectHub("s5p")
        dataAccessCommands.oSearch(filter, "IngestionDate desc", "$workingDir/s5p/no2/")

        while (startDate.isBefore(stopDate)) {
            // sample: S5P_OFFL_L2__NO2____20180709T000836_20180709T015006_03811_01_010002_20180715T020535.nc
            val id = startDate.format(DateTimeFormatter.BASIC_ISO_DATE)
            val mosaic = "$workingDir/s5p/no2-$id.tif"
            if (Files.exists(Paths.get(mosaic))) {
                startDate = startDate.plusDays(1)
                continue
            }

            val prev = startDate.minusDays(1)
            val next = startDate.plusDays(1)
            println(" ** Creating working dir for $id...")
            val target = Paths.get("$workingDir/s5p/no2/$id/")
            if (Files.exists(target))
                FileUtils.deleteDirectory(target.toFile())
            Files.createDirectories(target)
            Files.list(Paths.get("$workingDir/s5p/no2/")).filter {
                it.fileName.toString().contains("S5P_OFFL_L2__NO2____$id") ||
                        it.fileName.toString().contains("S5P_OFFL_L2__NO2____${prev.format(DateTimeFormatter.BASIC_ISO_DATE)}") ||
                        it.fileName.toString().contains("S5P_OFFL_L2__NO2____${next.format(DateTimeFormatter.BASIC_ISO_DATE)}")
            }.forEach { Files.copy(it, target.resolve(it.fileName), StandardCopyOption.REPLACE_EXISTING) }


            sentinel5Commands.mergeNO2("$workingDir/s5p/no2/$id/*", "", false, mosaic)

            startDate = startDate.plusDays(1)
        }

    }

    @ShellMethod("Process sentinel 3 LST product")
    fun sen3Lst(year: Int, month: Int, @ShellOption(defaultValue = "false") force: Boolean) {

        val startDate = LocalDateTime.of(year, month, 1, 0, 0)
        val stopDate = LocalDateTime.of(year, month + 1, 1, 0, 0)

        val filter = "( beginPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] " +
                "AND endPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] ) " +
                "AND (productType:SL_2_LST* AND timeliness:NTC)"

        dataAccessCommands.selectHub("dias")
        dataAccessCommands.ondaSearch(filter, "IngestionDate desc", "$workingDir/S3$year-$month")

        sentinel3Commands.lstMerge("$workingDir/S3$year-$month/S3*", "", force)

//        sentinel3Commands.postprocess("ascending-warp.tif", 1.5, 1)
//        sentinel3Commands.postprocess("descending-warp.tif", 1.5, 1)
    }

    @ShellMethod("Process sentinel 3 OGVI product")
    fun sen3lfr(start: String, stop: String) {
        val startDate = LocalDateTime.parse(start, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        val stopDate = LocalDateTime.parse(stop, DateTimeFormatter.ISO_LOCAL_DATE_TIME)

        val filter = " ( beginPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] " +
                "AND endPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] ) " +
                "AND footprint:\"Intersects(POLYGON((3.6403226412286407 48.35007718040529,1.2672757662286553 35.18417665926795,22.009463266228643 34.53511194265073,23.943057016228636 47.821672583009956,3.6403226412286407 48.35007718040529,3.6403226412286407 48.35007718040529)))\" " +
                "AND (platformname:Sentinel-3 AND producttype:OL_2_LFR___ AND timeliness:\"Near Real Time\")  "
        dataAccessCommands.selectHub("s3_preops")
        dataAccessCommands.oSearch(filter, "IngestionDate desc", "$workingDir/S3${startDate.year}-${startDate.month}")

        sentinel3Commands.lstMerge("$workingDir/S3${startDate.year}-${startDate.month}/S3*", "-co COMPRESS=JPEG")
    }

    @ShellMethod("Process sentinel 1 OCN product")
    fun sen1Ocn(start: String, stop: String) {
        val startDate = LocalDateTime.parse(start, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        val stopDate = LocalDateTime.parse(stop, DateTimeFormatter.ISO_LOCAL_DATE_TIME)

        val filter = " ( beginPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] " +
                "AND endPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] ) " +
                "AND footprint:\"Intersects(POLYGON((3.6403226412286407 48.35007718040529,1.2672757662286553 35.18417665926795,22.009463266228643 34.53511194265073,23.943057016228636 47.821672583009956,3.6403226412286407 48.35007718040529,3.6403226412286407 48.35007718040529)))\" " +
                "AND (platformname:Sentinel-1 AND producttype:OCN)  "
        dataAccessCommands.selectHub("scihub")
        dataAccessCommands.oSearch(filter, "IngestionDate desc", "$workingDir/S1${startDate.year}-${startDate.dayOfYear}")

        sentinel1Commands.ocnMergeGeotiff("$workingDir/S1${startDate.year}-${startDate.dayOfYear}/S1*", "-projwin 8 44 21.5 35")
    }
}