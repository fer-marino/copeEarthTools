package org.esb.tools.controllers

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import java.nio.file.Files
import java.nio.file.Paths
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

    @Autowired
    lateinit var s5pMeasurementTypes: Map<String, Sentinel5PCommands.Measurement>

    private var workingDir = System.getProperty("user.dir")

    @ShellMethod("The working directory")
    fun workingDir(@ShellOption(defaultValue = "") workingDir: String) {
        if (workingDir.isEmpty())
            println(" ** Working dir is: ${this.workingDir}")
        else
            this.workingDir = workingDir
    }

    @ShellMethod("Process sentinel 5 product")
    fun sen5(year: Int, month: Int) {
        dataAccessCommands.dataHub("s5p")

        s5pMeasurementTypes.keys.forEach { name ->
            val measurement = s5pMeasurementTypes[name]!!
            var startDate = LocalDateTime.of(year, month, 1, 0, 0)
            val stopDate = LocalDateTime.of(year, month + 1, 1, 0, 0)

            println(" -- Measurement type: ${measurement.description}")

            val filter = "( beginPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] " +
                    "AND endPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] ) " +
                    "AND (producttype:${measurement.prodType}*) AND processingmode:Offline"

            dataAccessCommands.oSearch(filter, "IngestionDate desc", "$workingDir/s5p/${measurement.prodType}/")

            while (startDate.isBefore(stopDate)) {
                // sample: S5P_OFFL_L2__NO2____20180709T000836_20180709T015006_03811_01_010002_20180715T020535.nc
                val id = startDate.format(DateTimeFormatter.BASIC_ISO_DATE)
                val mosaic = "$workingDir/s5p/$name-$id.tif"
                if (Files.exists(Paths.get(mosaic))) {
                    startDate = startDate.plusDays(1)
                    continue
                }

                sentinel5Commands.s5pMerge("$workingDir/s5p/${measurement.prodType}/S5P_OFFL_${measurement.prodType}_$id*", measurement, "", false, 50, mosaic)

                startDate = startDate.plusDays(1)
            }

        }

    }

    @ShellMethod("Process sentinel 3 LST product")
    fun sen3Lst(year: Int, month: Int, @ShellOption(defaultValue = "false") force: Boolean) {

        val startDate = LocalDateTime.of(year, month, 1, 0, 0)
        val stopDate = LocalDateTime.of(year, month + 1, 1, 0, 0)

        val filter = "( beginPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] " +
                "AND endPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] ) " +
                "AND (productType:SL_2_LST* AND timeliness:NTC)"

        dataAccessCommands.dataHub("dias")
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
        dataAccessCommands.dataHub("s3_preops")
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
        dataAccessCommands.dataHub("scihub")
        dataAccessCommands.oSearch(filter, "IngestionDate desc", "$workingDir/S1${startDate.year}-${startDate.dayOfYear}")

        sentinel1Commands.ocnMergeGeotiff("$workingDir/S1${startDate.year}-${startDate.dayOfYear}/S1*", "-projwin 8 44 21.5 35")
    }
}