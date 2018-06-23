package org.esb.tools.controllers

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
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
    lateinit var dhusCommands: DhusCommands

    @ShellMethod("Process sentinel 3 LST product")
    fun sen3Lst(year: Int, month: Int, @ShellOption(defaultValue = "false") force: Boolean) {

        val startDate = LocalDateTime.of(year, month, 1, 0, 0)
        val stopDate = LocalDateTime.of(year, month + 1, 1, 0, 0)

        val filter = " ( beginPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] " +
                "AND endPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] ) " +
                "AND (platformname:Sentinel-3 AND producttype:SL_2_LST___ AND timeliness:\"Non Time Critical\")  "

        dhusCommands.selectHub("s3_preops")
        dhusCommands.searchOSearch(filter, "IngestionDate desc", "S3$year-$month")

        sentinel3Commands.lstMerge("S3$year-$month/S3*", "", force)

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
        dhusCommands.selectHub("s3_preops")
        dhusCommands.searchOSearch(filter, "IngestionDate desc", "S3${startDate.year}-${startDate.month}")

        sentinel3Commands.lstMerge("S3${startDate.year}-${startDate.month}/S3*", "-co COMPRESS=JPEG")
    }

    @ShellMethod("Process sentinel 1 OCN product")
    fun sen1Ocn(start: String, stop: String) {
        val startDate = LocalDateTime.parse(start, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        val stopDate = LocalDateTime.parse(stop, DateTimeFormatter.ISO_LOCAL_DATE_TIME)

        val filter = " ( beginPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] " +
                "AND endPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate.toInstant(ZoneOffset.UTC))} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate.toInstant(ZoneOffset.UTC))}] ) " +
                "AND footprint:\"Intersects(POLYGON((3.6403226412286407 48.35007718040529,1.2672757662286553 35.18417665926795,22.009463266228643 34.53511194265073,23.943057016228636 47.821672583009956,3.6403226412286407 48.35007718040529,3.6403226412286407 48.35007718040529)))\" " +
                "AND (platformname:Sentinel-1 AND producttype:OCN)  "
        dhusCommands.selectHub("scihub")
        dhusCommands.searchOSearch(filter, "IngestionDate desc", "S1${startDate.year}-${startDate.dayOfYear}")

        sentinel1Commands.ocnMergeGeotiff("S1${startDate.year}-${startDate.dayOfYear}/S1*", "-projwin 8 44 21.5 35")
    }
}