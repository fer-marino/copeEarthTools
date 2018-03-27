package org.esb.tools.controllers

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import java.time.DayOfWeek
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.IsoFields
import java.time.temporal.TemporalAdjusters


@ShellComponent
class EsbPipeline {
    @Autowired lateinit var sentinel3Commands: Sentinel3Commands
    @Autowired lateinit var sentinel1Commands: Sentinel1Commands
    @Autowired lateinit var dhusCommands: DhusCommands

    @ShellMethod("Process sentinel 3 LST product")
    fun sen3Lst(year: Int, woy: Long)  {
        val startDate = LocalDate.ofYearDay(year, 50).with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, woy)
                .with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).atStartOfDay().atZone(ZoneId.of("UTC")).toInstant()
        val stopDate = startDate.plus(Duration.ofDays(3))

        DateTimeFormatter.ISO_INSTANT.format(startDate)
        val filter = " ( beginPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate)} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate)}] " +
                "AND endPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate)} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate)}] ) " +
                "AND footprint:\"Intersects(POLYGON((3.6403226412286407 48.35007718040529,1.2672757662286553 35.18417665926795,22.009463266228643 34.53511194265073,23.943057016228636 47.821672583009956,3.6403226412286407 48.35007718040529,3.6403226412286407 48.35007718040529)))\" " +
                "AND (platformname:Sentinel-1 AND producttype:OCN)  "
        dhusCommands.searchOSearch("test", "test",  filter, "IngestionDate desc", "$year-$woy")

        sentinel3Commands.lstMerge("$year-$woy/*", "-projwin 9 44 21.5 35")
    }

    @ShellMethod("Process sentinel 1 OCN product")
    fun sen1Ocn(year: Int, woy: Long) {
        val startDate = LocalDate.ofYearDay(year, 50).with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, woy)
                .with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).atStartOfDay().atZone(ZoneId.of("UTC")).toInstant()
        val stopDate = startDate.plus(Duration.ofDays(3))

        DateTimeFormatter.ISO_INSTANT.format(startDate)
        val filter = " ( beginPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate)} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate)}] " +
                "AND endPosition:[${DateTimeFormatter.ISO_INSTANT.format(startDate)} TO ${DateTimeFormatter.ISO_INSTANT.format(stopDate)}] ) " +
                "AND footprint:\"Intersects(POLYGON((3.6403226412286407 48.35007718040529,1.2672757662286553 35.18417665926795,22.009463266228643 34.53511194265073,23.943057016228636 47.821672583009956,3.6403226412286407 48.35007718040529,3.6403226412286407 48.35007718040529)))\" " +
                "AND (platformname:Sentinel-1 AND producttype:OCN)  "
        dhusCommands.searchOSearch("test", "test",  filter, "IngestionDate desc", "$year-$woy")

        sentinel1Commands.ocnMergeGeotiff("$year-$woy/*", "-projwin 9 44 21.5 35")
    }
}