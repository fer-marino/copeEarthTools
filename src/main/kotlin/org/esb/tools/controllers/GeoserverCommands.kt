package org.esb.tools.controllers

import org.esb.tools.configuration.GeoserverConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.context.annotation.Bean
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.client.RestOperations
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*


@ShellComponent
class GeoserverCommands {

    @Autowired
    private lateinit var restTemplate: RestOperations
    @Autowired
    private lateinit var geoserverConfiguration: GeoserverConfiguration

    @Bean
    fun rest(builder: RestTemplateBuilder, geoserverConfiguration: GeoserverConfiguration): RestOperations = builder.basicAuthorization(geoserverConfiguration.username, geoserverConfiguration.password).build()

    @ShellMethod("Show store structure")
    fun getStoreIndex(coverageStore: String) {
        // GET "http://localhost:8083/geoserver/rest/workspaces/geosolutions/coveragestores/polyphemus/coverages/NO2/index.xml"
        try {
            val ris = restTemplate.getForObject("${geoserverConfiguration.baseUrl}/rest/workspaces/${geoserverConfiguration.workspace}/coveragestores" +
                    "/$coverageStore/coverages/$coverageStore/index.json", Map::class.java)

            println(ris)
        } catch (e: HttpServerErrorException) {
            println(" * Error during granule list: ${e.message} \n\t ${e.responseBodyAsString}")
        }
    }

    @ShellMethod("List granules for a given datastore")
    fun listGranules(coverageStore: String, @ShellOption(defaultValue = "100") limit: Int) {
        try {
            val ris = restTemplate.getForObject("${geoserverConfiguration.baseUrl}/rest/workspaces/${geoserverConfiguration.workspace}/coveragestores" +
                    "/$coverageStore/coverages/$coverageStore/index/granules.json?limit=$limit", FeatureCollection::class.java)

            ris?.features?.forEachIndexed { i, it ->
                println(" * [$i] $it")
            }
        } catch (e: HttpServerErrorException) {
            println(" * Error during granule list: ${e.message} \n\t ${e.responseBodyAsString}")
        }
    }

    @ShellMethod("Add a granule pattern to an existing coverage store. The granulePattern shall be located in the geoserver filesystem")
    fun addGranule(@ShellOption("The granule path to upload. Can be an ant pattern") granulePattern: String, coverageStore: String) {
        val matches = PathMatchingResourcePatternResolver().getResources("file:$granulePattern")
        matches.map { it.file.toPath().toAbsolutePath() }.forEachIndexed { i, g ->
            try {
                println(" * [${i + 1}/${matches.size}] Adding $g ...")

                val path = Paths.get("/data/geoserver/$coverageStore")
                if (!Files.exists(path)) {
                    println(" * ERROR: coverage folder store folder does not exist: $path")
                }

                val destination = path.resolve(g.fileName)
                if (!Files.exists(destination)) {
                    Files.move(g, destination)
                    val ris = restTemplate.postForObject("${geoserverConfiguration.baseUrl}/rest/workspaces/" +
                            "${geoserverConfiguration.workspace}/coveragestores/$coverageStore/external.imagemosaic",
                            destination.toString(), FeatureCollection::class.java, mapOf("Content-type" to "text"))
                    println(ris)
                }
            } catch (e: HttpServerErrorException) {
                println(" * Error during granule add: ${e.message} \n\t ${e.responseBodyAsString}")

            }
        }
        println(" * All granules added")
    }

    @ShellMethod("Delete a granule from a coverage store")
    fun removeGranule(coverageStore: String, @ShellOption("The granule id to remove. Use *list-granules* to look for IDs") granule: String) {
        val ris = restTemplate.getForObject("${geoserverConfiguration.baseUrl}/rest/workspaces/" +
                "${geoserverConfiguration.workspace}/coveragestores/$coverageStore/" +
                "coverages/$coverageStore/index/granule/$granule.json", FeatureCollection::class.java)
        println(" * Granule $granule removed from $coverageStore")
        println(ris)
    }

    @ShellMethod("List coverage stores")
    fun listCoverageStores() {
        // /workspaces/<ws>/coveragestores[.<format>]

        val ris = restTemplate.getForObject("${geoserverConfiguration.baseUrl}/rest/workspaces/${geoserverConfiguration.workspace}/coveragestores.json", Map::class.java)

        println(ris)
    }
}

data class FeatureCollection(var bbox: DoubleArray, var crs: Any, var features: List<Feature>) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as FeatureCollection

        if (!Arrays.equals(bbox, other.bbox)) return false
        if (crs != other.crs) return false
        if (features != other.features) return false

        return true
    }

    override fun hashCode(): Int {
        var result = Arrays.hashCode(bbox)
        result = 31 * result + crs.hashCode()
        result = 31 * result + features.hashCode()
        return result
    }
}

data class Feature(var id: String,  var properties: Map<String, String>, var geometry: Any)