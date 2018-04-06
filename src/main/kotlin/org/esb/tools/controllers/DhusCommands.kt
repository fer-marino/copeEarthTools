package org.esb.tools.controllers

import com.fasterxml.jackson.annotation.JsonAlias
import org.apache.commons.io.FileUtils
import org.apache.commons.io.input.CountingInputStream
import org.esb.tools.Utils
import org.jline.utils.AttributedString
import org.jline.utils.AttributedStyle
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.context.annotation.Bean
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.shell.jline.PromptProvider
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import org.springframework.util.Base64Utils
import org.springframework.web.client.HttpStatusCodeException
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.net.HttpURLConnection
import java.net.URL
import java.nio.file.Files
import java.nio.file.Paths
import java.util.zip.ZipInputStream

@ShellComponent
class DhusCommands {
    @Autowired
    lateinit var restTemplateBuilder: RestTemplateBuilder

    @Bean
    fun myPromptProvider(): PromptProvider = PromptProvider {
        AttributedString("shell:>",
                AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW))
    }

    @Value("\${dhus.url:https://scihub.copernicus.eu/s3}")
    private lateinit var dhusUrl: String

    @ShellMethod("Query Copernicus open hub on Open Sarch API and download all the results")
    fun searchOSearch(
        @ShellOption(defaultValue = "test") username: String,
        @ShellOption(defaultValue = "test") password: String,
        @ShellOption(defaultValue = "producttype:SL_2_LST___ AND timeliness:\"Near Real Time\"") filter: String,
        @ShellOption(defaultValue = "IngestionDate desc") orderBy: String,
        @ShellOption(defaultValue = "download") destination: String
    ): List<String> {
        var skip = 0
        val pageSize = 100
        val out = mutableListOf<String>()

        val tpl = restTemplateBuilder.basicAuthorization(username, password).build()
        try {
            do {
                val start = System.currentTimeMillis()
                val headers = HttpHeaders()
                headers.accept = listOf(MediaType.APPLICATION_JSON)
                val entity = HttpEntity("parameters", headers)
                val query = "$dhusUrl/search?start=$skip&rows=$pageSize&orderby=$orderBy&format=json&q=$filter"
                println(" * Running query $query")
                val response = tpl.exchange(query, HttpMethod.GET, entity, Map::class.java)
                val feed = response.body["feed"] as Map<String, Any>

                println(" * Returned ${feed["opensearch:totalResults"]} products in ${System.currentTimeMillis() - start}msec")
                var skipCount = 0
                if (feed.containsKey("entry")) {
                    for (entry in feed["entry"] as List<Map<String, Any>>) {
                        if (!(Files.exists(Paths.get(destination)) && Files.list(Paths.get(destination)).map { it.toString() }.anyMatch { it.contains(entry["title"].toString()) })) {
                            if (skipCount != 0) {
                                println(" * Skipped $skipCount products as already downloaded")
                                skipCount = 0
                            }
                            downloadProduct(entry["id"].toString(), entry["title"].toString(), username, password, destination)
                            unzip(entry["title"].toString(), destination, destination)
                        } else
                            skipCount++

                        out.add(entry["title"].toString())
                    }

                    if (skipCount != 0) println(" * Skipped $skipCount products as already downloaded")

                    skip += pageSize
                } else
                    break
            } while (true)
        } catch (e: HttpStatusCodeException) {
            println("HTTP Error (${e.statusCode}): ${e.message}")
            println(e.responseBodyAsString)
        }

        return out
    }

    @ShellMethod("Query Copernicus open hub on ODATA API and download all the results")
    fun searchOdata(
        @ShellOption(defaultValue = "test") username: String,
        @ShellOption(defaultValue = "test") password: String,
        @ShellOption(defaultValue = "substringof('SR_2_LAN', Name)") filter: String,
        @ShellOption(defaultValue = "IngestionDate desc") orderBy: String,
        @ShellOption(defaultValue = "download") destination: String
    ): List<String> {
        var skip = 0
        val pageSize = 100
        val out = mutableListOf<String>()

        val tpl = restTemplateBuilder.basicAuthorization(username, password).build()

        try {
            do {
                val start = System.currentTimeMillis()
                val headers = HttpHeaders()
                headers.accept = listOf(MediaType.APPLICATION_JSON)
                val entity = HttpEntity("parameters", headers)
                val query = "$dhusUrl/odata/v1/Products?\$filter=$filter" +
                        "&\$orderby=$orderBy&\$skip=$skip&\$top=$pageSize"
                println(" * Running query $query")
                val response = tpl.exchange(query, HttpMethod.GET, entity, ODataRoot::class.java)

                println(" * Total products ${response.body.d.results.size} in ${System.currentTimeMillis() - start}msec")
                var skipCount = 0
                for (entry in response.body.d.results) {
                    if (!Files.exists(Paths.get(destination, entry.name + ".SEN3"))) {
                        if (skipCount != 0) {
                            println(" * Skipped $skipCount products as already downloaded")
                            skipCount = 0
                        }
                        downloadProduct(entry.id, entry.name, username, password, destination)
                        unzip(entry.name, destination, destination)
                    } else
                        skipCount++
                    out.add(entry.name)
                }

                if (skipCount != 0) println(" * Skipped $skipCount products as already downloaded")

                skip += pageSize
            } while (response.body.d.results.size == pageSize)
        } catch (e: HttpStatusCodeException) {
            println("HTTP Error (${e.statusCode}): ${e.message}")
            println(e.responseBodyAsString)
        }

        return out
    }

    @ShellMethod("Download a product component having id and product name")
    fun downloadProduct(id: String, productName: String, username: String = "test", password: String = "test", destination: String = "download") {
        val uc: HttpURLConnection = URL("$dhusUrl/odata/v1/Products('$id')/\$value").openConnection() as HttpURLConnection
        val basicAuth = "Basic " + Base64Utils.encodeToString("$username:$password".toByteArray())
        uc.setRequestProperty("Authorization", basicAuth)
        val stream = CountingInputStream(uc.inputStream)

        var complete = false
        val start = System.currentTimeMillis()
        Thread {
            FileUtils.copyInputStreamToFile(stream, Paths.get(destination, "$productName.zip").toFile())
            complete = true
        }.start()

        var prev: Long = 0
        while (!complete) {
            Thread.sleep(1000)
            print("\r * Downloading $productName of size ${Utils.readableFileSize(uc.contentLengthLong)}: " +
                    "${Math.round(stream.byteCount * 1000f / uc.contentLengthLong) / 10f}% (${Utils.readableFileSize(stream.byteCount - prev)}/sec)")
            prev = stream.byteCount
        }
        println("\r * Download of $productName of size ${Utils.readableFileSize(prev)} finished in ${(System.currentTimeMillis() - start) / 1000} seconds")
    }

    @ShellMethod("Unzip product")
    fun unzip(product: String, @ShellOption(defaultValue = "uncompressed") destination: String = "uncompressed", @ShellOption(defaultValue = "download") source: String, @ShellOption(defaultValue = "false") notDelete: Boolean = false) {
        print(" * Unzipping $product...")
        val buffer = ByteArray(1024)
        val zis = ZipInputStream(FileInputStream("$source/$product.zip"))
        var zipEntry = zis.nextEntry
        while (zipEntry != null) {
            if (zipEntry.isDirectory && !Files.exists(Paths.get("$destination/${zipEntry.name}"))) {
                Files.createDirectories(Paths.get("$destination/${zipEntry.name}"))
            } else {
                val fos = FileOutputStream(File("$destination/${zipEntry.name}"))
                var len: Int = zis.read(buffer)
                while (len > 0) {
                    fos.write(buffer, 0, len)
                    len = zis.read(buffer)
                }
                fos.close()
            }
            zipEntry = zis.nextEntry
        }
        zis.closeEntry()
        zis.close()

        if (!notDelete)
            Files.delete(Paths.get("$source/$product.zip"))

        println(" done")
    }
}

// Classes used to deserialize json answer from ODATA
data class ODataEntry(
    @JsonAlias("Id")
    var id: String,
    @JsonAlias("Name")
    var name: String,
    @JsonAlias("IngestionDate")
    var ingestionDate: String?
)

data class ODataRoot(var d: ODataResults)

data class ODataResults(var results: List<ODataEntry>)
