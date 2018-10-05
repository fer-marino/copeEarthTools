package org.esb.tools.controllers

import com.fasterxml.jackson.annotation.JsonAlias
import org.apache.commons.io.FileUtils
import org.apache.commons.io.input.CountingInputStream
import org.esb.tools.Utils
import org.esb.tools.configuration.DataHubConfiguration
import org.esb.tools.model.DataHub
import org.esb.tools.model.Product
import org.jline.utils.AttributedString
import org.jline.utils.AttributedStyle
import org.springframework.beans.factory.annotation.Autowired
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
import java.util.concurrent.LinkedBlockingQueue
import java.util.zip.ZipInputStream

@ShellComponent
class DataAccessCommands {
    @Autowired
    lateinit var restTemplateBuilder: RestTemplateBuilder
    @Autowired
    lateinit var dataHubConfiguration: DataHubConfiguration
    private var selectedHub: DataHub? = null

    @Bean
    fun myPromptProvider(): PromptProvider = PromptProvider {
        AttributedString("esb:>", AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW))
    }

    @ShellMethod("Manage available data hubs")
    fun dataHub(@ShellOption(defaultValue = "") id: String) {
        if (id.isNullOrEmpty()) {
            println(" ** Available hubs are ${dataHubConfiguration.hubs.map { it.id }.reduceRight { s, acc -> (if (s == selectedHub?.id) "[$s]" else s) + ", $acc" }} ")
        } else {
            val hub = dataHubConfiguration.hubs.find { it.id == id }
            if (hub != null)
                selectedHub = hub
            else
                println(" ** DataHub with id $id not found. Available hubs are ${dataHubConfiguration.hubs.map { it.id }.reduceRight { s, acc -> (if (s == selectedHub?.id) "\\e[1m$s\\e[21m" else s) + ", $acc" }} ")
        }
    }

    @ShellMethod("Query Serco Dias catalogue and download all the results")
    fun ondaSearch(
            @ShellOption(defaultValue = "*") filter: String,
            @ShellOption(defaultValue = "IngestionDate desc") orderBy: String,
            @ShellOption(defaultValue = "") destination: String
    ): List<String> {
        if (selectedHub == null) {
            println(" **** No hub selected. Select an hub first")
            return listOf()
        }

        var skip = 0
        val pageSize = 100
        val out = mutableListOf<String>()

        val downloader = Downloader(2)
        val hub = selectedHub!!

        val tpl = restTemplateBuilder.basicAuthorization(hub.username, hub.password).build()
        try {
            do {
                val start = System.currentTimeMillis()
                val headers = HttpHeaders()
                headers.accept = listOf(MediaType.APPLICATION_JSON)
                val entity = HttpEntity("parameters", headers)
                val query = "${hub.url}/Products?\$search=\"$filter\"&\$skip=$skip&\$top=$pageSize&\$format=json"
                if (skip == 0)
                    println(" * Running query $query")
                else
                    print("\r * Requesting page ${skip / pageSize + 1} " +
                            "                                                                                                                         ")
                val response = tpl.exchange(query, HttpMethod.GET, entity, Map::class.java)
                val feed = response.body!!["value"] as List<Map<String, Any>>

                if (skip == 0)
                    println(" * Returned lots of products in ${System.currentTimeMillis() - start}msec")
                var skipCount = 0
                for (entry in feed) {
                    if (!(Files.exists(Paths.get(destination)) && Files.list(Paths.get(destination)).map { it.toString() }.anyMatch { it.contains(entry["title"].toString()) })) {
                        if (skipCount != 0 && skipCount != 100) {
                            print("\r * Skipped $skipCount products as already downloaded")
                            skipCount = 0
                        }
                        if (destination.isNotEmpty())
                            downloader.download(Product(entry["id"].toString(), entry["name"].toString(), destination, 0, hub))
                        else
                            println(" ** ${entry["name"]}")
                    } else
                        skipCount++

                    out.add(entry["title"].toString())
                }

                if (skipCount != 0 && skipCount != 100) print("\r * Skipped $skipCount products as already downloaded  " +
                        "                                                                                                                         ")

                skip += pageSize
                if (feed.size < 100) break
            } while (true)
        } catch (e: HttpStatusCodeException) {
            println("HTTP Error (${e.statusCode}): ${e.message}")
            println(e.responseBodyAsString)
        }

        while (downloader.isActive()) Thread.sleep(2000)

        return out
    }

    @ShellMethod("Query Copernicus open hub on Open Sarch API and download all the results")
    fun oSearch(
            @ShellOption(defaultValue = "*") filter: String,
            @ShellOption(defaultValue = "") orderBy: String,
            @ShellOption(defaultValue = "") destination: String,
            @ShellOption(defaultValue = Integer.MAX_VALUE.toString()) limit: Int = Integer.MAX_VALUE
    ): List<String> {
        if (selectedHub == null) {
            println(" **** No hub selected. Select an hub first")
            return listOf()
        }

        var skip = 0
        val pageSize = 100
        val out = mutableListOf<String>()
        var totalProduct = 0

        val downloader = Downloader(2)
        val hub = selectedHub!!

        val tpl = restTemplateBuilder.basicAuthorization(hub.username, hub.password).build()
        var skipCount = 0
        try {
            loop@ do {
                val start = System.currentTimeMillis()
                val headers = HttpHeaders()
                headers.accept = listOf(MediaType.APPLICATION_JSON)
                val entity = HttpEntity("parameters", headers)
                val ordering = if(orderBy.isNotBlank()) "&orderby=$orderBy" else ""
                val query = "${hub.url}/search?start=$skip&rows=$pageSize$ordering&format=json&q=$filter"
                if (skip == 0)
                    println(" * Running query $query")
                else if (skip < totalProduct)
                    print("\r * Requesting page ${skip / pageSize + 1} of ${totalProduct / pageSize + 1} " +
                            "                                                                                                                 ")
                val response = tpl.exchange(query, HttpMethod.GET, entity, Map::class.java)
                val feed = response.body!!["feed"] as Map<String, Any>

                if (skip == 0)
                    println(" * Returned ${feed["opensearch:totalResults"]} products in ${System.currentTimeMillis() - start}msec")
                totalProduct = feed["opensearch:totalResults"].toString().toInt()
                if (feed.containsKey("entry")) {
                    for (entry in feed["entry"] as List<Map<String, Any>>) {
                        if (!(Files.exists(Paths.get(destination)) && Files.list(Paths.get(destination)).map { it.toString() }.anyMatch { it.contains(entry["title"].toString()) })) {
                            if (skipCount != 0 && skipCount % 100 != 0) {
                                print("\r * Skipped $skipCount products as already downloaded                                                                                                                          ")
                                skipCount = 0
                            }
                            if (destination.isNotEmpty())
                                downloader.download(Product(entry["id"].toString(), entry["title"].toString(), destination, 0, hub))
                            else
                                println(" ** ${entry["title"]}")
                        } else
                            skipCount++

                        out.add(entry["title"].toString())

                        if(out.size > limit)
                            break@loop
                    }

                    if (skipCount != 0 && skipCount % 100 != 0) print("\r * Skipped $skipCount products as already downloaded                                                                                          ")

                    skip += pageSize
                } else
                    break
            } while (true)
            println()
        } catch (e: HttpStatusCodeException) {
            println("HTTP Error (${e.statusCode}): ${e.message}")
            println(e.responseBodyAsString)
        }

        while (downloader.isActive()) Thread.sleep(2000)

        return out
    }

    @ShellMethod("Query Copernicus open hub on ODATA API and download all the results")
    fun oData(
            @ShellOption(defaultValue = "substringof('SR_2_LAN', Name)") filter: String,
            @ShellOption(defaultValue = "IngestionDate desc") orderBy: String,
            @ShellOption(defaultValue = "download") destination: String
    ): List<String> {
        if (selectedHub == null) {
            println(" **** No hub selected. Select an hub first")
            return listOf()
        }

        var skip = 0
        val pageSize = 100
        val out = mutableListOf<String>()

        val downloader = Downloader(2)
        val hub = selectedHub!!

        val tpl = restTemplateBuilder.basicAuthorization(hub.username, hub.password).build()

        try {
            do {
                val start = System.currentTimeMillis()
                val headers = HttpHeaders()
                headers.accept = listOf(MediaType.APPLICATION_JSON)
                val entity = HttpEntity("parameters", headers)
                val query = "${hub.url}/odata/v1/Products?\$filter=$filter" +
                        "&\$orderby=$orderBy&\$skip=$skip&\$top=$pageSize"
                println(" * Running query $query")
                val response = tpl.exchange(query, HttpMethod.GET, entity, ODataRoot::class.java)

                println(" * Total products ${response.body!!.d.results.size} in ${System.currentTimeMillis() - start}msec")
                var skipCount = 0
                for (entry in response.body!!.d.results) {
                    if (!Files.exists(Paths.get(destination, entry.name + ".SEN3"))) {
                        if (skipCount != 0) {
                            println(" * Skipped $skipCount products as already downloaded")
                            skipCount = 0
                        }
                        downloader.download(Product(entry.id, entry.name, destination, 0, hub))
//                        downloadProduct(entry.id, entry.name, username, password, destination)
//                        unzip(entry.name, destination, destination)
                    } else
                        skipCount++
                    out.add(entry.name)
                }

                if (skipCount != 0) println(" * Skipped $skipCount products as already downloaded")

                skip += pageSize
            } while (response.body!!.d.results.size == pageSize)
        } catch (e: HttpStatusCodeException) {
            println("HTTP Error (${e.statusCode}): ${e.message}")
            println(e.responseBodyAsString)
        }

        return out
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

    class Downloader(val threads: Int = 1) {
        private val queue = LinkedBlockingQueue<Product>()
        private val activeDownloads = LinkedBlockingQueue<Download>()

        fun isActive() = queue.size > 0 || activeDownloads.size > 0

        fun download(product: Product) {
            queue.add(product)
        }

        init {
            Thread {
                while (true) {
                    activeDownloads.removeIf { !it.isActive() }

                    if (activeDownloads.size < threads) {
                        val toDownload = queue.poll()

                        if (toDownload != null) {
                            val newDownload = Download(toDownload)
                            activeDownloads.offer(newDownload)
                            newDownload.download()
                        }
                    }

                    Thread.sleep(1000)
                }
            }.start()

            // status update
            Thread {
                var sleep: Boolean
                while (true) {
                    sleep = false
                    activeDownloads.forEachIndexed { i, it ->
                        print("\r * [${queue.size} ${i + 1}] $it")
                        Thread.sleep(2000)
                        sleep = true
                    }
                }
            }.start()
        }
    }

    class Download(private val product: Product) {
        private var stream: CountingInputStream
        private var prevSize = 0L
        private var completed = false

        init {
            val uc: HttpURLConnection = if (product.hub.id.toLowerCase().contains("dias"))
                URL("${product.hub.url}/Products(${product.id})/\$value").openConnection() as HttpURLConnection
            else
                URL("${product.hub.url}/odata/v1/Products('${product.id}')/\$value").openConnection() as HttpURLConnection
            try {
                val basicAuth = "Basic " + Base64Utils.encodeToString("${product.hub.username}:${product.hub.password}".toByteArray())
                uc.setRequestProperty("Authorization", basicAuth)
                uc.setRequestProperty("Accept-Encoding", "gzip, deflate, br")
                uc.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                product.name = uc.headerFields["Content-Disposition"]?.first()?.substringAfter("filename=")?.replace("\"", "") ?: product.name
                stream = CountingInputStream(uc.inputStream)
                product.size = uc.contentLengthLong
            } catch (e: Exception) {
                println(" ** Error while establishing download connection for product ${product.name}: ${e.message} ")
                stream = CountingInputStream(null)
                completed = true
            }
        }

        override fun toString(): String {
            val out = "Downloading ${product.name} of size ${Utils.readableFileSize(product.size!!)}: " +
                    "${Math.round(stream.byteCount * 1000f / product.size!!) / 10f}% (${Utils.readableFileSize(stream.byteCount - prevSize)}/sec)              "
            prevSize = stream.byteCount
            return out
        }

        fun download() {
            if (completed) return
            Thread {
                try {
                    val start = System.currentTimeMillis()
                    FileUtils.copyInputStreamToFile(stream, Paths.get(product.destination, product.name).toFile())

                    // unzip
                    if (product.name.endsWith(".zip")) {
                        val buffer = ByteArray(1024)
                        val zis = ZipInputStream(FileInputStream(Paths.get(product.destination, product.name).toFile()))
                        var zipEntry = zis.nextEntry
                        while (zipEntry != null) {
                            if (zipEntry.isDirectory && !Files.exists(Paths.get("${product.destination}/${zipEntry.name}"))) {
                                Files.createDirectories(Paths.get("${product.destination}/${zipEntry.name}"))
                            } else {
                                val fos = FileOutputStream(File("${product.destination}/${zipEntry.name}"))
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

                        Files.delete(Paths.get("${product.destination}/${product.name}"))
                    }
//                    println("\r ** Download of ${product.name} successfully completed in ${(System.currentTimeMillis() - start) / 1000} seconds                                        ")
                } catch (e: Exception) {
                    println("An error occurred during download of ${product.name}: ${e.message}")
                } finally {
                    completed = true
                }
            }.start()
        }

        fun isActive() = !completed
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
