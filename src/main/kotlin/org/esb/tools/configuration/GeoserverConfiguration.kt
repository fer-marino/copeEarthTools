package org.esb.tools.configuration

import org.esb.tools.model.DataHub
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.NestedConfigurationProperty
import org.springframework.context.annotation.Configuration


@Configuration
@ConfigurationProperties("geoserver")
class GeoserverConfiguration {
    var baseUrl: String = ""
    var username: String = "admin"
    var password: String = "geoserver"
    var workspace: String = "esb"
}