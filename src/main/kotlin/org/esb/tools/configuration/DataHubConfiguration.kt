package org.esb.tools.configuration

import org.esb.tools.model.DataHub
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.NestedConfigurationProperty
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("datahub")
class DataHubConfiguration {
    var proxyUrl: String = ""
    @NestedConfigurationProperty
    var hubs: List<DataHub> = mutableListOf()
}