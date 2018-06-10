package org.esb.tools.model

data class Product(val id: String, val name: String, val destination: String, var size: Long?, val hub: DataHub)