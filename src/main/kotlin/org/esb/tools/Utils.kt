package org.esb.tools

import java.nio.file.Files
import java.nio.file.Paths
import java.text.DecimalFormat

class Utils {
    companion object {

        fun readableFileSize(size: Long): String {
            if (size <= 0) return "0"
            val units = arrayOf("B", "kB", "MB", "GB", "TB")
            val digitGroups = (Math.log10(size.toDouble()) / Math.log10(1024.0)).toInt()
            return DecimalFormat("#,##0.#").format(size / Math.pow(1024.0, digitGroups.toDouble())) + " " + units[digitGroups]
        }

        fun polarToRectangular(magnitude: Double, phase: Double): Pair<Double, Double> {
            val real = magnitude * Math.cos(Math.toRadians(phase))
            val imaginary = magnitude * Math.sin(Math.toRadians(phase))
            return real to imaginary
        }

        fun isAscending(product: String): Boolean {
            val xmlInput = Files.list(Paths.get(product)).filter { it.toString().contains("manifest") }.findFirst()
            return !Files.lines(xmlInput.get()).anyMatch { it.toLowerCase().contains("descending") }
        }
    }
}
