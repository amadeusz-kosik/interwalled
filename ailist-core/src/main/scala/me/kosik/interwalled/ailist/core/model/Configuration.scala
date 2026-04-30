package me.kosik.interwalled.ailist.core.model


final case class Configuration(
    intervalsCountToCheckLookahead: Int,
    intervalsCountToTriggerExtraction: Int,
    maximumComponentSize: Int
)

object Configuration {
    val DEFAULT: Configuration = Configuration(24, 16, 100000)

    def apply(): Configuration =
        Configuration(24, 16, 100000)
}

