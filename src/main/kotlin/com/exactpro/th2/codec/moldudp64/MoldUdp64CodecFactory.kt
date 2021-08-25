/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec.moldudp64

import com.exactpro.th2.codec.api.IPipelineCodecContext
import com.exactpro.th2.codec.api.IPipelineCodecFactory
import com.exactpro.th2.codec.api.IPipelineCodecSettings
import mu.KotlinLogging

class MoldUdp64CodecFactory : IPipelineCodecFactory {
    override val settingsClass: Class<out IPipelineCodecSettings> = MoldUdp64CodecSettings::class.java
    override val protocol: String = PROTOCOL

    override fun init(context: IPipelineCodecContext) = Unit

    override fun create(settings: IPipelineCodecSettings?) = MoldUdp64Codec(requireNotNull(settings as? MoldUdp64CodecSettings) {
        "settings is not an instance of ${MoldUdp64CodecSettings::class.simpleName}: $settings"
    })

    companion object {
        const val PROTOCOL = "moldudp64"
        private val LOGGER = KotlinLogging.logger {}
    }
}