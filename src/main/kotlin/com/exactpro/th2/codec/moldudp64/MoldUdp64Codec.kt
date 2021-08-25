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

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.moldudp64.MoldUdp64CodecFactory.Companion.PROTOCOL
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.addFields
import com.exactpro.th2.common.message.messageType
import com.google.protobuf.ByteString
import java.nio.ByteBuffer

class MoldUdp64Codec(private val settings: MoldUdp64CodecSettings) : IPipelineCodec {
    override fun encode(messageGroup: MessageGroup): MessageGroup {
        val messages = messageGroup.messagesList

        if (messages.isEmpty()) return messageGroup

        val header = messages[0].takeIf(AnyMessage::hasMessage)?.message?.apply {
            val protocol = metadata.protocol
            require(protocol.isEmpty() || protocol == PROTOCOL) { "Unexpected protocol: $protocol (expected: $PROTOCOL)" }
            require(messageType == HEADER_MESSAGE_TYPE) { "Unexpected header message type: $messageType (expected: $HEADER_MESSAGE_TYPE)" }
        }

        val payload = messages.subList(if (header == null) 0 else 1, messages.size).run {
            require(all(AnyMessage::hasRawMessage)) { "All payload messages must be raw messages" }
            map { it.rawMessage }
        }

        val builder = MessageGroup.newBuilder()

        // encode header and payload here

        return builder.build()
    }

    override fun decode(messageGroup: MessageGroup): MessageGroup {
        val messages = messageGroup.messagesList

        if (messages.isEmpty()) return messageGroup

        require(messages.size == 1) { "Message group contains more than 1 message" }
        require(messages[0].hasRawMessage()) { "Input message is not a raw message" }

        val message = messages[0].rawMessage
        val protocol = message.metadata.protocol

        require(protocol.isEmpty() || protocol == PROTOCOL) { "Unexpected protocol: $protocol (expected: $PROTOCOL)" }

        val builder = MessageGroup.newBuilder()

        // decode message's body here
        val sessionField = String(message.body.substring(0, 10).toByteArray())
        val sequenceField = ByteBuffer.wrap(message.body.substring(10, 18).toByteArray()).long
        val countField = ByteBuffer.wrap(message.body.substring(18, 20).toByteArray()).short.toUShort()
        val lengthsField = ArrayList<Short>()
        val messagesField = ArrayList<ByteString>()

        if (message.body.size() > 20) {
            var k = 20
            while (lengthsField.size != countField.toInt()) {
                lengthsField.add(ByteBuffer.wrap(message.body.substring(k, k + 2).toByteArray()).short)
                k += ByteBuffer.wrap(message.body.substring(k, k + 2).toByteArray()).short + 2
            }
            var firstIndex: Int = 22
            var secondIndex: Int = lengthsField.get(0).toInt() + firstIndex
            k = 1
            while (k != countField.toInt()) {
                messagesField.add(message.body.substring(firstIndex, secondIndex))
                firstIndex = secondIndex + 2
                secondIndex = firstIndex + lengthsField.get(k)
                k++
            }
            messagesField.add(message.body.substring(firstIndex, secondIndex))
        }
        builder.addMessages(
            AnyMessage.newBuilder().setMessage(
                Message.newBuilder().addFields(
                    SEQUENCE_FIELD, sequenceField,
                    SESSION_FIELD, sessionField,
                    COUNT_FIELD, countField,
                    LENGTHS_FIELD, lengthsField
                ).setMetadata(
                    MessageMetadata.newBuilder()
                        .putAllProperties(message.metadata.propertiesMap)
                        .setTimestamp(message.metadata.timestamp)
                        .setMessageType(HEADER_MESSAGE_TYPE)
                        .setId(message.metadata.id)
                        .build()
                )
                    .build()
            )
        )
        if (!messagesField.isEmpty()) {
            var k = 1
            for (msg in messagesField) {
                builder.addMessages(
                    AnyMessage.newBuilder().setRawMessage(
                        RawMessage.newBuilder()
                            .setBody(msg)
                            .setMetadata(
                                RawMessageMetadata.newBuilder()
                                    .putAllProperties(message.metadata.propertiesMap)
                                    .setTimestamp(message.metadata.timestamp)
                                    .setId(MessageID.newBuilder(message.metadata.id).addSubsequence(k).build()).build()
                            ).build()
                    )
                )
                k++
            }
        }
        println(sessionField)
        println(sequenceField)
        println(countField)
        println(lengthsField)
        println(messagesField)
        println(builder.build())
        return builder.build()
    }

    companion object {
        const val HEADER_MESSAGE_TYPE = "Header"
        const val SESSION_FIELD = "Session"
        const val SEQUENCE_FIELD = "SequenceNumber"
        const val COUNT_FIELD = "MessageCount"
        const val LENGTHS_FIELD = "MessageLengths"
    }
}
