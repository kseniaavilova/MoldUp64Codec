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
import com.exactpro.th2.common.message.*
import com.exactpro.th2.common.value.getInt
import com.google.common.primitives.Longs
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.*
import kotlin.collections.ArrayList

class MoldUdp64Codec(private val settings: MoldUdp64CodecSettings) : IPipelineCodec {

    private fun convertToBytes(value: Int): ByteArray {
        val result = ByteArray(2)
        result[0] = (value ushr 8 and 0xFF).toByte()
        result[1] = (value and 0xFF).toByte()
        return result
    }

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
        var messageBody: ByteString
        var index = 0
        val propertiesMap: Map<String, String>
        val timestamp: Timestamp
        val messageId: MessageID
        if (header == null) {
            messageBody = ByteString.copyFrom(" ".repeat(10).toByteArray())
            messageBody = messageBody.concat(ByteString.copyFrom(ByteArray(8)))
            messageBody = messageBody.concat(ByteString.copyFrom(convertToBytes(payload.size)))
            propertiesMap = payload[0].metadata.propertiesMap
            timestamp = payload[0].metadata.timestamp
            messageId = payload[0].metadata.id

            for (msg in payload) {
                messageBody = messageBody.concat(ByteString.copyFrom(convertToBytes(msg.body.size())))
                messageBody = messageBody.concat(msg.body)
            }

        } else {

            val sequenceField = header.getLong(SEQUENCE_FIELD)
                ?: throw IllegalArgumentException(
                    "Cannot fit sequence number into 8 bytes: " + header.getString(
                        SEQUENCE_FIELD
                    )
                )
            if (sequenceField < 0) {
                throw IllegalArgumentException("Negative sequence number: $sequenceField")

            }
            val lengthsField = header.getList(LENGTHS_FIELD)
            val countMessage = header.getInt(COUNT_FIELD)
            if (countMessage != null) {
                if (countMessage > 65535) {
                    throw IllegalArgumentException("Cannot fit message count into 2 bytes: $countMessage")
                }
                if (countMessage < 0) {
                    throw IllegalArgumentException("Negative message count: $countMessage")
                }
            }
            if (countMessage != 65535 && header.metadata.id.direction != Direction.SECOND) {


                if (lengthsField != null) {
                    if (countMessage != null) {
                        if (countMessage < lengthsField.size) {
                            throw IllegalArgumentException("Invalid amount of message lengths: " + lengthsField.size + " (expected: " + countMessage + ")")
                        }
                    }
                }



                if (countMessage != payload.size) {
                    throw IllegalArgumentException("Invalid message count: " + countMessage + " (expected: " + payload.size + ")")
                }


                if (lengthsField != null) {
                    for (i in lengthsField.indices) {
                        val num = lengthsField[i].getInt()
                        if (num != null) {
                            if (num > 65535) {
                                throw IllegalArgumentException("Cannot fit length of message 1 into 2 bytes: $num")
                            }
                            if (num < 0) {
                                throw IllegalArgumentException("Negative length of message " + (i + 1) + ": " + num)
                            }
                            val messageSize: Int = payload[i].body.size()
                            if (num != messageSize) {
                                throw java.lang.IllegalArgumentException("Mismatching length of message " + (i + 1) + ": " + num + " (expected: " + messageSize + ")")
                            }
                        }
                    }
                    for (i in payload.indices) {
                        val messageSize: Int = payload[i].body.size()
                        if (messageSize > 65535) {
                            throw java.lang.IllegalArgumentException("Actual length of message " + (i + 1) + " is too big: " + messageSize + " (expected at most: 65535)")
                        }
                    }
                }
            }

            val sessionField = header.getString(SESSION_FIELD)
            if (sessionField != null) {
                if (sessionField.toByteArray().size > 10) {
                    throw IllegalArgumentException("Cannot fit session alias into 10 bytes: $sessionField")
                }
            }

            propertiesMap = header.metadata.propertiesMap
            timestamp = header.metadata.timestamp
            messageId = header.metadata.id
            messageBody = ByteString.copyFrom(sessionField, Charset.defaultCharset())
            messageBody = messageBody.concat(ByteString.copyFrom(sequenceField.let { Longs.toByteArray(it) }))
            messageBody = messageBody.concat(ByteString.copyFrom(countMessage?.let { convertToBytes(it) }))

            if (payload.isNotEmpty()) {
                for (msg in payload) {
                    messageBody = messageBody.concat(
                        ByteString.copyFrom(lengthsField?.get(index)?.getInt()
                            ?.let { convertToBytes(it) })
                    )
                    messageBody = messageBody.concat(msg.body)
                    index++
                }
            }

        }
        builder.addMessages(
            AnyMessage.newBuilder().setRawMessage(
                RawMessage.newBuilder()
                    .setBody(messageBody)
                    .setMetadata(
                        RawMessageMetadata.newBuilder()
                            .putAllProperties(propertiesMap)
                            .setTimestamp(timestamp)
                            .setId(MessageID.newBuilder(messageId).build())
                    )
            )
        )
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
        if (message.body.size()<20) {
            throw  IllegalArgumentException("Not enough bytes to decode packet header from: " + message.body.size() + " (expected at least: 20)")
        }

        // decode message's body here
        val sessionField = String(message.body.substring(0, 10).toByteArray())
        val sequenceField = ByteBuffer.wrap(message.body.substring(10, 18).toByteArray()).long
        val countField = ByteBuffer.wrap(message.body.substring(18, 20).toByteArray()).short.toUShort()
        val lengthsField = ArrayList<Short>()
        val messagesField = ArrayList<ByteString>()

        if (message.body.size() > 20) {
            var index = 20
            while (lengthsField.size != countField.toInt()) {
                lengthsField.add(ByteBuffer.wrap(message.body.substring(index, index + 2).toByteArray()).short)
                index += ByteBuffer.wrap(message.body.substring(index, index + 2).toByteArray()).short + 2
            }
            var firstIndex = 22
            var secondIndex: Int = lengthsField[0].toInt() + firstIndex
            index = 1
            while (index != countField.toInt()) {
                messagesField.add(message.body.substring(firstIndex, secondIndex))
                firstIndex = secondIndex + 2
                secondIndex = firstIndex + lengthsField[index]
                index++
            }
            if (secondIndex>message.body.size()) {
                throw IllegalArgumentException("Not enough bytes to decode message "+ (messagesField.size+1) +" from: "+(message.body.size()-firstIndex) + " (expected: " + lengthsField[index-1] + ")")
            }
            messagesField.add(message.body.substring(firstIndex, secondIndex))
        }

        if (countField.toInt() == 65535 || message.metadata.id.direction == Direction.SECOND) {

        } else {
            if (countField.toInt() > 0 && message.body.size() == 20) {
                throw IllegalArgumentException("Not enough bytes to decode " + countField.toInt() + " messages from: 0 (expected at least: " + countField.toInt() * 2 + ")")
            }

            if (message.body.size() > 20) {
                var messageSize = 0
                for (integer in lengthsField) {
                    messageSize += integer + 2
                }
                check(message.body.size() -  messageSize <= 20) { "Decoded all messages but still have " + (message.body.size() -  messageSize - 20) + " bytes to decode" }
            }
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
        if (messagesField.isNotEmpty()) {
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


