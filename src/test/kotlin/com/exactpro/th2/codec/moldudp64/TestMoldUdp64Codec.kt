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

import com.exactpro.th2.codec.moldudp64.MoldUdp64Codec.Companion.COUNT_FIELD
import com.exactpro.th2.codec.moldudp64.MoldUdp64Codec.Companion.HEADER_MESSAGE_TYPE
import com.exactpro.th2.codec.moldudp64.MoldUdp64Codec.Companion.LENGTHS_FIELD
import com.exactpro.th2.codec.moldudp64.MoldUdp64Codec.Companion.SEQUENCE_FIELD
import com.exactpro.th2.codec.moldudp64.MoldUdp64Codec.Companion.SESSION_FIELD
import com.exactpro.th2.codec.moldudp64.MoldUdp64CodecFactory.Companion.PROTOCOL
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.Value.KindCase.SIMPLE_VALUE
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.getList
import com.exactpro.th2.common.message.getString
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.set
import com.exactpro.th2.common.message.toTimestamp
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.util.UUID
import kotlin.random.Random
import kotlin.random.nextULong
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TestMoldUdp64Codec {
    private val codec = MoldUdp64Codec(MoldUdp64CodecSettings())

    @Nested
    inner class Positive {
        @Test
        fun `heartbeat packet round-trip`() {
            val raw = raw(
                packet = byteArrayOf(
                    115, 101, 115, 115, 105, 111, 110, 45, 53, 56, // session
                    0, 7, 21, 37, -66, 7, -36, 50, // sequence number
                    0, 0 // message count
                ),
            )

            val decoded = codec.decode(raw)

            decoded.withHeader {
                assertSession("session-58")
                assertSequenceNumber(1993576683134002U)
                assertMessageCount(0U)
                assertMessageLengths(listOf())
            }

            decoded.assertMessageCount(0)
            assertEquals(raw, codec.encode(decoded))
        }

        @Test
        fun `end-of-session packet round-trip`() {
            val raw = raw(
                packet = byteArrayOf(
                    115, 101, 115, 115, 105, 111, 110, 45, 52, 54, // session
                    0, 7, 21, 37, -67, 100, 99, -123, // sequence number
                    -1, -1 // message count
                ),
            )

            val decoded = codec.decode(raw)

            decoded.withHeader {
                assertSession("session-46")
                assertSequenceNumber(1993576672420741U)
                assertMessageCount(0xFFFFU)
                assertMessageLengths(listOf())
            }

            decoded.assertMessageCount(0)
            assertEquals(raw, codec.encode(decoded))
        }

        @Test
        fun `request packet round-trip`() {
            val raw = raw(
                packet = byteArrayOf(
                    115, 101, 115, 115, 105, 111, 110, 45, 57, 49, // session
                    0, 7, 21, 37, -66, 46, 88, 121, // sequence number
                    3, -24 // message count
                ),
                th2Direction = SECOND
            )

            val decoded = codec.decode(raw)

            decoded.withHeader {
                assertSession("session-91")
                assertSequenceNumber(1993576685656185U)
                assertMessageCount(1000U)
                assertMessageLengths(listOf())
            }

            decoded.assertMessageCount(0)
            assertEquals(raw, codec.encode(decoded))
        }

        @Test
        fun `regular packet round-trip`() {
            val raw = raw(
                packet = byteArrayOf(
                    115, 101, 115, 115, 105, 111, 110, 45, 57, 48, // session
                    0, 7, 21, 37, -77, 61, 105, -85, // sequence number
                    0, 3, // message count
                    0, 13, // message 1 length
                    102, 105, 114, 115, 116, 32, 109, 101, 115, 115, 97, 103, 101, // message 1
                    0, 0, // message 2 length
                    0, 13, // message 3 length
                    116, 104, 105, 114, 100, 32, 109, 101, 115, 115, 97, 103, 101 // message 3
                ),
            )

            val decoded = codec.decode(raw)

            decoded.withHeader {
                assertSession("session-90")
                assertSequenceNumber(1993576502094251U)
                assertMessageCount(3U)
                assertMessageLengths(listOf(13U, 0U, 13U))
            }

            decoded.assertMessageCount(3)
            decoded.assertMessage(0, "first message".toByteArray())
            decoded.assertMessage(1, byteArrayOf())
            decoded.assertMessage(2, "third message".toByteArray())

            assertEquals(raw, codec.encode(decoded))
        }

        @Test
        fun `encoding payload without header`() {
            val parsed = parsed(
                header(),
                message("first message".toByteArray()),
                message(byteArrayOf()),
                message("third message".toByteArray())
            ).toBuilder().run {
                removeMessages(0)
                build()
            }

            val raw = codec.encode(parsed)

            assertEquals(1, raw.messagesCount, "Unexpected count of decoded messages")
            assertTrue(raw.getMessages(0).hasRawMessage(), "Unexpected parsed message")

            val actualBody = raw.getMessages(0).rawMessage.body.toByteArray()
            val expectedBody = byteArrayOf(
                32, 32, 32, 32, 32, 32, 32, 32, 32, 32, // session
                0, 0, 0, 0, 0, 0, 0, 0, // sequence number
                0, 3, // message count
                0, 13, // message 1 length
                102, 105, 114, 115, 116, 32, 109, 101, 115, 115, 97, 103, 101, // message 1
                0, 0, // message 2 length
                0, 13, // message 3 length
                116, 104, 105, 114, 100, 32, 109, 101, 115, 115, 97, 103, 101 // message 3
            )

            assertArrayEquals(expectedBody, actualBody)
        }
    }

    @Nested
    inner class Negative {
        @Test
        fun `invalid header protocol`() {
            val parsed = parsed(header().apply { metadataBuilder.protocol = "http" })
            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Unexpected protocol: http (expected: $PROTOCOL)", exception.message)
        }

        @Test
        fun `invalid header message type`() {
            val parsed = parsed(header().apply { messageType = "zzz" })
            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Unexpected header message type: zzz (expected: $HEADER_MESSAGE_TYPE)", exception.message)
        }

        @Test
        fun `parsed payload message`() {
            val group = MessageGroup.newBuilder().apply {
                this += header()
                this += Message.getDefaultInstance()
            }.build()

            val exception = assertThrows<IllegalArgumentException> { codec.encode(group) }
            assertEquals("All payload messages must be raw messages", exception.message)
        }

        @Test
        fun `more than 1 message to decode`() {
            val group = MessageGroup.newBuilder().apply {
                this += RawMessage.getDefaultInstance()
                this += RawMessage.getDefaultInstance()
            }.build()

            val exception = assertThrows<IllegalArgumentException> { codec.decode(group) }
            assertEquals("Message group contains more than 1 message", exception.message)
        }

        @Test
        fun `decode parsed message`() {
            val group = MessageGroup.newBuilder().apply {
                this += Message.getDefaultInstance()
            }.build()

            val exception = assertThrows<IllegalArgumentException> { codec.decode(group) }
            assertEquals("Input message is not a raw message", exception.message)
        }

        @Test
        fun `invalid packet protocol`() {
            val group = MessageGroup.newBuilder().apply {
                this += RawMessage.newBuilder().apply { metadataBuilder.protocol = "http" }
            }.build()

            val exception = assertThrows<IllegalArgumentException> { codec.decode(group) }
            assertEquals("Unexpected protocol: http (expected: $PROTOCOL)", exception.message)
        }

        @Test
        fun `incomplete header`() {
            val raw = raw(packet = byteArrayOf(1, 2, 3, 4))
            val exception = assertThrows<IllegalArgumentException> { codec.decode(raw) }
            assertEquals("Not enough bytes to decode packet header from: 4 (expected at least: 20)", exception.message)
        }

        @Test
        fun `empty payload but positive message count`() {
            val raw = raw(
                packet = byteArrayOf(
                    115, 101, 115, 115, 105, 111, 110, 45, 53, 56,
                    0, 7, 21, 37, -66, 7, -36, 50,
                    0, 2
                ),
            )

            val exception = assertThrows<IllegalArgumentException> { codec.decode(raw) }
            assertEquals("Not enough bytes to decode 2 messages from: 0 (expected at least: 4)", exception.message)
        }

        @Test
        fun `message length is greater then expected`() {
            val raw = raw(
                packet = byteArrayOf(
                    115, 101, 115, 115, 105, 111, 110, 45, 53, 56,
                    0, 7, 21, 37, -66, 7, -36, 50,
                    0, 2,
                    0, 1,
                    1,
                    0, 8,
                    2, 2
                ),
            )

            val exception = assertThrows<IllegalArgumentException> { codec.decode(raw) }
            assertEquals("Not enough bytes to decode message 2 from: 2 (expected: 8)", exception.message)
        }

        @Test
        fun `garbage at the end of a packet`() {
            val raw = raw(
                packet = byteArrayOf(
                    115, 101, 115, 115, 105, 111, 110, 45, 53, 56,
                    0, 7, 21, 37, -66, 7, -36, 50,
                    0, 1,
                    0, 1,
                    1,
                    0, 8,
                    2, 2
                ),
            )

            val exception = assertThrows<IllegalStateException> { codec.decode(raw) }
            assertEquals("Decoded all messages but still have 4 bytes to decode", exception.message)
        }

        @Test
        fun `long session`() {
            val parsed = parsed(header(packetSession = "session-1337"))
            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Cannot fit session alias into 10 bytes: session-1337", exception.message)
        }

        @Test
        fun `big sequence number`() {
            val parsed = parsed(header().addField(SEQUENCE_FIELD, "18446744073709551616"))
            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Cannot fit sequence number into 8 bytes: 18446744073709551616", exception.message)
        }

        @Test
        fun `negative sequence number`() {
            val parsed = parsed(header().addField(SEQUENCE_FIELD, "-1"))
            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Negative sequence number: -1", exception.message)
        }

        @Test
        fun `big message count`() {
            val parsed = parsed(header().addField(COUNT_FIELD, "65536"))
            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Cannot fit message count into 2 bytes: 65536", exception.message)
        }

        @Test
        fun `negative message count`() {
            val parsed = parsed(header().addField(COUNT_FIELD, "-1"))
            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Negative message count: -1", exception.message)
        }

        @Test
        fun `invalid message count`() {
            val parsed = parsed(header(packetMessageCount = 2U))
            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Invalid message count: 2 (expected: 0)", exception.message)
        }

        @Test
        fun `big message length`() {
            val parsed = parsed(
                header(packetMessageCount = 1U).addField(LENGTHS_FIELD, listOf("65536")),
                message(byteArrayOf())
            )

            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Cannot fit length of message 1 into 2 bytes: 65536", exception.message)
        }

        @Test
        fun `negative message length`() {
            val parsed = parsed(
                header(packetMessageCount = 1U).addField(LENGTHS_FIELD, listOf("-1")),
                message(byteArrayOf())
            )

            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Negative length of message 1: -1", exception.message)
        }

        @Test
        fun `invalid amount of message lengths`() {
            val parsed = parsed(
                header(packetMessageCount = 1U, packetMessageLengths = listOf(0U, 1U)),
                message(byteArrayOf())
            )

            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Invalid amount of message lengths: 2 (expected: 1)", exception.message)
        }

        @Test
        fun `mismatching message length`() {
            val parsed = parsed(
                header(packetMessageCount = 1U, packetMessageLengths = listOf(2U)),
                message(byteArrayOf(1, 2, 3))
            )

            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Mismatching length of message 1: 2 (expected: 3)", exception.message)
        }

        @Test
        fun `big actual message length`() {
            val parsed = parsed(
                header(packetMessageCount = 1U),
                message(Random.nextBytes(65536))
            )

            val exception = assertThrows<IllegalArgumentException> { codec.encode(parsed) }
            assertEquals("Actual length of message 1 is too big: 65536 (expected at most: 65535)", exception.message)
        }
    }

    companion object {
        private fun raw(
            packet: ByteArray,
            th2SessionAlias: String = UUID.randomUUID().toString(),
            th2Direction: Direction = FIRST,
            th2Sequence: Long = System.currentTimeMillis(),
            th2MetadataProperties: Map<String, String> = mapOf("uuid-${Random.nextULong(256U)}" to UUID.randomUUID().toString())
        ): MessageGroup = MessageGroup.newBuilder().apply {
            this += RawMessage.newBuilder().apply {
                metadataBuilder.apply {
                    putAllProperties(th2MetadataProperties)
                    timestamp = Instant.now().toTimestamp()
                    idBuilder.apply {
                        connectionIdBuilder.sessionAlias = th2SessionAlias
                        direction = th2Direction
                        sequence = th2Sequence
                    }
                }

                body = ByteString.copyFrom(packet)
            }
        }.build()

        private fun header(
            packetSession: String = System.nanoTime().toString(),
            packetSequenceNumber: ULong = System.nanoTime().toULong(),
            packetMessageCount: UShort = 0U,
            packetMessageLengths: List<UShort> = listOf(),
            th2SessionAlias: String = UUID.randomUUID().toString(),
            th2Direction: Direction = FIRST,
            th2Sequence: Long = Random.nextLong(),
            th2MetadataProperties: Map<String, String> = mapOf("uuid-${Random.nextULong(256U)}" to UUID.randomUUID().toString())
        ): Message.Builder = Message.newBuilder().apply {
            messageType = HEADER_MESSAGE_TYPE
            sessionAlias = th2SessionAlias
            direction = th2Direction
            sequence = th2Sequence

            metadataBuilder.apply {
                timestamp = Instant.now().toTimestamp()
                putAllProperties(th2MetadataProperties)
            }

            this[SESSION_FIELD] = packetSession
            this[SEQUENCE_FIELD] = packetSequenceNumber
            this[COUNT_FIELD] = packetMessageCount
            this[LENGTHS_FIELD] = packetMessageLengths
        }

        private fun message(messageBody: ByteArray): RawMessage.Builder = RawMessage.newBuilder().setBody(ByteString.copyFrom(messageBody))

        fun parsed(
            header: Message.Builder,
            vararg messages: RawMessage.Builder
        ): MessageGroup = MessageGroup.newBuilder().run {
            this += header

            messages.forEachIndexed { index, message ->
                this += message.apply {
                    metadataBuilder.apply {
                        putAllProperties(header.metadataBuilder.propertiesMap)
                        idBuilder.apply {
                            connectionIdBuilder.sessionAlias = header.sessionAlias
                            direction = header.direction
                            sequence = header.sequence
                            timestamp = header.metadata.timestamp
                            addSubsequence(index + 1)
                        }
                    }
                }
            }

            build()
        }

        private fun MessageGroup.assertHeaderPresence() {
            assertTrue(messagesList.isNotEmpty(), "Message group is empty")
            assertTrue(messagesList[0].hasMessage(), "Header message is not a parsed one")
            assertEquals(HEADER_MESSAGE_TYPE, messagesList[0].message.messageType, "Unexpected header message type")
        }

        private fun MessageGroup.withHeader(block: Message.() -> Unit) {
            assertHeaderPresence()
            messagesList[0].message.block()
        }

        private fun Message.assertSession(session: String) {
            assertEquals(session, getString(SESSION_FIELD), "Unexpected packet session")
        }

        private fun Message.assertSequenceNumber(sequenceNumber: ULong) {
            val parsedSequenceNumber = assertNotNull(getString(SEQUENCE_FIELD), "No sequence number field")
            assertEquals(sequenceNumber, parsedSequenceNumber.toULong(), "Unexpected packet sequence number")
        }

        private fun Message.assertMessageCount(messageCount: UShort) {
            val parsedMessageCount = assertNotNull(getString(COUNT_FIELD), "No message count field")
            assertEquals(messageCount, parsedMessageCount.toUShort(), "Unexpected packet message count")
        }

        private fun Message.assertMessageLengths(messageLengths: List<UShort>) {
            val packetMessageLengths = assertNotNull(getList(LENGTHS_FIELD), "No message lengths field").map {
                assertEquals(SIMPLE_VALUE, it.kindCase, "Message length is not a simple value: $it")
                it.simpleValue.toUShort()
            }

            assertEquals(messageLengths, packetMessageLengths, "Unexpected packet message lengths")
        }

        private fun MessageGroup.assertMessageCount(count: Int) {
            val messages = messagesList.subList(1, messagesCount)
            assertTrue(messages.all(AnyMessage::hasRawMessage), "Not all messages are raw messages")
            assertEquals(count, messages.size, "Unexpected message count")
        }

        private fun MessageGroup.withMessage(index: Int, block: RawMessage.() -> Unit) = messagesList[index + 1].rawMessage.block()

        private fun RawMessage.assertContent(content: ByteArray) = assertArrayEquals(content, body.toByteArray(), "Unexpected payload content")

        private fun RawMessage.assertSessionAlias(sessionAlias: String) = assertEquals(sessionAlias, metadata.id.connectionId.sessionAlias, "Unexpected session alias")

        private fun RawMessage.assertSequence(sequence: Long) = assertEquals(sequence, metadata.id.sequence, "Unexpected sequence")

        private fun RawMessage.assertSubsequence(subsequence: Int) = assertEquals(listOf(subsequence), metadata.id.subsequenceList, "Unexpected subsequence")

        private fun RawMessage.assertTimestamp(timestamp: Timestamp) = assertEquals(timestamp, metadata.timestamp, "Unexpected timestamp")

        private fun MessageGroup.assertMessage(index: Int, content: ByteArray) = withHeader {
            val sessionAlias = sessionAlias
            val sequence = sequence
            val timestamp = metadata.timestamp

            withMessage(index) {
                assertSessionAlias(sessionAlias)
                assertSequence(sequence)
                assertSubsequence(index + 1)
                assertTimestamp(timestamp)
                assertContent(content)
            }
        }
    }
}
