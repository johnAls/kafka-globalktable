package se.mm;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class GlobalKTableIntegrationTest {

  private static final int NUM_BROKERS = 1;
  private static final Properties BROKER_CONFIG;

  static {
    BROKER_CONFIG = new Properties();
    BROKER_CONFIG.put("transaction.state.log.replication.factor", (short) 1);
    BROKER_CONFIG.put("transaction.state.log.min.isr", 1);
  }

  @ClassRule
  public static final EmbeddedKafkaCluster CLUSTER =
      new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);
  public String SERVER = null;

  private static volatile int testNo = 0;
  private final Time mockTime = (Time)CLUSTER.time;

  private final String globalStore = "globalStore";
  private final Map<String, String> results = new HashMap<>();
  private StreamsBuilder builder;
  private Properties streamsConfiguration;
  private KafkaStreams kafkaStreams;
  private String globalTableTopic = "globalTableTopic";
  private String streamTopic = "stream";
  private GlobalKTable<Long, String> globalTable;
  private KStream<Long, String> stream;
  private ForeachAction<String, String> foreachAction;

  @Before
  public void before() throws InterruptedException {
    SERVER = CLUSTER.bootstrapServers();
    testNo++;
    createTopics();
    builder = new StreamsBuilder();
    //createTopics();
    streamsConfiguration = new Properties();

    final String applicationId = "globalTableTopic-table-test-" + testNo;
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    streamsConfiguration
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
    streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    globalTable = builder.globalTable(globalTableTopic, Consumed.with(Serdes.Long(), Serdes.String()),
        Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(globalStore)
            .withKeySerde(Serdes.Long())
            .withValueSerde(Serdes.String()));
    final Consumed<Long, String> stringLongConsumed = Consumed.with(Serdes.Long(), Serdes.String());

    stream = builder.stream(streamTopic, stringLongConsumed);

    stream.to(globalTableTopic, Produced.with(Serdes.Long(), Serdes.String()));
  }

  @After
  public void whenShuttingDown() throws IOException {
    if (kafkaStreams != null) {
      kafkaStreams.close();
    }
    IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
  }

  @Test
  public void shouldRestoreTransactionalMessages2() throws Exception {
    produceInitialGlobalTableValues(false);
    startStreams();

    final Map<Long, String> expected = new HashMap<>();
    expected.put(1L, "A");
    expected.put(2L, "G");
    expected.put(3L, "C");
    expected.put(4L, "D");

    TestUtils.waitForCondition(new TestCondition() {
      @Override
      public boolean conditionMet() {
        ReadOnlyKeyValueStore<Long, String> store = null;
        try {
          store = kafkaStreams.store(globalStore, QueryableStoreTypes.<Long, String>keyValueStore());
        } catch (InvalidStateStoreException ex) {
          return false;
        }
        Map<Long, String> result = new HashMap<>();
        Iterator<KeyValue<Long, String>> it = store.all();
        while (it.hasNext()) {
          KeyValue<Long, String> kv = it.next();
          result.put(kv.key, kv.value);
        }
        return result.equals(expected);
      }
    }, 30000L, "waiting for initial values");
    System.out.println("First run");

    kafkaStreams.close();
    System.out.println("Close and restart - should run forever");
    startStreams();

    System.out.println("Test done - shouldn't happen");
    throw new Exception("shouldn't happen");
  }

  private void createTopics() throws InterruptedException {
    streamTopic = "stream-" + testNo;
    globalTableTopic = "globalTable-" + testNo;
    CLUSTER.createTopics(streamTopic);
    CLUSTER.createTopic(globalTableTopic, 2, 1);
  }

  private void startStreams() {
    kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
    kafkaStreams.start();
  }

  private void produceInitialGlobalTableValues(final boolean enableTransactions) throws Exception {
    Properties properties = new Properties();
    if (enableTransactions) {
      properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "someid");
    }
    properties.put(ProducerConfig.RETRIES_CONFIG, 1);
    IntegrationTestUtils.produceKeyValuesSynchronously(
        streamTopic,
        Arrays.asList(
            new KeyValue<>(1L, "A"),
            new KeyValue<>(2L, "G"),
            new KeyValue<>(3L, "C"),
            new KeyValue<>(4L, "D")),
        TestUtils.producerConfig(
            SERVER,
            LongSerializer.class,
            StringSerializer.class,
            properties),
        mockTime,
        enableTransactions);
  }
}
