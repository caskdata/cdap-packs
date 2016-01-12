/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.kafka.flow;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;

/**
 * Provides information about Kafka brokers by watching for changes in ZooKeeper. It is specific for Kafka 0.7
 * ZooKeeper layout (https://cwiki.apache.org/confluence/display/KAFKA/Writing+a+Driver+for+Kafka)
 * <p/>
 * In brief, Kafka 0.7 has the following ZK node structure:
 *
 * <pre>
 * /brokers
 *   /ids
 *     /[broker1_id]
 *     /[broker2_id]
 *   /topics
 *     /[topic_1]
 *       /[broker1_id]
 *     /[topic_2]
 *       /[broker1_id]
 * </pre>
 *
 * <table border="1">
 *   <tr>
 *     <th>Role</th>
 *     <th>ZooKeeper Path</th>
 *     <th>Type</th>
 *     <th>Data Description</th>
 *   </tr>
 *   <tr>
 *     <td>ID Registry</td>
 *     <td><pre>/brokers/ids/[0..N]</pre></td>
 *     <td>Ephemeral</td>
 *     <td>String in the format of "creator:host:port" of the broker.</td>
 *   </tr>
 *   <tr>
 *     <td>Topic Registry</td>
 *     <td><pre>/brokers/topics/[topic]/[0..N]</pre></td>
 *     <td>Ephemeral</td>
 *     <td>Number of partitions that topic has on that Broker.</td>
 *   </tr>
 * </table>
 */
final class KafkaBrokerCache extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaBrokerCache.class);

  private static final String BROKERS_PATH = "/brokers";

  private final ZKClient zkClient;
  private final Map<String, KafkaBroker> brokers;

  // topicBrokers is from topic->partition size->brokerId
  private final Map<String, SortedMap<Integer, Collection<String>>> topicBrokers;
  private final Runnable invokeGetBrokers = new Runnable() {
    @Override
    public void run() {
      getBrokers();
    }
  };
  private final Runnable invokeGetTopics = new Runnable() {
    @Override
    public void run() {
      getTopics();
    }
  };

  KafkaBrokerCache(ZKClient zkClient) {
    this.zkClient = zkClient;
    this.brokers = Maps.newConcurrentMap();
    this.topicBrokers = Maps.newConcurrentMap();
  }

  @Override
  protected void startUp() throws Exception {
    getBrokers();
    getTopics();
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op
  }

  public int getPartitionSize(String topic) {
    SortedMap<Integer, Collection<String>> partitionBrokers = topicBrokers.get(topic);
    if (partitionBrokers == null || partitionBrokers.isEmpty()) {
      return 1;
    }
    return partitionBrokers.lastKey();
  }

  /**
   * Returns a list of {@link KafkaBroker} that contains the given topic and partition. The list
   * is already sorted by the broker id in ascending order.
   */
  public List<KafkaBroker> getBrokers(String topic, int partition) {
    SortedMap<Integer, Collection<String>> partitionBrokers = topicBrokers.get(topic);

    // If there is no broker for the topic partition, return empty list
    if (partitionBrokers == null || partitionBrokers.isEmpty() || partition >= partitionBrokers.lastKey()) {
      return ImmutableList.of();
    }

    List<KafkaBroker> result = new ArrayList<>();
    for (String brokerId : Iterables.concat(partitionBrokers.tailMap(partition + 1).values())) {
      result.add(brokers.get(brokerId));
    }
    Collections.sort(result);
    return result;
  }

  /**
   * Returns a random broker address or {@code null} if none are available.
   */
  public KafkaBroker getRandomBroker() {
    Collection<KafkaBroker> brokers = this.brokers.values();
    if (brokers.isEmpty()) {
      return null;
    }

    return Iterables.get(brokers, new Random().nextInt(brokers.size()), null);
  }

  /**
   * Gets brokerIds (async) and starts watching for changes.
   */
  private void getBrokers() {
    final String idsPath = BROKERS_PATH + "/ids";

    Futures.addCallback(zkClient.getChildren(idsPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (isRunning()) {
          getBrokers();
        }
      }
    }), new ExistsOnFailureFutureCallback<NodeChildren>(idsPath, invokeGetBrokers) {
      @Override
      public void onSuccess(NodeChildren result) {
        Set<String> children = ImmutableSet.copyOf(result.getChildren());
        for (String child : children) {
          getBrokenData(idsPath + "/" + child, child);
        }
        // Remove all removed brokers
        removeDiff(children, brokers);
      }
    });
  }

  /**
   * Gets all topic nodes (async). Also leave a node watch for changes of topics.
   */
  private void getTopics() {
    final String topicsPath = BROKERS_PATH + "/topics";
    Futures.addCallback(zkClient.getChildren(topicsPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (isRunning()) {
          getTopics();
        }
      }
    }), new ExistsOnFailureFutureCallback<NodeChildren>(topicsPath, invokeGetTopics) {
      @Override
      public void onSuccess(NodeChildren result) {
        Set<String> children = ImmutableSet.copyOf(result.getChildren());

        // Process new topic. For existing topic, changes in broker list under it is being watched already.
        for (String topic : Sets.difference(children, topicBrokers.keySet())) {
          getTopicBrokers(topicsPath + "/" + topic, topic);
        }

        // Remove old topics
        removeDiff(children, topicBrokers);
      }
    });
  }

  /**
   * Gets the broker node data. The broker data has the form {@code creator:host:port}.
   *
   * @param path ZK path to the broker node
   * @param brokerId The broker Id
   */
  private void getBrokenData(String path, final String brokerId) {
    Futures.addCallback(zkClient.getData(path), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        // result.getData() shouldn't be null, as it's data written by Kafka when the node was created.
        String data = new String(result.getData(), Charsets.UTF_8);
        // We are only interested in host and port
        Iterator<String> splits = Iterables.skip(Splitter.on(':').split(data), 1).iterator();
        String host = splits.next();
        int port = Integer.parseInt(splits.next());
        brokers.put(brokerId, new KafkaBroker(brokerId, host, port));
      }

      @Override
      public void onFailure(Throwable t) {
        // No-op, the watch on the parent node will handle it.
      }
    });
  }

  /**
   * Gets all broker associated with the given topic. It also starts watching for changes in broker list.
   *
   * @param path ZK path to the topic node
   * @param topic the topic
   */
  private void getTopicBrokers(final String path, final String topic) {
    Futures.addCallback(zkClient.getChildren(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!isRunning()) {
          return;
        }
        // Other event type changes are either could be ignored or handled by parent watcher
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
          getTopicBrokers(path, topic);
        }
      }
    }), new FutureCallback<NodeChildren>() {
      @Override
      public void onSuccess(NodeChildren result) {
        List<String> children = result.getChildren();
        final List<ListenableFuture<BrokerPartition>> futures = new ArrayList<>(children.size());

        // Fetch data (number of partitions) from each broken node and transform it to BrokerPartition
        for (final String brokerId : children) {
          futures.add(Futures.transform(zkClient.getData(path + "/" + brokerId),
                                        new Function<NodeData, BrokerPartition>() {
            @Override
            public BrokerPartition apply(NodeData input) {
              return new BrokerPartition(brokerId, Integer.parseInt(new String(input.getData(), Charsets.UTF_8)));
            }
          }));
        }

        // When all fetching is done, build the partition size->broker map for this topic
        Futures.addCallback(Futures.successfulAsList(futures), new FutureCallback<List<BrokerPartition>>() {
          @Override
          public void onSuccess(List<BrokerPartition> result) {
            TreeMultimap<Integer, String> partitionBrokers = TreeMultimap.create();

            for (BrokerPartition brokerPartition : result) {
              if (brokerPartition == null) {
                // Ignore if getData() on the broker node failed (hence got null result).
                continue;
              }

              partitionBrokers.put(brokerPartition.getPartitionSize(), brokerPartition.getBrokerId());
            }
            topicBrokers.put(topic, partitionBrokers.asMap());
          }

          @Override
          public void onFailure(Throwable t) {
            // This never happens, which is the contract of successfulAsList.
          }
        }, Threads.SAME_THREAD_EXECUTOR);
      }

      @Override
      public void onFailure(Throwable t) {
        // No-op. Failure would be handled by parent watcher already (e.g. node not exists -> children change in parent)
      }
    });
  }

  /**
   * Removes all Map entries that are not present in the provided key Set.
   */
  private <K, V> void removeDiff(Set<K> keys, Map<K, V> map) {
    for (K key : Sets.difference(map.keySet(), keys)) {
      map.remove(key);
    }
  }

  /**
   * A {@link FutureCallback} for ZK operations so that if an operation failed due
   * to {@link KeeperException.Code#NONODE} error, it will watch for the existence of the given node and perform
   * a given action when the node become available. Upon node removal, the node will again be watched for creation
   * and will trigger the given action again once it is created.
   *
   * @param <V> result type of the Future it is listening on.
   */
  private abstract class ExistsOnFailureFutureCallback<V> implements FutureCallback<V> {

    private final String path;
    private final Runnable action;

    protected ExistsOnFailureFutureCallback(String path, Runnable action) {
      this.path = path;
      this.action = action;
    }

    @Override
    public final void onFailure(Throwable t) {
      if (!isNotExists(t)) {
        LOG.error("Operation failed for path {}", path, t);
        return;
      }

      // If failed due to not exist error, watch for creation of the node.
      waitExists(path);
    }

    private boolean isNotExists(Throwable t) {
      return ((t instanceof KeeperException) && ((KeeperException) t).code() == KeeperException.Code.NONODE);
    }

    private void waitExists(final String path) {
      LOG.debug("Path {} not exists. Watch for creation.", path);

      // If the node doesn't exists, use the "exists" call to watch for node creation.
      Futures.addCallback(zkClient.exists(path, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          if (!isRunning()) {
            return;
          }
          if (event.getType() == Event.EventType.NodeCreated) {
            action.run();
          } else if (event.getType() == Event.EventType.NodeDeleted) {
            // If the node is deleted, keep watching it.
            waitExists(path);
          }
        }
      }), new FutureCallback<Stat>() {
        @Override
        public void onSuccess(Stat result) {
          // If path exists on the exists call, execution the action.
          if (result != null) {
            action.run();
          }
        }

        @Override
        public void onFailure(Throwable t) {
          LOG.error("Failed to get existence of path {}", path, t);
        }
      });
    }
  }

  /**
   * A helper POJO to hold brokerId and the number of partitions (of a given topic) for that broker.
   */
  private static final class BrokerPartition {
    private final String brokerId;
    private final int partitionSize;

    private BrokerPartition(String brokerId, int partitionSize) {
      this.brokerId = brokerId;
      this.partitionSize = partitionSize;
    }

    public String getBrokerId() {
      return brokerId;
    }

    public int getPartitionSize() {
      return partitionSize;
    }
  }
}
