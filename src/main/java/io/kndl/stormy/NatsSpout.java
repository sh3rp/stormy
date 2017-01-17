package io.kndl.stormy;

import io.nats.client.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pb.Pkt2Bus;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created by shep on 1/14/2017.
 */
public class NatsSpout implements IRichSpout {

    private final Logger logger = LoggerFactory.getLogger(NatsSpout.class);

    private final ConnectionFactory natsConnectionFactory;
    private final String channelName;
    private Connection nats;
    private SyncSubscription sub;

    private SpoutOutputCollector collector;
    private Map conf;
    private TopologyContext ctx;

    private State state;

    public enum State {
        INITIALIZED,CONNECTED,RUNNING,WAITING,DISCONNECTED,STOPPED;
    }

    public NatsSpout(String natsUrl, String channel) {
        this.natsConnectionFactory = new ConnectionFactory(natsUrl);
        this.channelName = channel;
        this.state = State.INITIALIZED;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.conf = conf;
        this.ctx = context;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {
        this.state = State.INITIALIZED;
        try {
            this.nats = natsConnectionFactory.createConnection();
            this.sub = nats.subscribeSync(this.channelName);
            this.state = State.CONNECTED;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deactivate() {
        if(nats != null && !nats.isClosed()) {
            nats.close();
            this.state = State.DISCONNECTED;
        }
    }

    @Override
    public void nextTuple() {
        try {
            this.state = State.RUNNING;
            Message msg = sub.nextMessage();
            logger.debug(msg.getData().toString());
            Pkt2Bus.Packet pkt = Pkt2Bus.Packet.parseFrom(msg.getData());
            logger.warn(pkt.getType().name());
            this.state = State.WAITING;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public State getState() {
        return state;
    }
}
