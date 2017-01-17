package io.kndl.stormy;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.client.SyncSubscription;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import pb.Pkt2Bus;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by shep on 1/14/2017.
 */
public class NatsSpoutTest {

    private SyncSubscription sub;

    @Before
    public void setup() throws IOException, TimeoutException {
        ConnectionFactory f = new ConnectionFactory("nats://192.168.1.204:4222");
        Connection c = f.createConnection();
        this.sub = c.subscribeSync("test");
        c.publish("test","test".getBytes());
    }

    @Test
    public void testConnecting() throws IOException, InterruptedException {
        NatsSpout spout = new NatsSpout("nats://192.168.1.204:4222","test");
        Assert.assertEquals(spout.getState(),NatsSpout.State.INITIALIZED);
        spout.activate();
        Assert.assertEquals(spout.getState(),NatsSpout.State.CONNECTED);
        spout.deactivate();
        Assert.assertEquals(spout.getState(),NatsSpout.State.DISCONNECTED);
    }

    @After
    public void teardown() {
        sub.close();
    }

    private void sendPacket(Pkt2Bus.Packet pkt) {

    }
}
