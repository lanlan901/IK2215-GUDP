import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class GUDPSocket implements GUDPSocketAPI {
    final LinkedList<GUDPEndPoint> sendList;
    final LinkedList<GUDPEndPoint> receiveList;
    private final Map<GUDPEndPoint, Boolean> timerStatusMap = new ConcurrentHashMap<>();
    public boolean isReceive = false;
    public boolean isRunning = true;
    public BlockingQueue<GUDPPacket> receiveQueue = new LinkedBlockingQueue<>();
    DatagramSocket datagramSocket;
    Thread threadSend;
    Thread threadReceive;
    Thread threadACK;
    SendThread sendThread;ReceiveThread receiveThread;
    AckThread ackThread;
    recThread recThread = new recThread();
    public GUDPSocket(DatagramSocket socket) {
        datagramSocket = socket;
        //instantiate send and receive lists;
        sendList = new LinkedList<>();
        receiveList = new LinkedList<>();
        //instantiate send and receive threads;
        sendThread = new SendThread(sendList);
        receiveThread = new ReceiveThread(receiveList);
        ackThread = new AckThread(sendList);

        threadSend = new Thread(sendThread);
        threadReceive = new Thread(receiveThread);
        threadACK = new Thread(ackThread);
    }


    public void send(DatagramPacket packet) throws IOException {
        //create a new end point if it doesn't exist
        GUDPEndPoint sendEndPoint = getEndPoint(sendList, packet.getAddress(), packet.getPort());
        if (sendEndPoint == null) {
            sendEndPoint = new GUDPEndPoint(packet.getAddress(), packet.getPort());
            synchronized (sendList) {
                sendList.add(sendEndPoint);
            }
        }
        //start sending thread and ack thread
        if (!threadSend.isAlive()) {
            threadSend.setName("threadSend");
            threadSend.start();
            System.out.println("thread for send start");
        }
        if (!threadACK.isAlive()) {
            threadACK.setName("threadACK");
            threadACK.start();
            System.out.println("thread for ACK start");
        }
        synchronized (sendList) {
            GUDPPacket gudpPacket;
            switch (sendEndPoint.getState()) {
                case INIT:
                    System.out.println("send INIT");
                    sendEndPoint.setState(GUDPEndPoint.endPointState.BSN);
                case BSN:
                    System.out.println("send thread: BSN");
                    /*create new BSN GUDP packet and put it in the buffer list */
                    ByteBuffer buffer = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
                    buffer.order(ByteOrder.BIG_ENDIAN);
                    gudpPacket = new GUDPPacket(buffer);
                    gudpPacket.setType(GUDPPacket.TYPE_BSN);
                    gudpPacket.setVersion(GUDPPacket.GUDP_VERSION);
                    gudpPacket.setSocketAddress((InetSocketAddress) packet.getSocketAddress());
                    gudpPacket.setPayloadLength(0);

                    //create a sequence number:
                    Random rand = new Random();
                    int seqNumber = rand.nextInt(Short.MAX_VALUE);
                    gudpPacket.setSeqno(seqNumber);
                    sendEndPoint.setNextseqnum(seqNumber);
                    sendEndPoint.setBase(seqNumber);
                    sendEndPoint.setLast(seqNumber);
                    sendEndPoint.add(gudpPacket);
                    System.out.println("The sequence number of sending packet is :" + seqNumber);
                    sendEndPoint.setState(GUDPEndPoint.endPointState.READY);
                    sendList.notify();

                case READY:
                    System.out.println("ready to send the packet with sequence number:" + (sendEndPoint.getLast() + 1));
                    gudpPacket = GUDPPacket.encapsulate(packet);
                    gudpPacket.setSeqno(sendEndPoint.getLast() + 1);
                    sendEndPoint.setLast(sendEndPoint.getLast() + 1);
                    sendEndPoint.add(gudpPacket);
                    break;

                case MAXRETRIED:
                    throw new IOException("send() ERROR: MAX RETRIED ON SEND END POINT "
                            + sendEndPoint.getRemoteEndPoint().getAddress()
                            + ": " + sendEndPoint.getRemoteEndPoint().getPort());
                case FINISHED:
                    System.err.println("FINISHED cannot send more packets!"
                            + sendEndPoint.getRemoteEndPoint().getAddress()
                            + ": " + sendEndPoint.getRemoteEndPoint().getPort());
                    break;

                case CLOSED:
                    System.err.println("CLOSED cannot send more packets!"
                            + sendEndPoint.getRemoteEndPoint().getAddress()
                            + ": " + sendEndPoint.getRemoteEndPoint().getPort());
                    break;
            }
        }
    }

    public void receive(DatagramPacket packet) throws IOException {
        if (!isReceive) {
            recThread.start();
            isReceive = true;
        }
        GUDPPacket gudpPacket;
        try {
            gudpPacket = receiveQueue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        gudpPacket.decapsulate(packet);
    }

    public void finish() throws IOException {
        System.out.println("finish() start");
        //create FIN packet for all end points
        //wait for sendList to finish sending or timeout before return to application
        while (sendList.size() > 0) {
            int i = 0;
            //iterate through all send end points
            while (i < sendList.size()) {
                if (sendList.get(i).getState() == GUDPEndPoint.endPointState.MAXRETRIED) {
                    throw new IOException("send ERROR : MAX RETRIED ON SEND END POINT" + sendList.get(i).getRemoteEndPoint().getAddress()
                            + ":" + sendList.get(i).getRemoteEndPoint().getPort());
                }
                if ((sendList.get(i).getBase() >= sendList.get(i).getLast())
                        && (sendList.get(i).getState() != GUDPEndPoint.endPointState.FINISHED)) {
                    //base > last implies that sending is finished and we can remove send end point
                    System.out.println("FINISHED SEND END POINT:" + sendList.get(i).getRemoteEndPoint().getAddress()
                            + ":" + sendList.get(i).getRemoteEndPoint().getPort());
                    sendList.get(i).setState(GUDPEndPoint.endPointState.FINISHED);
                    GUDPEndPoint gudpEndPoint;
                    for (int j = 0; j < sendList.size(); j++) {
                        gudpEndPoint = sendList.get(j);
                        ByteBuffer buffer = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
                        buffer.order(ByteOrder.BIG_ENDIAN);
                        GUDPPacket gudpPacket = new GUDPPacket(buffer);
                        gudpPacket.setVersion(GUDPPacket.GUDP_VERSION);
                        gudpPacket.setSocketAddress(gudpEndPoint.getRemoteEndPoint());
                        gudpPacket.setPayloadLength(0);
                        int seqNumber = gudpEndPoint.getNextseqnum();
                        gudpPacket.setSeqno(seqNumber);
                        gudpEndPoint.setLast(gudpEndPoint.getLast() + 1);
                        gudpEndPoint.add(gudpPacket);
                        System.out.println("finish");
                    }
                } else {
                    i++;
                }
                //slow down the main thread by putting it to sleep half a second in each iteration
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void close() throws IOException {
        System.out.println("thread close");
        System.exit(0);
        synchronized (sendList) {
            sendThread.stopSendThread();
            sendList.notifyAll();
        }
        synchronized (sendList) {
            ackThread.stopAckThread();
            sendList.notifyAll();
        }
        synchronized (receiveList) {
            receiveThread.stopReceiveThread();
            receiveList.notifyAll();
        }
        datagramSocket.close();
    }

    public GUDPEndPoint getEndPoint(LinkedList<GUDPEndPoint> gudpEndPoints, InetAddress address, int port) {
        synchronized (gudpEndPoints) {
            for (int i = 0; i < gudpEndPoints.size(); i++) {
                if (gudpEndPoints.get(i).getRemoteEndPoint().getAddress().equals(address) && gudpEndPoints.get(i).getRemoteEndPoint().getPort() == port) {
                    return gudpEndPoints.get(i);
                }
            }
            return null;
        }
    }

    public void processSend(GUDPEndPoint gudpEndPoint) {
        synchronized (gudpEndPoint) {
            DatagramPacket udppacket;
            switch (gudpEndPoint.getEvent()) {
                case WAIT:
                    //do nothing
                    break;
                case INIT:
                    //do nothing and proceed to the next state
                case SEND:
                    synchronized (sendList) {
                        while ((gudpEndPoint.getNextseqnum() - gudpEndPoint.getBase() <= gudpEndPoint.getWindowSize())
                                && (gudpEndPoint.getNextseqnum() <= gudpEndPoint.getLast())) {
                            try {
                                udppacket = gudpEndPoint.getPacket(gudpEndPoint.getNextseqnum()).pack();
                                datagramSocket.send(udppacket);
                                System.out.println("processSend send sequence Number :" + gudpEndPoint.getNextseqnum());
                            } catch (IOException e) {
                                System.err.println("IOException in sendFSM: SEND");
                                e.printStackTrace();
                            }
                            if (gudpEndPoint.getNextseqnum() - gudpEndPoint.getBase() == gudpEndPoint.getWindowSize()) {
                                System.out.println("start timer");
                                gudpEndPoint.startTimer();
                                sendList.notifyAll();
                            }
                            gudpEndPoint.setNextseqnum(gudpEndPoint.getNextseqnum() + 1);
                        }
                    }
                    if ((gudpEndPoint.getBase() > gudpEndPoint.getLast())
                            && (gudpEndPoint.getState() == GUDPEndPoint.endPointState.FINISHED)) {
                        gudpEndPoint.stopTimer();
                    }
                    break;

                case TIMEOUT:
                    System.out.println("process send : TIMEOUT");
                    if (gudpEndPoint.getRetry() >= gudpEndPoint.getMaxRetry()) {
                        gudpEndPoint.setState(GUDPEndPoint.endPointState.MAXRETRIED);
                        System.err.println("MAX RETRANSMISSION TIMEOUT: " + gudpEndPoint.getMaxRetry());
                        System.err.println("Terminate sending");
                        try {
                            close();
                        } catch (IOException e) {
                            System.err.println("IOException error in processSend: TIMEOUT");
                            e.printStackTrace();
                        }
                        break;
                    }
                    gudpEndPoint.startTimer();
                    int retry = gudpEndPoint.getBase();
                    int last;
                    if (gudpEndPoint.getLast() - retry < gudpEndPoint.getWindowSize()) {
                        last = gudpEndPoint.getLast();
                    } else {
                        last = retry + gudpEndPoint.getWindowSize();
                    }
                    while (retry <= last) {
                        try {
                            udppacket = gudpEndPoint.getPacket(retry).pack();
                            datagramSocket.send(udppacket);
                            System.out.println("Start resend with sequence number:" + retry);
                            if (retry > gudpEndPoint.getNextseqnum()) {
                                gudpEndPoint.setNextseqnum(retry);
                            }
                        } catch (IOException e) {
                            System.err.println("IOException error in processSend: TIMEOUT");
                            e.printStackTrace();
                        }
                        retry++;
                    }
                    gudpEndPoint.setRetry(gudpEndPoint.getRetry() + 1);
                    gudpEndPoint.setEvent(GUDPEndPoint.readyEvent.SEND);
                    break;
                //if all send are acked, stop timer;
                //otherwise reset the timer
                case RECEIVE:
                    if (gudpEndPoint.getBase() == gudpEndPoint.getNextseqnum()) {
                        gudpEndPoint.stopTimer();
                        gudpEndPoint.setRetry(0);
                        System.out.println("process send: RECEIVE");
                    } else {
                        safelyStopTimer(gudpEndPoint);
                        safelyStartTimer(gudpEndPoint);
                    }
                    gudpEndPoint.setEvent(GUDPEndPoint.readyEvent.SEND);
                    break;
            }
        }
    }

    private synchronized void safelyStartTimer(GUDPEndPoint endPoint) {
        if (!timerStatusMap.getOrDefault(endPoint, false)) {
            endPoint.startTimer();
            timerStatusMap.put(endPoint, true);
        }
    }

    private synchronized void safelyStopTimer(GUDPEndPoint endPoint) {
        if (timerStatusMap.getOrDefault(endPoint, false)) {
            endPoint.stopTimer();
            timerStatusMap.put(endPoint, false);
        }
    }

    public class SendThread extends Thread {
        private final LinkedList<GUDPEndPoint> sendList;
        private boolean runFlag = true;

        public SendThread(LinkedList<GUDPEndPoint> sendList) {
            this.sendList = sendList;
        }

        public void stopSendThread() {
            runFlag = false;
        }

        @Override
        public void run() {
            System.out.println("Start sending thread");
            while (this.runFlag) {
                synchronized (sendList) {
                    if (sendList.size() == 0) {
                        //wait for application to send packet
                        try {
                            sendList.wait();
                        } catch (InterruptedException e) {
                            System.err.println("sendThread interrupted probably because of socket closed");
                            e.printStackTrace();
                        }
                    }
                    //iterate through each end point in sendList
                    int i = 0;
                    while ((i < sendList.size()) && (sendList.size() > 0) && (this.runFlag)) {
                        GUDPEndPoint gudpEndPoint = sendList.get(i);
                        switch (gudpEndPoint.getState()) {
                            case INIT:
                                //do nothing
                                break;
                            case BSN:
                                System.out.println("sendthread : BSN");
                                // do nothing
                                break;
                            case MAXRETRIED:
                                System.err.println("RETRY:" + gudpEndPoint.getRetry()
                                        + "reaches MAX TIMEOUT: " + gudpEndPoint.getMaxRetry());
                                gudpEndPoint.setState(GUDPEndPoint.endPointState.FINISHED);
                                break;
                            case FINISHED:
                                if (gudpEndPoint.isEmptyBuffer()) {
                                    System.out.println("移除发送endpoint"
                                            + sendList.get(i).getRemoteEndPoint().getAddress()
                                            + ":" + sendList.get(i).getRemoteEndPoint().getPort());
                                    sendList.remove(gudpEndPoint);
                                    continue;
                                }
                            case READY:
                                processSend(gudpEndPoint);
                                break;
                            case CLOSED:
                                break;
                        }
                        i++;
                    }
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public class AckThread extends Thread {
        private final LinkedList<GUDPEndPoint> sendList;
        private boolean runFlag = true;
        public AckThread(LinkedList<GUDPEndPoint> sendList) {
            this.sendList = sendList;
        }
        public void stopAckThread() {
            runFlag = false;
        }

        @Override
        public void run() {
            System.out.println("Start ack thread");
            byte[] buffer = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
            DatagramPacket udpPacket = new DatagramPacket(buffer, buffer.length);
            while (this.runFlag) {
                GUDPPacket gudppacket;
                GUDPEndPoint gudpEndPoint;
                try {
                    datagramSocket.receive(udpPacket);
                    gudppacket = GUDPPacket.unpack(udpPacket);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                InetSocketAddress socketAddress = gudppacket.getSocketAddress();
                int seqNumber = gudppacket.getSeqno();
                if (gudppacket.getType() == GUDPPacket.TYPE_ACK) {
                    gudpEndPoint = null;
                    for (int i = 0; i < sendList.size(); i++) {
                        InetSocketAddress inetSocketAddress = sendList.get(i).getRemoteEndPoint();
                        if (inetSocketAddress.equals(socketAddress)) {
                            gudpEndPoint = sendList.get(i);
                            break;
                        }
                    }
                    if (gudpEndPoint == null) {
                        System.err.println("no endpoint");
                    } else {
                        synchronized (sendList) {
                            gudpEndPoint.removeAllACK(seqNumber - 1);
                            gudpEndPoint.setBase(seqNumber);
                            System.out.println("ACK sequence number:" + seqNumber);
                            gudpEndPoint.setEvent(GUDPEndPoint.readyEvent.RECEIVE);
                            processSend(gudpEndPoint);
                            sendList.notifyAll();
                        }
                    }
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public class ReceiveThread extends Thread {
        private final LinkedList<GUDPEndPoint> receiveList;
        private boolean runFlag = true;

        public ReceiveThread(LinkedList<GUDPEndPoint> receiveList) {
            this.receiveList = receiveList;
        }

        public void stopReceiveThread() {
            runFlag = false;
        }

        @Override
        public void run() {
            System.out.println("The receive Thread has started");
            byte[] buffer = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
            DatagramPacket udppacket = new DatagramPacket(buffer, buffer.length);
            while (this.runFlag) {
                GUDPPacket gudpPacket;
                GUDPEndPoint gudpEndPoint;
                try {
                    System.out.println("Waiting for packet...");
                    datagramSocket.receive(udppacket);
                    System.out.println("Packet received!");
                    gudpPacket = GUDPPacket.unpack(udppacket);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                InetSocketAddress socketAddress = gudpPacket.getSocketAddress();
                int seqNumber = gudpPacket.getSeqno();
                switch (gudpPacket.getType()) {
                    case GUDPPacket.TYPE_BSN:
                        synchronized (receiveList) {
                            System.out.println("receive thread: BSN");
                            gudpEndPoint = getEndPoint(receiveList, socketAddress.getAddress(), socketAddress.getPort());
                            if (gudpEndPoint == null) {
                                gudpEndPoint = new GUDPEndPoint(socketAddress.getAddress(), socketAddress.getPort());
                                gudpEndPoint.setExpectedseqnum(seqNumber + 1);
                                gudpEndPoint.add(gudpPacket);
                                receiveList.add(gudpEndPoint);
                                receiveList.notifyAll();
                                System.out.println("BSN sequence Number: " + gudpPacket.getSeqno());
                            } else {
                                gudpEndPoint.removeAll();
                            }
                        }
                        sendACK(gudpPacket);
                        System.out.println("send ACK sequence Number: " + seqNumber);
                        break;
                    case GUDPPacket.TYPE_DATA:
                        gudpEndPoint = getEndPoint(receiveList, socketAddress.getAddress(), socketAddress.getPort());
                        if (gudpEndPoint == null) {
                            System.err.println("WARN: DATA arrives before receiving a BSN: DATA DISCARDED");
                            break;
                        }
                        if ((seqNumber == gudpEndPoint.getExpectedseqnum())
                                && (gudpEndPoint.getState() == GUDPEndPoint.endPointState.READY)
                        ) {
                            synchronized (receiveList) {
                                gudpEndPoint.add(gudpPacket);
                                gudpEndPoint.setExpectedseqnum(gudpEndPoint.getExpectedseqnum() + 1);
                                receiveList.notifyAll();

                            }
                            sendACK(gudpPacket);
                            System.out.println("sendACK sequence number" + seqNumber);
                        }
                        break;
                    case GUDPPacket.TYPE_ACK:
                        break;
                    case GUDPPacket.TYPE_FIN:
                        gudpEndPoint = getEndPoint(receiveList, socketAddress.getAddress(), socketAddress.getPort());
                        if (gudpEndPoint == null) {
                            System.err.println("WARN: FIN arrives before receiving a BSN: DATA DISCARDED");
                            break;
                        }
                        gudpEndPoint.setState(GUDPEndPoint.endPointState.FINISHED);
                        sendACK(gudpPacket);
                        System.out.println("FINISH received");
                        break;
                }
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public class recThread extends Thread {
        private final List<Queue<GUDPPacket>> gudpBufferQueue = new ArrayList<>();
        public int index = -1;
        public int expectSeqNum = 0;

        @Override
        public void run() {
            while (isRunning) {
                try {
                    byte[] buf = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
                    DatagramPacket udpPacket = new DatagramPacket(buf, buf.length);
                    datagramSocket.receive(udpPacket);
                    GUDPPacket gudppacket = GUDPPacket.unpack(udpPacket);
                    processPacket(gudppacket);
                    processReceivedQueue();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        private void processPacket(GUDPPacket gudpPacket) throws IOException {
            // Ensure a PriorityQueue is present in gudpBufferQueue at the desired index
            if (index < 0 || index >= gudpBufferQueue.size()) {
                gudpBufferQueue.add(new PriorityQueue<>(Comparator.comparingInt(GUDPPacket::getSeqno)));
                index = gudpBufferQueue.size() - 1;  // Ensuring the index points to the last added Queue
            }
            Queue<GUDPPacket> currentRecQueue = gudpBufferQueue.get(index);
            if (gudpPacket.getType() == GUDPPacket.TYPE_BSN) {
                expectSeqNum = gudpPacket.getSeqno() + 1;
                currentRecQueue.add(gudpPacket);
                sendACK(gudpPacket);
            } else if (gudpPacket.getType() == GUDPPacket.TYPE_DATA && gudpPacket.getSeqno() == expectSeqNum) {
                currentRecQueue.add(gudpPacket);
                sendACK(gudpPacket);
            }else if (gudpPacket.getType() == GUDPPacket.TYPE_FIN){
                sendACK(gudpPacket);
            }
        }

        private void processReceivedQueue() {
            // Ensure the index is valid before getting the Queue
            if (index < 0 || index >= gudpBufferQueue.size()) {
                return;
            }
            Queue<GUDPPacket> currentRecQueue = gudpBufferQueue.get(index);
            while (true) {
                GUDPPacket gudpPacket = currentRecQueue.peek();
                if (gudpPacket == null) {
                    break;
                }
                int seqNo = gudpPacket.getSeqno();
                if (seqNo < expectSeqNum) {
                    currentRecQueue.remove();
                    continue;
                }
                receiveQueue.add(gudpPacket);
                currentRecQueue.remove();
                expectSeqNum++;
            }
        }
    }
    public void sendACK(GUDPPacket gudpPacket) {
        ByteBuffer ackBuffer = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
        ackBuffer.order(ByteOrder.BIG_ENDIAN);
        GUDPPacket AckPacket = new GUDPPacket(ackBuffer);
        AckPacket.setVersion(GUDPPacket.GUDP_VERSION);
        AckPacket.setType(GUDPPacket.TYPE_ACK);
        AckPacket.setSeqno(gudpPacket.getSeqno() + 1);
        AckPacket.setPayloadLength(0);
        InetSocketAddress socketAddress = gudpPacket.getSocketAddress();
        InetSocketAddress ackSocketAddress = new InetSocketAddress(socketAddress.getAddress(), socketAddress.getPort());
        AckPacket.setSocketAddress(ackSocketAddress);
        DatagramPacket udpPacket;
        try {
            udpPacket = AckPacket.pack();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            datagramSocket.send(udpPacket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("ACK sequence number: " + AckPacket.getSeqno());
    }
}







