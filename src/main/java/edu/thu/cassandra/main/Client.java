package edu.thu.cassandra.main;

import edu.thu.cassandra.server.GetResult;
import edu.thu.cassandra.util.NodeProbe;
import edu.thu.cassandra.util.task.GetBestStepPolicy;
import edu.thu.cassandra.util.task.GetMaxFlowPolicy;
import edu.thu.cassandra.util.task.TaskAssignInterface;
import edu.thu.cassandra.util.task.TaskAssignResult;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by dyf on 14/05/2017.
 */
public class Client {
    private NodeProbe nodeProbe;

    private Map<String, List<InetAddress>> keyNode = new HashMap<>();

    // 创建任务集合
    private List<FutureTask<ByteBuffer>> taskList = new ArrayList<>();
    // 创建线程池
    ExecutorService exec = Executors.newFixedThreadPool(30);
    public Client(){

    }

    public void setKeyNode(String keyspace, String cf, String key){
        try {
            if(nodeProbe == null)
                nodeProbe = new NodeProbe("127.0.0.1", 7199);
        } catch (IOException e){
            System.out.println(e.getMessage());
        }

        List<InetAddress> endpoints = nodeProbe.getEndpoints(keyspace, cf, key);
        String realKey = keyspace + " " + cf + " " +  key;
        keyNode.put(realKey, endpoints);
    }

    public List<InetAddress> getIfNotExist(String keyspace, String cf, String key){
        String realKey = keyspace + " " + cf + " " +  key;
        if(!keyNode.containsKey(realKey))
            setKeyNode(keyspace, cf, key);
        return keyNode.get(realKey);
    }

    public void getAll(int splitNumber, int time){
        String keyspace = "dyf6";
        String table = "test" + splitNumber;

        Map<String, Integer> ipToId = new HashMap<>();
        Map<Integer, String> idToIp = new HashMap<>();

        Map<Integer, List<Integer>> taskToNode = new HashMap<>();

        for(int i = 1; i <= splitNumber; i++){
            List<InetAddress> addresses = getIfNotExist(keyspace, table, "" + i);
            //System.out.println(addresses);
            List<Integer> list = new ArrayList<>();
            for(InetAddress address: addresses){
                if (ipToId.containsKey(address.getHostAddress())){
                    int ipId = ipToId.get(address.getHostAddress());
                    list.add(ipId);
                }else{
                    int ipId = ipToId.size() + 1;
                    ipToId.put(address.getHostAddress(), ipId);
                    idToIp.put(ipId, address.getHostAddress());

                    list.add(ipId);
                }
            }

            taskToNode.put(i, list);
        }


        List<Integer> nodeStatus = new ArrayList<>();
        for(int i = 0; i< ipToId.size(); i++){
            nodeStatus.add(1);
        }

        TaskAssignInterface taskAssignInterface2 = new GetMaxFlowPolicy();
        TaskAssignResult taskAssignResult2 = taskAssignInterface2.assignTask(ipToId.size(), splitNumber, taskToNode, nodeStatus);
        //System.out.println(taskAssignResult2.cost);
        //System.out.println(taskAssignResult2.assign);


        Long startTime = System.currentTimeMillis();
        CountDownLatch countDownLatch  =  new CountDownLatch(splitNumber) ;
        for(Map.Entry<Integer, Integer> entry: taskAssignResult2.assign.entrySet()){
            int task_id = entry.getKey();
            String ip = idToIp.get(entry.getValue());

            //System.out.println("call:" + task_id + " in ip:" + ip);

            FutureTask<ByteBuffer> ft = new FutureTask<>(
                    () ->  doIt(ip, keyspace, table, ""+task_id, ""+time, 0, -1, countDownLatch)
            );
            exec.submit(ft);
            taskList.add(ft);
        }

        try {
            countDownLatch.await();
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        System.out.println("finish! time:" + (System.currentTimeMillis() - startTime));

    }

    public void getSplice(int splitNumber, int id, int allTime, int startPos, int endPos){
        String keyspace = "dyf6";
        String table = "test" + splitNumber;

        Map<String, Integer> ipToId = new HashMap<>();
        Map<Integer, String> idToIp = new HashMap<>();

        Map<Integer, List<Integer>> taskToNode = new HashMap<>();

        for(int i = 1; i <= allTime; i++){
            List<InetAddress> addresses = getIfNotExist(keyspace, table, "" + id);
            System.out.println(addresses);
            List<Integer> list = new ArrayList<>();
            for(InetAddress address: addresses){
                if (ipToId.containsKey(address.getHostAddress())){
                    int ipId = ipToId.get(address.getHostAddress());
                    list.add(ipId);
                }else{
                    int ipId = ipToId.size() + 1;
                    ipToId.put(address.getHostAddress(), ipId);
                    idToIp.put(ipId, address.getHostAddress());

                    list.add(ipId);
                }
            }

            taskToNode.put(i, list);
        }

        List<Integer> nodeStatus = new ArrayList<>();
        for(int i = 0; i< ipToId.size(); i++){
            nodeStatus.add(1);
        }

        //System.out.println(taskToNode);
        //System.out.println(nodeStatus);

        TaskAssignInterface taskAssignInterface2 = new GetMaxFlowPolicy();
        TaskAssignResult taskAssignResult2 = taskAssignInterface2.assignTask(ipToId.size(), allTime, taskToNode, nodeStatus);
        //System.out.println(taskAssignResult2.cost);
        //System.out.println(taskAssignResult2.assign);


        Long startTime = System.currentTimeMillis();
        CountDownLatch countDownLatch  =  new CountDownLatch(allTime) ;
        for(Map.Entry<Integer, Integer> entry: taskAssignResult2.assign.entrySet()){
            int task_id = entry.getKey();
            String ip = idToIp.get(entry.getValue());

            //System.out.println("call:" + task_id + " in ip:" + ip);

            FutureTask<ByteBuffer> ft = new FutureTask<>(
                    () ->  doIt(ip, keyspace, table, ""+id, ""+task_id, startPos, endPos, countDownLatch)
            );
            exec.submit(ft);
            taskList.add(ft);
        }

        try {
            countDownLatch.await();
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        System.out.println("finish! time:" + (System.currentTimeMillis() - startTime));

    }

    public ByteBuffer doIt(String ip, String keyspace, String cf, String key, String col, int startPos, int endPos, CountDownLatch countDownLatch){
        TTransport transport = null;
        try {
            transport = new TSocket(ip, 9090);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            GetResult.Client client = new GetResult.Client(protocol);

            ByteBuffer bf = client.getResult(keyspace, cf, key, col, startPos, endPos);
            //System.out.println(client.getResult("dyf6", "test100", "1", "1", 16, 24).order(ByteOrder.LITTLE_ENDIAN).getLong());
            countDownLatch.countDown();
            return bf;
        } catch (TException x) {
            x.printStackTrace();
        }finally {
            if (transport != null)
                transport.close();

        }
        return null;
    }

    public static void main(String[] args) {

        //System.out.println(new Client().getIfNotExist("dyf6", "test10", "1"));
//        new Client().getAll(1, 1);
//        new Client().getAll(10, 1);
//        new Client().getAll(20, 1);
//        new Client().getAll(30, 1);
//        new Client().getAll(50, 1);
//        new Client().getAll(100, 1);

        new Client().getSplice(100, 1, 30, 0, 8);
        new Client().getSplice(100, 1, 300, 0, 8);
    }
}
