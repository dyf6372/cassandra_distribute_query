package edu.thu.cassandra.main;

import edu.thu.cassandra.server.GetResult;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import edu.thu.cassandra.server.GetResultHandler;

public class Server {

    public static GetResultHandler handler;

    public static GetResult.Processor processor;

    public static void main(String [] args) {
        try {
            handler = new GetResultHandler();
            processor = new GetResult.Processor(handler);

            Runnable simple = () -> simple(processor);

            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(GetResult.Processor processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(9090);
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

            System.out.println("Starting the simple server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}