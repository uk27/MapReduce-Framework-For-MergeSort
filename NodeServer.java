import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import  java.util.concurrent.TimeUnit;
import java.lang.Math.*;

/*
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
 */
import org.apache.thrift.transport.*;

import org.apache.thrift.server.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.io.*;

public class NodeServer {

	public static node myNode= new node();
	public static String SuperNodeHost= Server.SNIp;	//"csel-x32-01.cselabs.umn.edu";
	public static int SuperNodePort= Server.SNPort;	
	public static NodeHandler handler;
	public static NService.Processor processor;
	public static boolean running = true;

	public static Map <Integer, Boolean> mapJobStatus = Collections.synchronizedMap(new HashMap<Integer, Boolean>());
	public static Map <LinkedList<String>, Boolean> reduceJobStatus = Collections.synchronizedMap(new HashMap <LinkedList<String>, Boolean>());
 
	
	public static void joinNetwork() {
		try {
			TTransport transport= new TSocket(SuperNodeHost, SuperNodePort);
			TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
			SNService.Client client = new SNService.Client(protocol);
			transport.open();
			perform(client);
			transport.close();
		} catch (TException x) {
			x.printStackTrace();
		}
	}

	//We are calling function of the supernode for Join
	private static void perform(SNService.Client client) throws TException{
		myNode.id = client.getId(myNode);
		System.out.println("Starting a server with Id "+ myNode.id);
	}

	public static void main(String [] args){
		SuperNodePort= Server.SNPort;
		try{
			myNode.ip= InetAddress.getLocalHost().getHostAddress();
		}
		catch (UnknownHostException e){
			e.printStackTrace();
		}
		myNode.port= Integer.parseInt(args[0]);

		//Join request to the supernode through perform
		joinNetwork();
		//Now we start our server
		try {
			handler = new NodeHandler();
			processor = new NService.Processor(handler);
			Runnable simple = new Runnable() {
				public void run() {
					simple(processor);
				}
			};
			new Thread(simple).start();
		} catch (Exception x) {
			x.printStackTrace();
		}
	}


	public static void simple(NService.Processor processor) {
		try {
			TServerTransport serverTransport = new TServerSocket(myNode.port);
			TTransportFactory factory = new TFramedTransport.Factory();  
			TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);  
			args.processor(processor);  
			args.transportFactory(factory); 
			TThreadPoolServer server = new TThreadPoolServer(args);
			server.serve();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

