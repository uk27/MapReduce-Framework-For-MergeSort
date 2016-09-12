import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.lang.Math.*;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.nio.file.*;
import java.nio.charset.*;

public class Client {

	public static void main(String args[]) {
		int chunkSize= Integer.parseInt(args[0]);
		int mergeJobs= Integer.parseInt(args[1]);
		String fileName = args[2];

		//convert chunkSizeInMB in Bytes
		String sortedFileName	= "";
		try {
			//To be changes to the IP and Port of the Super Node      
			TTransport transport= new TSocket("csel-x32-01.cselabs.umn.edu", Server.SNPort);
			TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
			SNService.Client client = new SNService.Client(protocol);
			transport.open();
			String actualOutput =  client.sortFile(fileName,chunkSize,mergeJobs);
			System.out.println("Processed File: "+ actualOutput);
			transport.close();
		} catch (TException x) {
			x.printStackTrace();
		}
	}

}
