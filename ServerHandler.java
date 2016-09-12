import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import  java.util.concurrent.TimeUnit;
import java.lang.Math.*;

import org.apache.thrift.transport.*;

import org.apache.thrift.server.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.Charset;


public class ServerHandler implements SNService.Iface
{
	//Interface with the client	
	@Override
	public String sortFile (String fileName, int chunkSize, int mergeJobs ){

		//If there are no nodes in the system, return with handled errorr
		if(Server.nodeList.size()==0)
			return new String ("No Nodes In The System");

		File f = new File(fileName);//Append the path here
		Server.chunkSize= chunkSize;
		Server.mergeJobs= mergeJobs;
		Server.fileName= fileName;

		for(node n : Server.nodeList){
			Server.sortJobNode.put(new Integer(n.id),new Integer(0));
			Server.mergeJobNode.put(new Integer(n.id),new Integer(0));
			Server.avSortTime.put(new Integer(n.id),new Double(0.0));
			Server.avMergeTime.put(new Integer(n.id),new Double(0.0));
		}

		//Initializing the list of map jobs that will be sen to the server
		LinkedList<Integer> mapList= new LinkedList<Integer>();
		double fileSize= f.length();
		//If it is not exactly divisible by chunksize, there will be an extra file to be processes
		if(fileSize%chunkSize == 0.0)
			Server.numberOfChunks= (int) (fileSize/Server.chunkSize);
		else
			Server.numberOfChunks= (int) (fileSize/Server.chunkSize)+1;

		//System.out.println("\n Number of chunks: "+ Server.numberOfChunks);
		for(int i=1; i<= Server.numberOfChunks; i++)
			mapList.add(new Integer(i));

		long startTimeForJob = System.currentTimeMillis();
		System.out.println("\n Size of mapList: "+ mapList.size()+ "\n Calling mapper now");

		//Call mapper()
		Server.mapper(mapList, 0);
		System.out.println("Came outta the mapper and calling reducer");

		//Call reducer()
		String s= Server.reducer(Server.intermediateFileList, 0);
		System.out.println("Reducer returned the file: "+ s);
		System.out.println("Sorted File: " + s );
		
		//Statistics	
		long endTimeForJob = System.currentTimeMillis();
		System.out.println("Time elapsed to do the job: " + (endTimeForJob - startTimeForJob));
		System.out.println("Total faults occured are: " + Server.totalBadJobCount);
		for(node n : Server.nodeList){
		System.out.println("Total Sort Jobs: " + Server.sortJobNode.get(n.id));
		System.out.println("Total Merge Jobs: " + Server.mergeJobNode.get(n.id));
		System.out.println("Total Sort Jobs: " + Server.avSortTime.get(n.id));
		System.out.println("Total Sort Jobs: " + Server.avMergeTime.get(n.id));
		}

		//Clearing the state of server for future requests
		Server.clearState();
		return new String (s);		
	}

	//Server sends a new id to each compute node
	@Override
	public int getId (node newnode) throws TException {
		int tempid= Server.getNewId();
		while(Server.searchId(tempid)){
			tempid= Server.getNewId();
		}

		newnode.id= tempid;
		Server.nodeList.add(newnode);
		Server.nodeStatusList.put(newnode, new Boolean(true));
		return newnode.id;
	}	
}
