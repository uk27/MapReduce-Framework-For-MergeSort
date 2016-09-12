import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.w3c.dom.Node;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.io.*;

public class Server {
	public static ServerHandler handler;
	public static SNService.Processor processor;
	public static List<node> nodeList= new LinkedList<node>();
	public static HashMap<node, Boolean> nodeStatusList =new HashMap<node, Boolean>();

	//To know which nodes are assigned to each mapJob
	public static Map<Integer,Set<node>> mapperJobMap = Collections.synchronizedMap(new HashMap<Integer,Set<node>>());

	//To know which nodes are assigned to each reduceJob
	public static Map<List<String>,Set<node>> reducerJobMap = Collections.synchronizedMap(new HashMap<List<String>,Set<node>>());

	//Lists to store intermediate results from the mapper and reducer respectively
	public static List<String> intermediateFileList= Collections.synchronizedList(new LinkedList<String>());
	public static List<String> intermediateMergelist = Collections.synchronizedList(new LinkedList<String>());
	
   	//Number of map and merge jobs on each node
        public static Map<Integer,Integer> sortJobNode = Collections.synchronizedMap(new HashMap<Integer,Integer>());
        public static Map<Integer,Integer> mergeJobNode = Collections.synchronizedMap(new HashMap<Integer,Integer>());

	//Stores avg map and reduce time on each node id
        public static Map<Integer,Double> avSortTime = Collections.synchronizedMap(new HashMap<Integer,Double>());
        public static Map<Integer,Double> avMergeTime = Collections.synchronizedMap(new HashMap<Integer,Double>());

	//Counters for map threads for good, job and redundant tasks
	public static AtomicInteger goodJobCount = new AtomicInteger();
	public static AtomicInteger badJobCount= new AtomicInteger();
	public static AtomicInteger redundantJobCount= new AtomicInteger(); //Jobs which are killed

	//Counters for threads that that ping to all nodes for status
	public static AtomicInteger dead= new AtomicInteger();
	public static AtomicInteger alive= new AtomicInteger();

	//Counters for merge threads for good, bad and redundant tasks
	public static AtomicInteger goodMergeThread= new AtomicInteger();
	public static AtomicInteger badMergeThread= new AtomicInteger();
	public static AtomicInteger redundantMergeThread= new AtomicInteger();

	public static String fileName = "";
	public static int failProbability=0;
	public static int totalBadJobCount=0;

	//Hardcoded
	public static String SNIp = "csel-x32-01.cselabs.umn.edu";
	public static int SNPort=9097, nTotal, redundantNodes;

	public static int chunkSize, numberOfChunks, mergeJobs;


	//Clear the state after every successful operation i.e. after map and reduce have comppleted
	public static void clearState(){
		mapperJobMap.clear();	
		reducerJobMap.clear();

		intermediateFileList.clear();
		intermediateMergelist.clear();
		goodJobCount.set(0);
		badJobCount.set(0);
		goodMergeThread.set(0);
		badMergeThread.set(0);
		return;
	}


	//Returns a set of x number of random nodes where x is the redundant nodes that user inputs
	public static Set<node> getRandomNodes(){
		Set<node> tempSet= new HashSet<node>();
		while(tempSet.size()!= redundantNodes){
			Random r = new Random();
			tempSet.add(nodeList.get(r.nextInt(nodeList.size())));
		}	
		return tempSet;	
	}

	//Returns true or false based on probability
	public static boolean checkFail(){
		Random r = new Random();
		int Low = 1;
		int High = 100;
		int result = r.nextInt(High-Low) + Low;
		//int result = 100;
		if(result<= failProbability)
			return true;
		return false;
	}

	//The main mapper function
	public static void mapper(LinkedList<Integer> mapJobs, int mapRound){

		//Terminating condition for recursion
		if(mapJobs.size()==0){
			System.out.println("All map jobs completed successfully!\n");
			return;	
		}

		System.out.println("Inside the mapper");
		mapRound++;

		//Clearing the maps for this iteration
		mapperJobMap= Collections.synchronizedMap(new HashMap<Integer,Set<node>>());
		LinkedList<Integer> pendingMapList = new LinkedList<Integer>();

		System.out.println("mapRound: "+ mapRound);

		//Looping over each job
		for(Integer job : mapJobs){

			//Getting the list of nodes for each job
			Set<node> newSet= new HashSet<node>(getRandomNodes());
			mapperJobMap.put(job, newSet);

			//Making RPC for each node for this job
			for(node n : newSet){

				final Integer tempJobNumber= job;
				final node tempNode= n;

				//Create a new thread to call the node nID with (i, chunkSize, fileName)
				try {
					if(!checkFail()){
						//pendingMapList.add(job);
						throw new Exception("A Map operation at Node: "+ tempNode.ip+ ":" + tempNode.id+ " failed because of pobabilistic fault injection");
					}	
					System.out.println("Sending Map job number : "+ tempJobNumber + " to node: " + tempNode.id);
					Runnable sendJob = new Runnable() {
						public void run() {

							//RPC Call
							sendJob(tempNode, tempJobNumber);
						}
					};
					new Thread(sendJob).start();
				} catch (Exception x) {
					//x.printStackTrace();
					System.out.println("MapJob Failure Handled");
					badJobCount.incrementAndGet();
					totalBadJobCount++;
				}

			}

		}	

		//Wait till all threads have stopped executing with either success or failure (Custom join implementation)
		while((goodJobCount.get()+badJobCount.get())+ redundantJobCount.get()!= (mapJobs.size()*redundantNodes)){
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.println("After breaking out of the infinite loop: GoodJobCount: "+ goodJobCount.get() + " badJobCount: "+ badJobCount.get()+ " redundantJobCount: "+ redundantJobCount+ " total map jobs initialized: " + (mapJobs.size()*redundantNodes));

		//If nodes have become inactive
		for(node n: nodeList){
			if(nodeStatusList.get(n)== false){
				nodeStatusList.remove(n);
			}
		}

		//Update nodeList also with the updated list of nodes from nodestatuslist
		nodeList= new LinkedList<node>(nodeStatusList.keySet());

		System.out.println("Number of map jobs pending due to failure: "+ mapperJobMap.size());

		goodJobCount.set(0);
		badJobCount.set(0);
		redundantJobCount.set(0);

		//Storing the contents of the set in a list to make a recursion call
		for (Integer i: mapperJobMap.keySet()){
			pendingMapList.add(i);
		}

		//Recursively calling the mapper
		mapper(pendingMapList, mapRound);

	}//end of mapper

	
	//This function kills calls abort on the jobs on other nodes (that have been already completed by some node)
	public static void killMapJobs(final Integer jobNumber){
		Set<node> tempSet= mapperJobMap.get(jobNumber);
		
		//For each node associated with this job
		for(node tempNode : tempSet){
			if(nodeList.contains(tempNode)){
				try {
					final node n=tempNode;
					Runnable killMapper = new Runnable() {
						public void run() {
							try{
								TTransport transport = new TSocket(n.ip, n.port);
								TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
								NService.Client client = new NService.Client(protocol);
								transport.open();
								//RPC call to abort the map job	
								client.setStatusForMap(jobNumber);
								transport.close();
								//System.out.println("\n Aborting Map jobNumber: "+ jobNumber+ " for node: "+ n.id);
							}catch(TException x){
								System.out.println("Error in creating a connection to kill map job"+ x);
							}
						}
					};
					new Thread(killMapper).start();
				} catch (Exception x) {
					System.out.println("Failed to create a thread kill map job for job: "+ jobNumber+ " for node: "+ tempNode.id);
				}
			}
		}	
	}

	//The function which actually makes the rpc call for a map task	
	protected static void sendJob(node thisNode, Integer jobNumber) {
		try {
			TTransport transport = new TSocket(thisNode.ip, thisNode.port);
			TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
			NService.Client client = new NService.Client(protocol);
			transport.open();
			 long startTimeForSortJob = System.currentTimeMillis();

		     //Result is sorted file for this task who filename is returned
	             String intermediateFile = client.sortChunk(jobNumber, chunkSize, fileName);

	             long endTimeForSortJob = System.currentTimeMillis();
	             sortJobNode.put(thisNode.id, new Integer(sortJobNode.get(thisNode.id).intValue()+1));
        	     avSortTime.put(thisNode.id,new Double((((avSortTime.get(thisNode.id)*(sortJobNode.get(thisNode.id).intValue()-1)) + ((double) endTimeForSortJob-startTimeForSortJob))/sortJobNode.get(thisNode.id).intValue())));
			
			transport.close();
			//If job completed successfuly
			if(intermediateFile != null && !intermediateFile.isEmpty() ){

				//If not aborted 
				if(!intermediateFile.equals("aborted")){

					//This checks if the job had already been deleted from the list by a previous node so redundant
					if(mapperJobMap.get(jobNumber)==null){
						redundantJobCount.incrementAndGet();
						return;
					}

					//Call all the nodes associated with this job and ask them to abort
					killMapJobs(jobNumber);

					//Remove this job from the job list
					mapperJobMap.remove(jobNumber);

					//We are returning a dummy filename if the job did not have any numbers in the file. So it should not be sent for merging
					if(!intermediateFile.equals("dummy") && !intermediateFileList.contains(intermediateFile))
						intermediateFileList.add(intermediateFile);

					goodJobCount.incrementAndGet();
					System.out.println("\n A map job number: "+ jobNumber+ " has executed successfully at node: "+ thisNode.id);			
				}
				//If it was aborted so redundant++
				else {
					redundantJobCount.incrementAndGet();
					System.out.println("\nAbortion successfully handled for jobnumber: "+ jobNumber+ " at node: "+ thisNode.id);
					return;
				}

			}
			//Job failed
			else {
				badJobCount.incrementAndGet();
				totalBadJobCount++;
			}
		} catch (TException x) {
			//x.printStackTrace();
			System.out.println("A map thread failed failed for job: "+ jobNumber+ " at node: "+ thisNode.id);
			badJobCount.incrementAndGet();
			totalBadJobCount++;
		}
	}

	
	public static String reducer(List<String> interFileList, int roundNumber ){

		//Terminating condition for the recursion
		if(interFileList.size()==1){
			System.out.println("All reduce jobs done successfully: "+ interFileList.get(0));

			return (new String (interFileList.get(0)));
		}

		//Determining the number of jobs based on how many are merged each time
		int numberOfMergeJobs= (interFileList.size()%mergeJobs==0 ? (interFileList.size()/mergeJobs) : ((interFileList.size()/mergeJobs)+1));
		roundNumber++;
		LinkedList<String> pendingMergeList = new LinkedList<String>();

		//Making it empty to be populated for this round
		intermediateMergelist= Collections.synchronizedList(new LinkedList<String>());
		reducerJobMap= Collections.synchronizedMap(new HashMap<List<String>,Set<node>>());

		//For each job
		for(int i=0; i< numberOfMergeJobs; i++){

			LinkedList<String> tempList1= new LinkedList<String>();

			//Choosing the tasks for this job
			for(int j=0; j<mergeJobs && ((i*mergeJobs)+j < interFileList.size()); j++)	//Handled the case when remaining jobs less than mergeJobs  
				tempList1.add(interFileList.get((i*mergeJobs)+j));

			//Assigning nodes for this job
			Set<node> newSet= new HashSet<node>(getRandomNodes());
			reducerJobMap.put(tempList1, newSet);

			//For each node associated with the job, create a new thread
			for(node n : newSet){

				final node tempNode= n;
				final LinkedList<String> tempList2= new LinkedList<String>(tempList1);
				final int tempJobNumber= i;
				final int tempRoundNumber= roundNumber;

				//Create a new thread and send job
				try {	//Checking the probability whether this job goes through or not
					if(!checkFail()){
						
						throw new Exception("A Merge operation at Node: "+ tempNode.ip+ ":" + tempNode.id+ " failed because of probabilistic fault injection");
					}

					System.out.println("Sending Reduce job number : "+ tempJobNumber + " to node: " + tempNode.id);

					Runnable sendMergeJob = new Runnable() {
						public void run() {
							sendMergeJob(tempNode, tempList2, tempRoundNumber, tempJobNumber);
							
						}
					};

					new Thread(sendMergeJob).start();
				} catch (Exception x) {
					//x.printStackTrace();
					System.out.println("Problem in merge thread creation itself "+ x);
					badMergeThread.incrementAndGet();
					totalBadJobCount++;
				}

			}
		}	

		//Waiting for all the threads to end. Custom join implementation
		while(goodMergeThread.get()+badMergeThread.get()+ redundantMergeThread.get() != (numberOfMergeJobs*redundantNodes)){

			//System.out.println("GoodMergeThread: "+ goodMergeThread.get() + " badMergeThread: "+ badMergeThread.get()+ " total merge jobs initialized: " + numberOfMergeJobs);

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				//System.out.println("Exception in a sleep");
				e.printStackTrace();
			}
		}

		//Displaying statistics of how many merge threads are good, bad or failed
		System.out.println("After breaking out of the infinite loop: GoodMergeThread: "+ goodMergeThread.get() + " badMergeThread: "+ badMergeThread.get()+ "redundantMergeThread: "+ redundantMergeThread+" total merge jobs initialized: " + numberOfMergeJobs*redundantNodes);

		//Finding out which nodes are still active
		for(node n: nodeList){
				
			if(nodeStatusList.get(n)== false){
				nodeStatusList.remove(n);
			}
		}

		//Update nodeList also with the updated list of nodes from nodestatuslist
		nodeList= new LinkedList<node>(nodeStatusList.keySet());

		System.out.println("Number of merge jobs pending due to failure: "+ reducerJobMap.size());

		System.out.println("Total Number of new files created: "+ intermediateMergelist.size());

		for(List<String> list: reducerJobMap.keySet()){
			intermediateMergelist.addAll(list);
		}

		//Print all the files left
		for(String s: intermediateMergelist)
			System.out.print(s+ " ");
		System.out.println();


		goodMergeThread.set(0);
		badMergeThread.set(0);
		redundantMergeThread.set(0);
		String s1= reducer(intermediateMergelist, roundNumber);
		//System.out.println("In round :"+ roundNumber+" s: "+s1);
		return s1;


	}

	//The function that actually does the RPC call for merge
	protected static void sendMergeJob(node thisNode, LinkedList<String> jobList, Integer roundNumber, Integer jobNumber) {
		try {
			TTransport transport;
			transport = new TSocket(thisNode.ip, thisNode.port);
			TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
			NService.Client client = new NService.Client(protocol);
			transport.open();
			//Send job to this node
			long startTimeForMergeJob = System.currentTimeMillis();
            //Send job to this node
            String intermediateFile = client.mergeSortedChunks(jobList, fileName, roundNumber, jobNumber);
            long endTimeForMergeJob = System.currentTimeMillis();
            mergeJobNode.put(thisNode.id, new Integer(mergeJobNode.get(thisNode.id).intValue()+1));
            avMergeTime.put(thisNode.id,new Double((((avMergeTime.get(thisNode.id)*(mergeJobNode.get(thisNode.id).intValue()-1)) + ((double) endTimeForMergeJob-startTimeForMergeJob))/mergeJobNode.get(thisNode.id).intValue())));

			transport.close();
			//If job completed successfuly
			if(intermediateFile != null && !intermediateFile.isEmpty()){

				//If not returned "aborted"
				if(!intermediateFile.equalsIgnoreCase("aborted")){

					//Checking if this task has already been done and hence removed from the list of jobs to be done
					if(reducerJobMap.get(jobList)==null){
						
						redundantMergeThread.incrementAndGet();
						return;
					}	

					//Send signal to other nodes to abort	
					killReduceJobs(jobList);

					//Remove job num from list of this node
					reducerJobMap.remove(jobList);		

					//Append the file received to the list 
					if(!intermediateMergelist.contains(intermediateFile)){
						intermediateMergelist.add(intermediateFile);
					}
					goodMergeThread.incrementAndGet();
					System.out.println("\n A map reduce number: "+ jobNumber+ " has executed successfully at node: "+ thisNode.id);
				}
				
				//If it returned aborted, then redundant++
				else { 
					redundantMergeThread.incrementAndGet();
					System.out.println("\nAbortion successfully handled for merge job number: "+ jobNumber+ " has been for aborted node: "+ thisNode.id);
				}

			}
			else{
				System.out.println("Reducer returned something null or empty");	 
				badMergeThread.incrementAndGet();
				totalBadJobCount++;
			}
		} catch (TException x) {
			//x.printStackTrace();
			System.out.println("A merge thread failed for job: "+ jobNumber+ " at node: "+ thisNode.id);
			badMergeThread.incrementAndGet();
			totalBadJobCount++;
		}

	}

	//The function that makes RPC calls to other nodes to abort a merge job
	public static void killReduceJobs(final LinkedList<String> jobList){
		Set<node> newSet= new HashSet<node>(reducerJobMap.get(jobList));
		
		//For each node associated with this job
		for(node tempNode: newSet){
			
			//If this node is still active
			if(nodeList.contains(tempNode)){

				try {
					final node n= tempNode;
					Runnable killReducer = new Runnable() {
						public void run() {
							try{
								TTransport transport = new TSocket(n.ip, n.port);
								TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
								NService.Client client = new NService.Client(protocol);
								transport.open();
								//RPC call to abort
								client.setStatusForReduce(jobList);
								transport.close();
								//System.out.println("\n Aborting Reduce job for node: "+ n.id);
							}catch(TException x){
								System.out.println("Error in creating a connection to kill a reduce job");
							}

						}	
					};
					new Thread(killReducer).start();
				} catch (Exception x) {
					System.out.println("Failed to create a thread to kill a reduce job for node: "+ tempNode.id);
				}
			}
		}

	}

	//Searching the generated ID in the nodeList to avoid duplication of ID
	public static boolean searchId(int tempid){
		if(nodeList==null)
			return false;
		for(node n : nodeList){
			if(n.id==tempid)
				return true;
		}
		return false;
	}

	//Getting a random ID between 1 and 31
	public static int getNewId (){
		Random r = new Random();
		int low = 1;
		int high = nTotal;

		return (r.nextInt((high-low)+1) + low);

	}







	public static void main(String [] args) {


		nTotal= Integer.parseInt(args[0]);
		failProbability= Integer.parseInt(args[1]);
		redundantNodes= Integer.parseInt(args[2]);

		//Redundant nodes must be less than or equal to the total nodes
		if(redundantNodes> nTotal){
			System.out.println("Number of redundant nodes should be less than or equal to total nodes");
			return;
		}

		//Starting the server thread
		try {
			handler = new ServerHandler();
			processor = new SNService.Processor(handler);

			Runnable simple = new Runnable() {
				public void run() {
					simple(processor);
				}
			};

			new Thread(simple).start();
		} catch (Exception x) {
			x.printStackTrace();
		}


		//Take an instance for calling the ping function

		try {
			Runnable checkFault = new Runnable() {
				public void run() {
					checkFault();
				}
			};

			new Thread(checkFault).start();
		} catch (Exception x) {
			x.printStackTrace();
		}



	}//end of main



	//The function that ping on the nodes to see which are still active
	protected static void checkFault() {

		// Initializing status of all nodes to be failed
		for (Map.Entry<node, Boolean> entry : nodeStatusList.entrySet()) {
			entry.setValue(false);
		}

		dead.set(0);
		alive.set(0);
		
		//For each node
		for(node n: nodeList){

			final node tempNode=n;
			try {
				Runnable doPing = new Runnable() {
					public void run() {
						boolean status= false;
						//RPC call
						status = doPing(tempNode);
						if(status== true){
							nodeStatusList.put(tempNode, new Boolean(true));
							alive.incrementAndGet();
						}
						else
							dead.incrementAndGet();

					}
				};

				new Thread(doPing).start();
			} catch (Exception x) {
				//x.printStackTrace();
				System.out.println("Failure during creation of a ping thread");
				//Assuming this to be dead just to satify the condition in the while loop
				dead.incrementAndGet();
			}

		}

		//Waiting for all the ping threads to end. Custom join() implementation
		while((dead.get()+alive.get())!= nodeList.size()){
			//Sleep 
			try {
				Thread.sleep(100);
				//System.out.println("Alive: "+ alive.get() + " Dead: "+ dead.get()+ " total: "+ nodeList.size());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Alive: "+ alive.get() + " Dead: "+ dead.get()+ " total: "+ nodeList.size());

		//Recursively calling itself to keep the thread running
		try{
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		checkFault();

	}



	//RPC call to doPing()
	protected static Boolean doPing(node thisNode) {
		// TODO Auto-generated method stub

		//Ping thisnode in try catch
		try {
			TTransport transport;

			transport = new TSocket(thisNode.ip, thisNode.port);

			TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
			NService.Client client = new NService.Client(protocol);

			transport.open();

			Boolean status= client.ping();

			transport.close();

			return status;
		} catch (TException x) {

			//x.printStackTrace();
			System.out.println("Ping to node " + thisNode.id + ":"+ thisNode.port+ " failed");
			return false;

		}


	}




	public static void simple(SNService.Processor processor) {


		try {
			// System.out.println("Before opening socket");
			TServerTransport serverTransport = new TServerSocket(SNPort);
			TTransportFactory factory = new TFramedTransport.Factory();
			TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
			args.processor(processor);
			args.transportFactory(factory);
			// System.out.println("After configuring the port");
			TThreadPoolServer server = new TThreadPoolServer(args);
			System.out.println("Starting the main server...");
			server.serve();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}

