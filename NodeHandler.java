import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.lang.Math.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.Charset;

public class NodeHandler implements NService.Iface{


	@Override
	public boolean ping() throws TException {return true;}

	//Set status for this map job to false so that the node knows to abort
	@Override
	public void setStatusForMap(int jobNumber){
		NodeServer.mapJobStatus.put(jobNumber, new Boolean(false));
		//System.out.println("Map status set to false for  job: "+ jobNumber);
	}

	//Set status for this merge job to false so the node knows it has to abort
	@Override
	public void setStatusForReduce(List<String> mergeList){
		NodeServer.reduceJobStatus.put(new LinkedList<String>(mergeList), new Boolean(false));
		//System.out.println("Reduce status set to false for a job");
	}

	//This is called by the mapper in the server
	@Override
	public String sortChunk(int chunkNum, int chunkSize ,String fileName){
		NodeServer.mapJobStatus.put(new Integer(chunkNum), new Boolean (true));
		File f = new File(fileName);//Append the path here
		int fileSize= (int)f.length();
		//adjust the offsets
		int offSetStart = (chunkNum-1)*chunkSize;
		int offSetEnd = (chunkNum*chunkSize);
		if(offSetEnd> fileSize)
			offSetEnd=fileSize;

		//System.out.println("offSetStart: " + offSetStart + " offSetEnd: " +offSetEnd);
		int actualOffSetStart= preprocessStart(offSetStart,fileName,fileSize);
		int actualOffSetEnd= preprocessEnd(offSetEnd,fileName,fileSize);
		//System.out.println("actual start and end: " +actualOffSetStart + " " + actualOffSetEnd);
		if(actualOffSetStart!=-1){
			int readlen = actualOffSetEnd-actualOffSetStart+1;

			//read the chunk of file and extract numbers
			FileInputStream fis=null;
			ArrayList<Integer> numbers=null;
			try{
				fis = new FileInputStream(fileName);
				byte[] bs = new byte[chunkSize+5];
				numbers = new ArrayList<Integer>();
				fis.getChannel().position(actualOffSetStart);
				//System.out.println("\nChunksize and readlen is: " + chunkSize + " "  + readlen);
				fis.read(bs,0,readlen);
				//convert  byte to int
				String strNum = new String(bs);
				String[] splitedNums = strNum.split(" ");

				for(String s : splitedNums){
					numbers.add(new Integer(Integer.parseInt(s.trim())));
				}

				fis.close();	
			}catch(Exception ex){
				ex.printStackTrace();
			}

			Collections.sort(numbers);

			StringBuffer newContent = new StringBuffer();
			String delimiter = "";
			for(Integer n : numbers){
				newContent.append(delimiter);
				delimiter = " ";
				newContent.append(n.toString());
			}

			//Checking the status for this filename
			if(NodeServer.mapJobStatus.get(chunkNum)){
				//Write the sorted number into intermediate file	
				File interFile;
				try{
					interFile = new File(fileName + "_0_" + chunkNum); 
					FileOutputStream fOut = new FileOutputStream(interFile, false); 
					byte[] myBytes = newContent.toString().getBytes(); 
					fOut.write(myBytes);
					fOut.close();
				}
				catch(Exception e){
					e.printStackTrace();
				}

				System.out.println("The map job: "+ chunkNum+ " is successfully written");
				NodeServer.mapJobStatus.remove(NodeServer.mapJobStatus.get(new Integer(chunkNum)));
				return fileName + "_0_" + chunkNum;
			}
			//Is status is false, return aborted
			else{

				System.out.println("The map job: "+ chunkNum+ " became redundant");
				return new String("aborted");
			}
		}
		else{
			System.out.println("The intermediate file is empty");
			return new String("dummy");
		}
	}

	int preprocessStart(int offSetStart,String fileName, int fileSize){
		if(offSetStart==0) return offSetStart;

		FileInputStream fis = null;
		char[] cbuf = new char[5];
		byte[] bbuf = new byte[5];
		try {
			fis = new FileInputStream(fileName);
			fis.getChannel().position(offSetStart);
			int cache =5;
			if(fileSize-offSetStart< 5)
				cache = fileSize-offSetStart;
			fis.read(bbuf,0, cache);
			String s = new String(bbuf);
			//System.out.println("For offsetStart: " + offSetStart + " the cbuf is: ***"+ s + "****");
			cbuf = s.toCharArray();
			//char prev=cbuf[0];
			char curr=cbuf[0];
			if(curr==' '){
				return offSetStart+1;
			}
			else{ 
				if(cache==5){
					for(char c : cbuf){
						if(c==' ')
							return offSetStart+1;
						offSetStart++;
					}
				}
			}

			fis.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1; 
	}

	int preprocessEnd(int offSetEnd,String fileName, int fileSize){
		if (offSetEnd+1 == fileSize){
			return offSetEnd;
		}
		FileInputStream fis = null;
		char[] cbuf = new char[5];
		byte[] bbuf = new byte[5];
		try {

			fis = new FileInputStream(fileName);
			fis.getChannel().position(offSetEnd);
			int cache=5;
			if(fileSize-offSetEnd<5)
				cache=fileSize-offSetEnd;

			fis.read(bbuf,0, cache);
			String s = new String(bbuf);
			//System.out.println(s);
			cbuf = s.toCharArray();
			//char prev=cbuf[0];
			char curr=cbuf[0];
			if(curr==' '){
				return offSetEnd-1;
			}
			else{ 
				if(cache<5)
					return fileSize-1;

				for(char c : cbuf){
					if(c==' ')
						return offSetEnd-1;

					offSetEnd++;					}
			}

			fis.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return -1;
	}

	//This is called by the reducer in the server
	@Override
	public String mergeSortedChunks(List<String> filesToMerge, String fileName, int roundNo, int mergeJobNo){

		NodeServer.reduceJobStatus.put(new LinkedList<String>(filesToMerge), new Boolean(true));

		String finalFileName = new String(fileName +"_"+roundNo+"_"+mergeJobNo);
		try{

			// read the 1st number of every file into a map of <min_elemen, filename>
			HashMap<String,Integer>  cacheMap = new HashMap<String,Integer>();
			HashMap<String, Scanner> scannerMap = new HashMap<String, Scanner>();

			for(String file : filesToMerge){
				File f = new File(file);
				Scanner scan = new Scanner(f);
				if(scan.hasNextInt()){ 
					cacheMap.put(file,new Integer(scan.nextInt()));
					scannerMap.put(file,scan);
				}
				else{
					cacheMap.put(file,new Integer (999999));
					scannerMap.put(file,scan);
				}
				//scan.close();
				//f.close();
			}

			//System.out.println("cacheMap size is: " + cacheMap);
			File file = new File(finalFileName);
			if(!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw=new FileWriter(finalFileName);	
			Integer min = new Integer(999999999);
			String delimiter = "";
			while(min != 999999){

				//If the status is false, return the name aborted
				if(!NodeServer.reduceJobStatus.get(filesToMerge)){
					System.out.println("Aborting Reduce JobNumber: " +mergeJobNo);
					NodeServer.reduceJobStatus.remove(NodeServer.reduceJobStatus.get(new LinkedList<String>(filesToMerge)));
					return new String("aborted");
				}

				Set<Integer> numbers = new HashSet<Integer>(cacheMap.values());
				Iterator itr = numbers.iterator(); 
				min = (Integer)itr.next();
				Integer value;
				while (itr.hasNext()) { 
					value=(Integer)itr.next();
					if (min.intValue() > value.intValue()) { 
						min = value;
						System.out.println(min); 
					}
				}
				if(min==999999)
					break;
				fw.write(delimiter);
				fw.write(min.toString());
				String nextFile = new String("ttt");
				for (Map.Entry<String, Integer> e : cacheMap.entrySet()) {
					if(e.getValue().intValue()==min.intValue()){
						nextFile = e.getKey();
					}
				}
				//System.out.println("Inside merge: nextFile : " + nextFile);
				File f = new File(nextFile);	
				Scanner scan = scannerMap.get(nextFile);
				if(scan.hasNextInt()){  
					cacheMap.put(nextFile,new Integer(scan.nextInt()));
					scannerMap.put(nextFile,scan);
				}
				else{
					cacheMap.put(nextFile,new Integer (999999));
					scannerMap.put(nextFile,scan);
				}
				delimiter=" ";
			}
			fw.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
		System.out.println("Reduce Job completed successfully");
		NodeServer.reduceJobStatus.remove(NodeServer.reduceJobStatus.get(new LinkedList<String>(filesToMerge)));
		return finalFileName;
	}	
}
