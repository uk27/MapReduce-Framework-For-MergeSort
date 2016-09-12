include "Node.thrift"

	service NService {
  		bool ping(),
		string sortChunk(1: i32 chunkNum, 2: i32 offset,3: string fileName),
		string mergeSortedChunks(1: list<string> filesToMerge,2: string fileName,3: i32 roundNo,4: i32 mergeJobNo),
		void setStatusForMap(1: i32 jobNumber),
		void setStatusForReduce(1: list<string> mergeList),
	}
