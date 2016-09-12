include "Node.thrift"

	service SNService {
		string sortFile(1: string fileName, 2: i32 chunkSize, 3:i32 mergeJobs),
		i32 getId (1: Node.node newnode),
	}
