/**
 * 
 */
package sk.emandem.pipeline.example;

import java.util.Map;

import sk.emandem.pipeline.core.Workflow;
import sk.emandem.pipeline.core.pipe.IPipe;
import sk.emandem.pipeline.core.pipe.SynchronousPipe;
import sk.emandem.pipeline.core.worker.Worker;

/**
 * @author Michal Antolik (michal@emandem.sk)
 *
 */
public class SyncPipeMultiProcessorExample {

	public static void main(String[] args) throws Exception{
		// a workflow reader, which creates data 
		// and will send them through pipeline
		//Worker dataReader = createReader();
		int numberOfReaderProcessors = 5;
		Worker[] readerProcessors = new Worker[numberOfReaderProcessors];
		for(int i=0;i<numberOfReaderProcessors;i++){
			readerProcessors[i]=createReader();
		}
		
		// a workflow processors, each one will be executed in
		// a separate thread, each one will receive data as
		// they become available from the reader
		int numberOfProcessors = 8;
		Worker[] processors = new Worker[numberOfProcessors];
		for(int i=0;i<numberOfProcessors;i++){
			processors[i]=createProcessor();
		}
		
		// a workflow writer, in this case it collects all processed data
		// and it will iteratively create statistics from them
		//Worker writer = createWriter();
		
		int numberOfWriterProcessors = 4;
		Worker[] writerProcessors = new Worker[numberOfWriterProcessors];
		for(int i=0;i<numberOfWriterProcessors;i++){
			writerProcessors[i]=createWriter();
		}
		
		// create pipeline similar to UNIX pipeline
		IPipe readerPipe = new SynchronousPipe().addInputs(readerProcessors).addOutputs(processors);
		IPipe writerPipe = new SynchronousPipe().addInputs(processors).addOutputs(writerProcessors);
		
		// register pipes and execute workflow
		Workflow workflow = new Workflow();
		workflow.registerPipes(readerPipe, writerPipe);
		// do blocking execution
		workflow.execute();
	}
	
	private static Worker createReader(){
		return new Worker() {
			int count =0;
			
			@Override
			public void open() throws Exception {
				System.out.println("Entering Reader ... ");
			}
			
			@Override
			protected void doJob(Object data, Map<String, IPipe> outPipes)
					throws Exception {
				int max=1000;
				for(int i=0; i< max;i++){
					double x = Math.random();
					//System.out.println("Input - " + x);
					count++;
					outPipes.get(DEFAULT_PIPE_TYPE).send(new DataToken(x));
				}
			}
			
			@Override
			public void close() throws Exception {
				System.out.println("Exiting Reader ... processing count : " + count);
				count = 0;
			}
		};
	}
	
	private static Worker createProcessor(){
		return new Worker() {
			int count =0;
			
			@Override
			public void open() throws Exception {
				System.out.println("Entering Processor ... ");
			}
			
			@Override
			protected void doJob(Object data, Map<String, IPipe> outPipes)
					throws Exception {
				DataToken dataToken = (DataToken)data;
				dataToken.setToken(Math.floor(dataToken.getToken()*10));
				count++;
				outPipes.get(DEFAULT_PIPE_TYPE).send(dataToken);
			}
			
			@Override
			public void close() throws Exception {
				System.out.println("Exiting Processor ... processing count : " + count);
				count = 0;
			}
		};
	}
	
	private static Worker createWriter(){
		return new Worker() {
			private final int range=10;
			int[] receivedData = new int[range];
			int count =0;
			
			@Override
			public void open() throws Exception {
				System.out.println("Entering Writer ... ");
			}
			
			@Override
			protected void doJob(Object data, Map<String, IPipe> outPipes)
					throws Exception {
				DataToken dataToken = (DataToken)data;
				receivedData[(int)dataToken.getToken()]++;
				count++;
			}
			
			@Override
			public void close() throws Exception {
				int sum = 0;
				//print out the stats
				for(int i=0; i<range;i++){
					//System.out.println(String.format("Bucket %d: %d", i, receivedData[i]));
					sum += receivedData[i];
					//System.out.println("Total Generated Random Numbers : " + sum);
				}
				System.out.println("Exiting Writer ... processing count : " + count);
				count = 0;
			}
		};
	}
	
	private static class DataToken {
		private double token;
		public DataToken(double token) {
			this.token=token;
		}
		public double getToken() {
			return token;
		}
		public void setToken(double token) {
			this.token = token;
		}
	}
}
