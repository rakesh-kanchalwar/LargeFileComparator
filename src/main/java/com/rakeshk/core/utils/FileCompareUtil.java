package com.rakeshk.core.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.fluttercode.datafactory.impl.DataFactory;

/**
 * 
 *
 */
public class FileCompareUtil {

	boolean isInputFileProcessingCompleted = false;

	String mainDirectoryPath = "C://Users//Rakesh//Desktop//testData//";
	String input_file_name = "input.csv";
	String output_file_name = "output.csv";
	String temp_file_directory = "temp//";
	String split_file_names_list = "listOfSplitFileNames.properties";
	
	String inputFilePath = mainDirectoryPath + input_file_name;
	String outputFilePath = mainDirectoryPath + output_file_name;
	String tempFilePath = mainDirectoryPath + temp_file_directory;
	
	CountDownLatch latch;
	Queue<String> recordQueue = new ConcurrentLinkedQueue<>();
	Map<Character, BufferedWriter> map;

	public static void main(String[] args) throws IOException, InterruptedException {
		long startTime = System.currentTimeMillis();
		FileCompareUtil fileCompare = new FileCompareUtil();
		//app.generateInputFile();
		 
		/*fileCompare.readAndSplit(fileCompare, fileCompare.inputFilePath, fileCompare.mainDirectoryPath + "temp//inputSplit//", 
				fileCompare.tempFilePath + "inputSplit//" + fileCompare.split_file_names_list);*/
		
		/*fileCompare.readAndSplit(fileCompare, fileCompare.outputFilePath, fileCompare.mainDirectoryPath + "temp//outputSplit//", 
				fileCompare.tempFilePath + "outputSplit//" + fileCompare.split_file_names_list);*/
		
		
		fileCompare.compareAndGenerateReport(fileCompare.tempFilePath + "inputSplit//",
				fileCompare.tempFilePath + "outputSplit//", fileCompare.split_file_names_list);
		
		
		long endTime = System.currentTimeMillis();
		System.out.println("Finished task in :" + (endTime - startTime) / 1000 + "seconds");
	}

	private void compareAndGenerateReport(String inputFileSplitPath, String outputFileSplitPath, String split_file_list_name) throws IOException {
		
		Properties listOfInputSplitFileNames = new Properties();
		listOfInputSplitFileNames.load(Files.newInputStream(Paths.get(inputFileSplitPath + split_file_list_name, new String[]{""}), StandardOpenOption.READ));
		
		Properties listOfOutputSplitFileNames = new Properties();
		listOfOutputSplitFileNames.load(Files.newInputStream(Paths.get(outputFileSplitPath + split_file_list_name, new String[]{""}), StandardOpenOption.READ));
		
		
		for (Map.Entry<Object, Object> entry : listOfInputSplitFileNames.entrySet()) {
			String fileName = (String) entry.getKey();
			
			if(listOfOutputSplitFileNames.containsKey(fileName)){
				
				try (Stream<String> inputStream = Files.lines(Paths.get(inputFileSplitPath + entry.getValue()))) {
					inputStream.forEach(new Consumer<String>() {

						@Override
						public void accept(String inputRecord) {
							System.out.println(inputRecord);
							String inputRecordToken = inputRecord.split(",")[0];
							
							try (Stream<String> outputStream = Files.lines(Paths.get(outputFileSplitPath + entry.getValue()))) {
								outputStream.forEach(new Consumer<String>() {

									@Override
									public void accept(String outputRecord) {
										if(outputRecord.split(",")[0].equals(inputRecordToken) &&
												!outputRecord.equals(inputRecord)){
											System.out.println(inputRecord);
											System.out.println(outputRecord);
											System.out.println("");
										}
										//System.out.println(count++);
									}
								});

							} catch (IOException e) {
								System.out.println("Error in reading file");
							}
							
						}
					});

				} catch (IOException e) {
					System.out.println("Error in reading file");
				}
				
			}
		}
	}

	private void readAndSplit(FileCompareUtil fileCompare, String pathOfTheFileToRead, String pathOfTheSplitFilesDirectory, 
			String listOfSplitFiles) throws InterruptedException, IOException {
		fileCompare.map = new HashMap<>();
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		fileCompare.latch = new CountDownLatch(2);
		executorService.submit(fileCompare.new ReadTask(pathOfTheFileToRead));
		executorService.submit(fileCompare.new WriteTask(pathOfTheSplitFilesDirectory));
		executorService.shutdown();
		fileCompare.latch.await();
		
		fileCompare.createSplitFileList(fileCompare, listOfSplitFiles);
	}

	private void createSplitFileList(FileCompareUtil app, String filePath) throws IOException {
		Properties properties = new Properties();
		for(Entry<Character, BufferedWriter> entry : app.map.entrySet()){
			properties.put(entry.getKey().toString(), entry.getKey().toString() + ".csv");
		}
		properties.store(Files.newOutputStream(Paths.get(filePath, new String[]{""}), StandardOpenOption.CREATE, StandardOpenOption.WRITE), "");
	}

	class ReadTask implements Callable<Boolean> {
		String fileName;
		public ReadTask(String fileName) {
			this.fileName = fileName;
		}
		@Override
		public Boolean call() throws Exception {
			try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
				stream.forEach(new Consumer<String>() {

					@Override
					public void accept(String line) {
						while (true) {
							if (recordQueue.size() >= 1000) {
								try {
									Thread.sleep(2000);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							} else {
								break;
							}
						}
						recordQueue.add(line);
					}
				});

			} catch (IOException e) {
				System.out.println("Error in reading file");
				return false;
			}
			isInputFileProcessingCompleted = true;
			latch.countDown();
			return true;
		}
	}

	class WriteTask implements Callable<Boolean> {
		String tempDirectoryPath;
		public WriteTask(String tempDirectoryPath) {
			this.tempDirectoryPath = tempDirectoryPath;
		}
		@Override
		public Boolean call() throws Exception {
			while(true){
				try {
					String record = recordQueue.remove();
					Character character = record.split(",")[0].charAt(0);
					BufferedWriter bw;
					if(map.containsKey(character)){
						bw = map.get(character);
					}else{
						bw = new BufferedWriter(new FileWriter(new File(tempDirectoryPath + character.toString() + ".csv")));
						map.put(character, bw);
					}
					bw.write(record);
					bw.newLine();
				} catch (Exception e) {
					//System.out.println("No records in Queue");
				}
				if (isInputFileProcessingCompleted) {
					break;
				}
			}
			for(BufferedWriter bw : map.values()){
				bw.close();
			}
			latch.countDown();
			return true;
		}
	}
	
	private void generateInputFile() throws IOException {
		 DataFactory df = new DataFactory(); 
		 Random random = new Random();
		 random.setSeed(500000000); 
		 try (BufferedWriter bf = new BufferedWriter(new FileWriter(new File(mainDirectoryPath + "output.csv")))) { 
			 for (int i = 0; i < 5000000; i++) { 
				 StringBuffer sb = new StringBuffer(random.nextLong() + "," + df.getFirstName() + "," + df.getLastName() + "," + df.getFirstName() + " " + df.getLastName());
				 System.out.println(sb.toString()); bf.write(sb.toString());
				 bf.newLine(); bf.flush(); 
			 } 
		}
	}
}
