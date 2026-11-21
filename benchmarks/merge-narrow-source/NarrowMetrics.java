package bench;
import org.apache.spark.scheduler.*;
import org.apache.spark.executor.TaskMetrics;
import java.util.*;import java.nio.file.*;import com.fasterxml.jackson.databind.ObjectMapper;
public class NarrowMetrics extends SparkListener {
 private final ObjectMapper json=new ObjectMapper();
 private final Path path=Paths.get(System.getProperty("bench.events"));
 private synchronized void emit(Map<String,Object> m){try{m.put("observedEpochMillis",System.currentTimeMillis());Files.write(path,(json.writeValueAsString(m)+"\n").getBytes("UTF-8"),StandardOpenOption.CREATE,StandardOpenOption.APPEND);}catch(Exception e){throw new RuntimeException(e);}}
 public void onTaskEnd(SparkListenerTaskEnd e){TaskMetrics t=e.taskMetrics();if(t==null)return;Map<String,Object> m=new LinkedHashMap<>();m.put("type","task");m.put("stage",e.stageId());m.put("stageAttempt",e.stageAttemptId());m.put("executor",e.taskInfo().executorId());m.put("taskId",e.taskInfo().taskId());m.put("index",e.taskInfo().index());m.put("attempt",e.taskInfo().attemptNumber());m.put("speculative",e.taskInfo().speculative());m.put("start",e.taskInfo().launchTime());m.put("end",e.taskInfo().finishTime());m.put("success",e.taskInfo().successful());m.put("cpuNanos",t.executorCpuTime());m.put("gcMillis",t.jvmGCTime());m.put("shuffleWrite",t.shuffleWriteMetrics().bytesWritten());m.put("shuffleRead",t.shuffleReadMetrics().totalBytesRead());m.put("diskSpill",t.diskBytesSpilled());m.put("inputBytes",t.inputMetrics().bytesRead());m.put("outputBytes",t.outputMetrics().bytesWritten());emit(m);}
 public void onBlockUpdated(SparkListenerBlockUpdated e){Map<String,Object> m=new LinkedHashMap<>();m.put("type","block");m.put("block",e.blockUpdatedInfo().blockId().name());m.put("blockManager",e.blockUpdatedInfo().blockManagerId().toString());m.put("disk",e.blockUpdatedInfo().diskSize());m.put("memory",e.blockUpdatedInfo().memSize());emit(m);}
 public void onJobStart(SparkListenerJobStart e){Map<String,Object> m=new LinkedHashMap<>();m.put("type","jobStart");m.put("job",e.jobId());m.put("stages",e.stageIds().toString());m.put("execution",e.properties().getProperty("spark.sql.execution.id"));m.put("time",e.time());emit(m);}
 public void onOtherEvent(SparkListenerEvent e){String n=e.getClass().getSimpleName();if(!n.contains("SQL"))return;Map<String,Object> m=new LinkedHashMap<>();m.put("type",n);for(String k:new String[]{"executionId","physicalPlanDescription","description","time"})try{m.put(k,e.getClass().getMethod(k).invoke(e));}catch(Exception ignored){}emit(m);}
}
