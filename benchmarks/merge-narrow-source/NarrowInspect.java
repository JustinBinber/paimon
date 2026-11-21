package bench;
import java.util.*;import java.nio.file.*;import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.catalog.CatalogContext;import org.apache.paimon.options.Options;import org.apache.paimon.table.FileStoreTable;import org.apache.paimon.table.FileStoreTableFactory;import org.apache.paimon.manifest.ManifestEntry;
public class NarrowInspect {
 public static void inspectSource(String warehouse,String output) throws Exception {
  Options o=new Options();o.setString("path",Paths.get(warehouse,"bench.db","source").toUri().toString());FileStoreTable t=FileStoreTableFactory.create(CatalogContext.create(o));
  List<Object> files=new ArrayList<>();
  for(ManifestEntry e:t.store().newScan().plan().files()){
   java.nio.file.Path path=Paths.get(warehouse,"bench.db","source","bucket-"+e.bucket(),e.file().fileName());
   org.apache.parquet.hadoop.metadata.ParquetMetadata footer=org.apache.parquet.hadoop.ParquetFileReader.readFooter(new org.apache.hadoop.conf.Configuration(),new org.apache.hadoop.fs.Path(path.toUri()));
   Map<String,Object> normalized=new LinkedHashMap<>();normalized.put("schema",footer.getFileMetaData().getSchema().toString());normalized.put("rows",e.file().rowCount());
   List<Object> groups=new ArrayList<>();
   for(org.apache.parquet.hadoop.metadata.BlockMetaData block:footer.getBlocks()){
    Map<String,Object> group=new LinkedHashMap<>();group.put("rows",block.getRowCount());List<Object> columns=new ArrayList<>();
    for(org.apache.parquet.hadoop.metadata.ColumnChunkMetaData col:block.getColumns()){
     Map<String,Object> column=new LinkedHashMap<>();column.put("path",col.getPath().toDotString());column.put("codec",col.getCodec().toString());column.put("compressedBytes",col.getTotalSize());column.put("uncompressedBytes",col.getTotalUncompressedSize());column.put("statistics",col.getStatistics().toString());columns.add(column);
    }
    group.put("columns",columns);groups.add(group);
   }
   normalized.put("rowGroups",groups);Map<String,Object> file=new LinkedHashMap<>();file.put("name",e.file().fileName());file.put("bucket",e.bucket());file.put("bytes",e.file().fileSize());file.put("datasetFooter",normalized);files.add(file);
  }
  Files.write(Paths.get(output),new ObjectMapper().writeValueAsBytes(files));
 }
 public static void inspect(String warehouse,String output) throws Exception {
  Options o=new Options();o.setString("path",Paths.get(warehouse,"bench.db","target").toUri().toString());FileStoreTable t=FileStoreTableFactory.create(CatalogContext.create(o));
  List<Object> rows=new ArrayList<>();
  for(ManifestEntry e:t.store().newScan().plan().files()){Map<String,Object> m=new LinkedHashMap<>();m.put("name",e.file().fileName());m.put("bucket",e.bucket());m.put("firstRowId",e.file().firstRowId());m.put("rows",e.file().rowCount());m.put("bytes",e.file().fileSize());m.put("actualBytes",Files.size(Paths.get(warehouse,"bench.db","target","bucket-"+e.bucket(),e.file().fileName())));m.put("writeCols",e.file().writeCols());rows.add(m);}
  Map<String,Object> result=new LinkedHashMap<>();result.put("snapshot",t.store().snapshotManager().latestSnapshotId());result.put("files",rows);Files.write(Paths.get(output),new ObjectMapper().writeValueAsBytes(result));
 }
}
