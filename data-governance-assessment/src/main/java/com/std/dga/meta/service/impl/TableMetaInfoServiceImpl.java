package com.std.dga.meta.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.support.spring.PropertyPreFilters;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.std.dga.meta.bean.TableMetaInfo;
import com.std.dga.meta.bean.TableMetaInfoQuery;
import com.std.dga.meta.bean.TableMetaInfoVO;
import com.std.dga.meta.mapper.TableMetaInfoMapper;
import com.std.dga.meta.service.TableMetaInfoExtraService;
import com.std.dga.meta.service.TableMetaInfoService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.std.dga.util.SqlUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * 元数据表 服务实现类
 * </p>
 *
 * @author std
 * @since 2023-10-07
 */
@Service
@DS("dga")
public class TableMetaInfoServiceImpl extends
        ServiceImpl<TableMetaInfoMapper, TableMetaInfo> implements TableMetaInfoService {
    IMetaStoreClient hiveClient;

//    FileSystem fs = null;
    //从配置文件中读取配置，赋值到属性上
    @Value("${hive.metastore.server.url}")
    private String metaStoreUrl;
    @Value("${hdfs.uri}")
    private String hdfsUri;
    @Autowired
    TableMetaInfoExtraService tableMetaInfoExtraService;

    @Autowired
    TableMetaInfoMapper tableMetaInfoMapper;


    /**
     * 创建MetaStoreClient
     */
    @PostConstruct
    public void getHiveClient(){
        HiveConf hiveConf = new HiveConf();
        MetastoreConf.setVar(hiveConf , MetastoreConf.ConfVars.THRIFT_URIS , metaStoreUrl );
        try {
            hiveClient =  new HiveMetaStoreClient(hiveConf) ;
        } catch (MetaException e) {
            throw new RuntimeException(e);
        }
    }

//    @PostConstruct
//    private void getFileSystem(){
//        fs = FileSystem.get(new URI(hdfsUri), new Configuration(), tableMetaInfo.getTableFsOwner());
//    }

    public void initTableMeta(String databaseName , String assessDate ) throws Exception {

        //通过api方法获取hive元数据
        //根据库名查询表名列表
        List<String> allTableNameList = hiveClient.getAllTables(databaseName);
        List<TableMetaInfo> tableMetaInfoList = new ArrayList<>(allTableNameList.size()) ;
        for (String tableName : allTableNameList) {
            Table table = hiveClient.getTable(databaseName , tableName) ;
            //提取每个表的hive元数据
            TableMetaInfo tableMetaInfo = getTableMetaFromHive(table) ;
            //System.out.println("table =" +  tableMetaInfo);

            //提取hdfs的元数据
            addHdfsInfo(tableMetaInfo) ;

            //考评时间
            tableMetaInfo.setAssessDate(assessDate);
            //创建时间
            tableMetaInfo.setCreateTime(new Date());

            //保存到集合，稍后批写
            tableMetaInfoList.add(tableMetaInfo);
        }


        //写入数据之前， 先清理当日数据
        remove(new QueryWrapper<TableMetaInfo>().eq("assess_date" , assessDate)) ;
//        remove(new QueryWrapper<TableMetaInfo>().eq("schema_name" , "gmall")) ;
        //批写入数据库
        saveOrUpdateBatch(tableMetaInfoList) ;

        tableMetaInfoExtraService.initTableMetaExtra(tableMetaInfoList);
    }



    /**
     * 取hive中的元数据信息
     * @param table
     * @return
     */
    private  TableMetaInfo getTableMetaFromHive(Table table ){
        TableMetaInfo tableMetaInfo = new TableMetaInfo();
        tableMetaInfo.setTableName(table.getTableName());
        tableMetaInfo.setSchemaName(table.getDbName());
        //json过滤
        PropertyPreFilters.MySimplePropertyPreFilter propertyFilter
                = new PropertyPreFilters().addFilter("comment", "name", "type");

        tableMetaInfo.setColNameJson(JSON.toJSONString(table.getSd().getCols() ,propertyFilter));
        tableMetaInfo.setPartitionColNameJson(JSON.toJSONString(table.getPartitionKeys() ,propertyFilter));
        tableMetaInfo.setTableFsOwner(table.getOwner());
        tableMetaInfo.setTableParametersJson(JSON.toJSONString(table.getParameters()));
        tableMetaInfo.setTableComment(table.getParameters().get("comment"));
        tableMetaInfo.setTableFsPath(table.getSd().getLocation());
        tableMetaInfo.setTableInputFormat(table.getSd().getInputFormat());
        tableMetaInfo.setTableOutputFormat(table.getSd().getOutputFormat());
        tableMetaInfo.setTableRowFormatSerde(table.getSd().getSerdeInfo().getSerializationLib());
        tableMetaInfo.setTableCreateTime(new Date(table.getCreateTime() * 1000L));
        tableMetaInfo.setTableBucketNum((long) table.getSd().getNumBuckets());
        //判断有分桶
        if(tableMetaInfo.getTableBucketNum() > 0 ){
            tableMetaInfo.setTableBucketColsJson(JSON.toJSONString(table.getSd().getBucketCols()));
            tableMetaInfo.setTableSortColsJson(JSON.toJSONString(table.getSd().getSortCols()));
        }
        tableMetaInfo.setTableType( table.getTableType());
        return tableMetaInfo;
    }


    /**
     * 取hdfs的元数据信息
     * @param tableMetaInfo
     */
    private void addHdfsInfo(TableMetaInfo tableMetaInfo) throws Exception {
        //连接hdfs客户端 FileSystem
        FileSystem fs =
                FileSystem.get(new URI(hdfsUri), new Configuration(), tableMetaInfo.getTableFsOwner());
        //获取表路径下所有的文件及目录
        FileStatus[] listStatus = fs.listStatus(new Path(tableMetaInfo.getTableFsPath()));
        //递归补充文件相关信息
        addFileInfo( listStatus , tableMetaInfo , fs );
        //System.out.println(tableMetaInfo);

        //补充环境信息
        tableMetaInfo.setFsCapcitySize(fs.getStatus().getCapacity());
        tableMetaInfo.setFsUsedSize(fs.getStatus().getUsed());
        tableMetaInfo.setFsRemainSize(fs.getStatus().getRemaining());
    }


    /**
     * 递归
     *
     *         // table_total_size    table_size * 副本系数
     *
     *         // last_modify 遍历文件夹及其子目录下的所有文件最近修改时间
     *
     *         // last_access 遍历文件夹及其子目录下的所有文件最近访问时间
     *
     *         // 文件系统总容量 取现成
     *
     * @param listStatus
     * @param tableMetaInfo
     * @param fs
     */
    private void addFileInfo(FileStatus[] listStatus, TableMetaInfo tableMetaInfo, FileSystem fs) throws Exception {
        for (FileStatus fileStatus : listStatus) {
            if(fileStatus.isFile()){
                //处理 , 如果是文件，取文件的元数据信息进行累加统计
                tableMetaInfo.setTableSize(tableMetaInfo.getTableSize() + fileStatus.getLen()) ;

                tableMetaInfo.setTableTotalSize(tableMetaInfo.getTableTotalSize() + fileStatus.getLen() * fileStatus.getReplication() );

                long lastModifyTimeMs = Math.max(tableMetaInfo.getTableLastModifyTime() == null ? 0 : tableMetaInfo.getTableLastModifyTime().getTime(), fileStatus.getModificationTime());
                tableMetaInfo.setTableLastModifyTime(new Date(lastModifyTimeMs));

                long lastAccessTimeMs = Math.max(tableMetaInfo.getTableLastAccessTime() == null ? 0 : tableMetaInfo.getTableLastAccessTime().getTime(), fileStatus.getAccessTime());
                tableMetaInfo.setTableLastAccessTime(new Date(lastAccessTimeMs));

            }else{
                //下探 ，如果是目录，继续递归，
                addFileInfo(fs.listStatus(fileStatus.getPath()) , tableMetaInfo , fs);
            }
        }
    }

    @Override
    public List<TableMetaInfoVO> selectTableMetaInfoVOList(TableMetaInfoQuery tableMetaInfoQuery) {

        StringBuilder sqlBuilder = new StringBuilder(
                "SELECT ti.id , ti.table_name , ti.schema_name , ti.table_comment ,\n" +
                        "    ti.table_size , ti.table_total_size , ti.table_last_modify_time ,\n" +
                        "    ti.table_last_access_time ,te.tec_owner_user_name , te.busi_owner_user_name \n" +
                        "FROM table_meta_info ti  JOIN table_meta_info_extra te \n" +
                        "ON ti.table_name = te.table_name AND ti.schema_name = te.schema_name \n" +
                        "WHERE ti.assess_date = (SELECT MAX(assess_date) FROM table_meta_info) \n"
        );
        if(tableMetaInfoQuery.getTableName()!=null&&!tableMetaInfoQuery.getTableName().trim().isEmpty()){
            sqlBuilder.append(" and ti.table_name like '%"+ SqlUtil.filterUnsafeSql(tableMetaInfoQuery.getTableName()) +"%'");
        }
        if(tableMetaInfoQuery.getSchemaName()!=null&&!tableMetaInfoQuery.getSchemaName().trim().isEmpty()){
            sqlBuilder.append(" and ti.schema_name = '"+ SqlUtil.filterUnsafeSql(tableMetaInfoQuery.getSchemaName()) +"'");
        }
        if(tableMetaInfoQuery.getDwLevel()!=null&&!tableMetaInfoQuery.getDwLevel().trim().isEmpty()){
            sqlBuilder.append(" and te.dw_level = '"+ SqlUtil.filterUnsafeSql(tableMetaInfoQuery.getDwLevel()) +"'");
        }

        int start = (tableMetaInfoQuery.getPageNo() - 1) * tableMetaInfoQuery.getPageSize();
        sqlBuilder.append(" limit "+start+","+tableMetaInfoQuery.getPageSize());


        return tableMetaInfoMapper.selectTableMetaInfoVoList(sqlBuilder.toString());
    }

    @Override
    public Integer selectTableMetaInfoCount(TableMetaInfoQuery tableMetaInfoQuery) {
        StringBuilder sqlBuilder = new StringBuilder(
                "SELECT count(*) \n" +
                        "FROM table_meta_info ti  JOIN table_meta_info_extra te \n" +
                        "ON ti.table_name = te.table_name AND ti.schema_name = te.schema_name \n" +
                        "WHERE ti.assess_date = (SELECT MAX(assess_date) FROM table_meta_info) \n"
        );
        if(tableMetaInfoQuery.getTableName()!=null&&!tableMetaInfoQuery.getTableName().trim().isEmpty()){
            sqlBuilder.append(" and ti.table_name like '%"+ SqlUtil.filterUnsafeSql(tableMetaInfoQuery.getTableName()) +"%'");
        }
        if(tableMetaInfoQuery.getSchemaName()!=null&&!tableMetaInfoQuery.getSchemaName().trim().isEmpty()){
            sqlBuilder.append(" and ti.schema_name = '"+ SqlUtil.filterUnsafeSql(tableMetaInfoQuery.getSchemaName()) +"'");
        }
        if(tableMetaInfoQuery.getDwLevel()!=null&&!tableMetaInfoQuery.getDwLevel().trim().isEmpty()){
            sqlBuilder.append(" and te.dw_level = '"+ SqlUtil.filterUnsafeSql(tableMetaInfoQuery.getDwLevel()) +"'");
        }

        return tableMetaInfoMapper.selectTableMetaInfoCount(sqlBuilder.toString());
    }



}
