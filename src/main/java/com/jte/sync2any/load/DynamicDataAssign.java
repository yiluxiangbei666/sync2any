package com.jte.sync2any.load;

import com.jte.sync2any.conf.SpringContextUtils;
import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.model.mysql.Field;
import com.jte.sync2any.model.mysql.Row;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.model.mysql.TableRecords;
import com.jte.sync2any.util.DbUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Objects;

/**
 * @author JerryYin
 * @since 2021-04-26 9:41
 *
 */
@Slf4j
public abstract class DynamicDataAssign {
    /***
     * 动态表名
     * @param shardingValue
     * @return
     */
    public abstract String dynamicTableName(String targetDbId, String originTableName, Object shardingValue);


    /**
     * 获取动态表名
     * @param records
     * @param tableMeta
     * @return
     */
    public static String getDynamicTableName(TableRecords records,TableMeta tableMeta){
        log.info("tableMeta dynamic table name assigner:"+tableMeta.getDynamicTablenameAssigner());
        if(StringUtils.isNotBlank(tableMeta.getDynamicTablenameAssigner())){
            Object shardingValue = getShardingValue(records,tableMeta.getShardingKey());
            return getDynamicTableName(shardingValue,tableMeta);
        }else{
            return records.getTableMeta().getTargetTableName().toLowerCase();
        }

    }

    /**
     * 获取动态表名
     * @param ShardingValue
     * @param tableMeta
     * @return
     */
    public static String getDynamicTableName(Object ShardingValue,TableMeta tableMeta){
        log.info("tableMeta dynamic table name assigner:"+tableMeta.getDynamicTablenameAssigner());
        if(StringUtils.isNotBlank(tableMeta.getDynamicTablenameAssigner())){
            DynamicDataAssign assigner = DynamicDataAssign.getDynamicDataAssign(tableMeta);
            String tableName = assigner.dynamicTableName(tableMeta.getTargetDbId(),tableMeta.getTargetTableName(),ShardingValue);
            return tableName.toLowerCase();
        }else{
            return tableMeta.getTargetTableName().toLowerCase();
        }
    }


    /**
     * 获取表名计算器
     * @param tableMeta
     * @return
     */
    public static DynamicDataAssign getDynamicDataAssign(TableMeta tableMeta){
        String assigner = tableMeta.getDynamicTablenameAssigner();
        DynamicDataAssign dynamicDataAssign = (DynamicDataAssign) SpringContextUtils.getContext().getBean(assigner);
        if(Objects.isNull(assigner)){
            throw new ShouldNeverHappenException("dynamic data source is not found:"+assigner+",please check your configuration.");
        }
        return dynamicDataAssign;
    }


    /**
     * 获取分区健对应的值
     *
     * @param records
     * @param shardingKey
     * @return
     */
    private static Object getShardingValue(TableRecords records, String shardingKey) {
        if (StringUtils.isBlank(shardingKey)) {
            return null;
        }
        List<Row> rowList = records.getNewRows().isEmpty()?records.getOldRows():records.getNewRows();
        if (rowList.isEmpty()) {
            rowList = records.getOldRows();
        }
        if (rowList.isEmpty()) {
            return null;
        }
        Row row = rowList.get(0);
        Field field = row.getFields().stream().filter(e -> e.getName().equals(shardingKey)).findAny().orElse(null);
        return field.getValue();
    }

    /***
     * 动态表名
     * @param shardingValue
     * @return
     */
    public abstract void init(String targetDbId, String originTableName, Object shardingValue);
}
