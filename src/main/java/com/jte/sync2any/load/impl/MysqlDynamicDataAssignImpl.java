package com.jte.sync2any.load.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jte.sync2any.load.DynamicDataAssign;
import com.jte.sync2any.util.DbUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author JerryYin
 * @since 2021-04-26 10:37
 */
@Service("mysqlDynamicDataAssign")
@Slf4j
public class MysqlDynamicDataAssignImpl extends DynamicDataAssign {
    private Cache<String,String> groupTableName = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .expireAfterAccess(3600, TimeUnit.MINUTES)
            .build();
    /**
     * SELECT suffix_name FROM t_pms_table_router WHERE group_code= ?;
     */
    private static final String FIND_SUFFIX_SQL_TEMPLATE = "SELECT suffix_name FROM t_pms_table_router WHERE group_code= ?;";

    private static final String DYNAMIC_TABLE_SUFFIX_NAME = "SELECT distinct(suffix_name) FROM t_pms_table_router;";

    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s LIKE t_pms_booking_log";
    /**
     * key:targetDbId_groupCode
     * value:suffix name
     */
    @Resource
    Map<String, Object> allTargetDatasource;



    @Override
    public String dynamicTableName(String targetDbId,String originTableName , Object shardingValue) {
        String key = targetDbId+"_"+ shardingValue;
        String suffixName = groupTableName.getIfPresent(key);
        if(StringUtils.isNotBlank(suffixName)){
            return originTableName+suffixName;
        }
        JdbcTemplate jdbcTemplate = (JdbcTemplate) DbUtils.getTargetDsByDbId(allTargetDatasource,targetDbId);
        log.debug("shardingValue:{}",shardingValue);
        List<String> suffixNameList = jdbcTemplate.queryForList(FIND_SUFFIX_SQL_TEMPLATE,new Object[]{shardingValue},String.class);
        if(suffixNameList.isEmpty()){
            log.warn("can't not find any suffixName,return origin table name:{}",originTableName);
            return originTableName;
        }
        suffixName = suffixNameList.get(0);
        groupTableName.put(key,suffixName);
        return originTableName+suffixName;
    }

    @Override
    public void init(String targetDbId, String originTableName, Object shardingValue) {
        JdbcTemplate jdbcTemplate = (JdbcTemplate) DbUtils.getTargetDsByDbId(allTargetDatasource,targetDbId);
        log.debug("shardingValue:{}",shardingValue);
        List<String> suffixNameList = jdbcTemplate.queryForList(DYNAMIC_TABLE_SUFFIX_NAME,String.class);
        for (String s : suffixNameList) {
            jdbcTemplate.execute(String.format(CREATE_TABLE, originTableName + s));
        }
    }
}
