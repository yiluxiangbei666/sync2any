package com.jte.sync2any.conf;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.extract.impl.MysqlMetaExtractImpl;
import com.jte.sync2any.load.DynamicDataAssign;
import com.jte.sync2any.model.config.*;
import com.jte.sync2any.model.es.EsDateType;
import com.jte.sync2any.model.mysql.ColumnMeta;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.util.ColumnUtils;
import com.jte.sync2any.util.DbUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.sql.Types;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author JerryYin
 * 规则解析器：
 * 1、哪些表需要被传输
 * 2、表的字段名称和字段类型的映射
 * 3、索引名称的映射
 * 4、过滤器的规则
 */

@Configuration
@Slf4j
public class RuleConfigParser {


    /**
     * key: targetDbId$sourceDbId$sourceTableName
     * value: TableMeta
     */
    public static final Cache<String, TableMeta> RULES_MAP = CacheBuilder.newBuilder().build();

    @Resource
    private MysqlMetaExtractImpl metaExtract;
    @Resource
    private Sync2any sync2any;
    @Resource
    private SourceMysqlDb sourceMysqlDb;
    @Resource
    private TargetDatasources targetDatasources;


    public static List<TableMeta> getTableMetaListByMq(String topicName, String topicGroup) {
        List<TableMeta> tableMetaList = new ArrayList<>();
        Set<String> keySet = RULES_MAP.asMap().keySet();
        for (String key : keySet) {
            TableMeta tableMeta = RULES_MAP.getIfPresent(key);
            if (tableMeta.getTopicName().equals(topicName) && tableMeta.getTopicGroup().equals(topicGroup)) {
                tableMetaList.add(tableMeta);
            }
        }
        return tableMetaList;
    }

    /**
     * 加载配置文件进行初始化
     */
    public void initAllRules() {
        this.checkConfig();

        List<SyncConfig> syncConfigList = sync2any.getSyncConfigList();
        for (int r = 0; r < sync2any.getSyncConfigList().size(); r++) {
            SyncConfig config = syncConfigList.get(r);
            log.info("sync config:{}", config.toString());
            Conn conn = DbUtils.getConnByDbId(targetDatasources.getDatasources(), config.getTargetDbId());
            config.setTargetConn(conn);
            //获取所有的表名
            List<String> tableNameList = metaExtract.getAllTableName(config.getSourceDbId());
            log.debug("all tables:", tableNameList);
            //查看表名是否匹配
            String[] syncTableArray = config.getSyncTables().split(",");
            for (String syncTableName : syncTableArray) {
                List<String> matchTableName = tableNameList.stream()
                        .filter(rt -> Pattern.matches(syncTableName, rt))
                        .collect(Collectors.toList());
                //接下来解析rule
                for (String realTableName : matchTableName) {
                    initRuleByTableName(config, realTableName);
                }
            }
        }
    }

    /**
     * 根据指定的SyncConfig 和 tableName 进行初始化配置
     *
     * @param syncConfigList
     * @param tableName
     */
    public List<TableMeta> initRules(List<SyncConfig> syncConfigList, String tableName) {
        List<TableMeta> tableMetaList = new ArrayList<>();
        for (int r = 0; r < syncConfigList.size(); r++) {
            SyncConfig config = syncConfigList.get(r);
            //查看表名是否匹配
            String[] syncTableArray = config.getSyncTables().split(",");
            for (String syncTableName : syncTableArray) {
                boolean isMatch = Pattern.matches(syncTableName, tableName);
                log.info("initRules.... syncTableName:{} , tableName:{} , isMatch:{}", syncTableName, tableName, isMatch);
                if (isMatch) {
                    TableMeta tableMeta = initRuleByTableName(config, tableName);
                    if (Objects.nonNull(tableMeta)) {
                        tableMetaList.add(tableMeta);
                    }
                    if (Objects.isNull(RULES_MAP.getIfPresent(config.getTargetDbId() + "$" + config.getSourceDbId() + "$" + tableName.toLowerCase()))) {
                        RULES_MAP.put(config.getTargetDbId() + "$" + config.getSourceDbId() + "$" + tableName.toLowerCase(), tableMeta);
                    }
                }
            }
        }
        return tableMetaList;
    }


    /**
     * 根据源数据库id来匹配 相关的 sync-config-list
     *
     * @param sourceDbName
     * @return
     */
    public List<SyncConfig> getSysConfigBySourceDbName(String sourceDbName) {
        String dbId = sourceMysqlDb.getDatasources().stream().filter(c -> c.getDbName().equalsIgnoreCase(sourceDbName)).map(e -> e.getDbId()).findFirst().orElse(null);
        if (StringUtils.isBlank(dbId)) {
            return new ArrayList<SyncConfig>();
        }
        return sync2any.getSyncConfigList()
                .stream()
                .filter(e -> e.getSourceDbId().equals(dbId))
                .collect(Collectors.toList());
    }

    /**
     * 根据表名来初始化
     *
     * @param config
     * @param realTableName
     */
    public TableMeta initRuleByTableName(SyncConfig config, String realTableName) {
        String key = config.getTargetDbId() + "$" + config.getSourceDbId() + "$" + realTableName;
        //寻找到了匹配的表
        TableMeta tableMeta = RULES_MAP.getIfPresent(key);
        if (Objects.nonNull(tableMeta)) {
            log.warn("During initRule RULES_MAP already contains table:{}", realTableName);
            return null;
        }
        //该表还未解析规则，寻找规则
        tableMeta = metaExtract.getTableMate(config.getSourceDbId(), realTableName);
        tableMeta.setTopicName(config.getMq().getTopicName());
        tableMeta.setTopicGroup(config.getMq().getTopicGroup());
        tableMeta.setSourceDbId(config.getSourceDbId());
        tableMeta.setTargetDbId(config.getTargetDbId());
        tableMeta.setSyncConfig(config);

        List<Rule> ruleList = Optional.ofNullable(config.getRules()).orElse(Collections.emptyList());
        Rule rule = ruleList.stream()
                .filter(tr -> Pattern.matches(tr.getTable(), realTableName))
                .findFirst().orElse(null);

        //填充匹配规则
        parseColumnMeta(config.getTargetConn().getType(), tableMeta, rule, config);
        RULES_MAP.put(key, tableMeta);
        return tableMeta;
    }


    public static String getTableNameFromDdl(String ddlSql) {
        final String dbType = JdbcConstants.MYSQL.name();
        List<SQLStatement> stmtList = SQLUtils.parseStatements(ddlSql, dbType);
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) stmtList.get(0);
        return ColumnUtils.delEscape(stmt.getTableSource().getName().getSimpleName());
    }

    private void checkConfig() {
        if (StringUtils.isBlank(sync2any.getMysqldump())) {
            log.warn("mysqldump 未配置，将不会同步原始数据.");
        }
        if (sync2any.getSyncConfigList().size() == 0) {
            log.error("请至少填写一个sync-config-list配置");
            System.exit(500);
        }
        Set<String> topicGroupSet = new HashSet<>();
        sync2any.getSyncConfigList().forEach(s -> {
            if (StringUtils.isBlank(s.getSourceDbId())) {
                log.error("请填写sync-config-list下的db-name配置项！");
                System.exit(500);
            }
            if (StringUtils.isBlank(s.getSyncTables())) {
                log.error("请填写sync-config-list下的sync-tables配置项！");
                System.exit(500);
            }
            if (StringUtils.isBlank(s.getMq().getTopicName())) {
                log.error("请填写sync-config-list下的topic-name配置项！");
                System.exit(500);
            }
            if (StringUtils.isBlank(s.getMq().getTopicGroup())) {
                log.error("请填写sync-config-list下的topic-group配置项！");
                System.exit(500);
            } else if (topicGroupSet.contains(s.getMq().getTopicGroup())) {
                //因为要通过topicName来区分不同的任务。
                log.error("禁止多个同步任务使用同一个topicGroup！");
                System.exit(500);
            } else {
                topicGroupSet.add(s.getMq().getTopicGroup());
            }
        });
    }

    private void checkEsDataType(String dataType) {
        if (Objects.isNull(EsDateType.getDataType(dataType))) {
            throw new ShouldNeverHappenException("not support this data type for now. data type:" + dataType);
        }
    }

    /**
     * 映射配置es字段的数据类型
     *
     * @param columnMeta
     * @param targetType
     * @return
     */
    private ColumnMeta mapDataTypeOfEs(ColumnMeta columnMeta, String targetType) {
        if (!Conn.DB_TYPE_ES.equals(targetType)) {
            return columnMeta;
        }
        switch (columnMeta.getDataType()) {
            // BOOLEAN -->boolean
            case Types.BOOLEAN:
                columnMeta.setEsDataType(EsDateType.BOOLEAN.getDataType());
                break;
            //  INTEGER TINYINT -->integer
            case Types.TINYINT:
                columnMeta.setEsDataType(EsDateType.INTEGER.getDataType());
                break;
            case Types.INTEGER:
                columnMeta.setEsDataType(EsDateType.INTEGER.getDataType());
                break;
            // BIGINT NUMERIC-->long
            case Types.NUMERIC:
                columnMeta.setEsDataType(EsDateType.LONG.getDataType());
                break;
            case Types.BIGINT:
                columnMeta.setEsDataType(EsDateType.LONG.getDataType());
                break;
            // DECIMAL DOUBLE-->double
            case Types.DECIMAL:
                columnMeta.setEsDataType(EsDateType.DOUBLE.getDataType());
                break;
            case Types.DOUBLE:
                columnMeta.setEsDataType(EsDateType.DOUBLE.getDataType());
                break;
            // FLOAT -->float
            case Types.FLOAT:
                columnMeta.setEsDataType(EsDateType.FLOAT.getDataType());
                break;
            // CHAR NCHAR VARCHAR NCLOB CLOB-->keyword text
            case Types.CHAR:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
                break;
            case Types.NCHAR:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
                break;
            case Types.VARCHAR:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
                break;
            case Types.NCLOB:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
                break;
            case Types.CLOB:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
                break;
            // DATE >date
            case Types.DATE:
                columnMeta.setEsDataType(EsDateType.DATA.getDataType());
                break;
            // datetime >date
            case Types.TIMESTAMP:
                columnMeta.setEsDataType(EsDateType.DATA.getDataType());
                break;
            // other --> text
            default:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
        }
        return columnMeta;
    }

    private TableMeta parseColumnMeta(String targetType, TableMeta tableMeta, Rule rule, SyncConfig config) {
        if (Objects.isNull(rule)) {
            if (Conn.DB_TYPE_MYSQL.equals(targetType) || Conn.DB_TYPE_CLICKHOUSE.equals(targetType)) {
                tableMeta.setTargetTableName(tableMeta.getTableName().toLowerCase() + config.getTargetTableSuffix());
            } else {
                tableMeta.setTargetTableName(tableMeta.getDbName().toLowerCase() + "-" + tableMeta.getTableName().toLowerCase() + config.getTargetTableSuffix());
            }

            for (String columnName : tableMeta.getAllColumnMap().keySet()) {
                ColumnMeta columnMeta = tableMeta.getAllColumnMap().get(columnName);
                //未配置规则，统一化为小写
                columnMeta.setTargetColumnName(columnName.toLowerCase());
                mapDataTypeOfEs(columnMeta, targetType);
            }
            return tableMeta;
        }
        tableMeta.setDynamicTablenameAssigner(rule.getDynamicTablenameAssigner());
        tableMeta.setShardingKey(rule.getShardingKey());

        ObjectMapper jsonMapper = new ObjectMapper();
        //计算规则
        if (StringUtils.isNotBlank(rule.getIndexTable())) {
            tableMeta.setTargetTableName(rule.getIndexTable());
        } else {
            if (Conn.DB_TYPE_MYSQL.equals(targetType)) {
                tableMeta.setTargetTableName(tableMeta.getTableName().toLowerCase() + config.getTargetTableSuffix());
            } else {
                tableMeta.setTargetTableName(tableMeta.getDbName().toLowerCase() + "-" + tableMeta.getTableName().toLowerCase() + config.getTargetTableSuffix());
            }
        }
        if (StringUtils.isNotBlank(rule.getFieldFilter())) {
            List<String> includeFields = Arrays.asList(rule.getFieldFilter().toLowerCase().split(","));
            tableMeta.getAllColumnList()
                    .forEach(t -> t.setInclude(includeFields.contains(t.getColumnName().toLowerCase())));
            List<String> pkColumnNameList = tableMeta.getPrimaryKeyOnlyName();
            boolean isContainPk = tableMeta.getAllColumnList().stream()
                    .filter(c -> pkColumnNameList.contains(c.getColumnName().toLowerCase()))
                    .count() > 0;
            if (!isContainPk) {
                throw new ShouldNeverHappenException("fields filter must contain pk column!");
            }
        }

        if (Objects.nonNull(tableMeta.getDynamicTablenameAssigner())) {
            DynamicDataAssign assigner = DynamicDataAssign.getDynamicDataAssign(tableMeta);
            assigner.init(tableMeta.getTargetDbId(), tableMeta.getTargetTableName(), tableMeta.getShardingKey());
        }
        //计算字段
        if (StringUtils.isNotBlank(rule.getMap())) {
            Map<String, String> columnMap = null;
            try {
                columnMap = jsonMapper.readValue(rule.getMap(), Map.class);
            } catch (JsonProcessingException e) {
                throw new ShouldNeverHappenException("parse rule of map is failed, please make sure that map is a valid json。map:" + rule.getMap());
            }
            if (Objects.isNull(columnMap)) {
                throw new ShouldNeverHappenException("parse result is null, map:" + rule.getMap());
            }
            for (String columnName : tableMeta.getAllColumnMap().keySet()) {
                ColumnMeta columnMeta = tableMeta.getAllColumnMap().get(columnName);
                String mapValue = columnMap.get(columnName);
                if (StringUtils.isBlank(mapValue)) {
                    //未配置规则，统一化为小写
                    columnMeta.setTargetColumnName(columnName.toLowerCase());
                    mapDataTypeOfEs(columnMeta, targetType);
                    continue;
                }
                //有具体映射规则
                if (mapValue.contains(",")) {
                    //有类型规则  [0] 映射的字段名，[1] 映射的字段类型
                    String[] mapValueArray = mapValue.split(",");
                    if (StringUtils.isNotBlank(mapValueArray[0])) {
                        columnMeta.setTargetColumnName(mapValueArray[0]);
                    } else {
                        columnMeta.setTargetColumnName(columnName.toLowerCase());
                    }
                    if (StringUtils.isNotBlank(mapValueArray[1])) {
                        checkEsDataType(mapValueArray[1]);
                        columnMeta.setEsDataType(mapValueArray[1]);
                    } else {
                        mapDataTypeOfEs(columnMeta, targetType);
                    }
                } else {
                    //仅有名称映射规则
                    columnMeta.setTargetColumnName(mapValue);
                    mapDataTypeOfEs(columnMeta, targetType);
                }
            }
        } else {
            for (String columnName : tableMeta.getAllColumnMap().keySet()) {
                ColumnMeta columnMeta = tableMeta.getAllColumnMap().get(columnName);
                //未配置规则，统一化为小写
                columnMeta.setTargetColumnName(columnName.toLowerCase());
                mapDataTypeOfEs(columnMeta, targetType);
            }
        }
        return tableMeta;
    }
}
