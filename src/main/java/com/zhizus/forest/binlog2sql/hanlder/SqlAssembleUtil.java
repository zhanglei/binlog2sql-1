package com.zhizus.forest.binlog2sql.hanlder;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Created by dempezheng on 2017/10/31.
 */
public class SqlAssembleUtil {


    /**
     * @param eventType
     * @param entry
     * @param rowData
     * @return
     */
    public static String assembleSql(CanalEntry.EventType eventType, CanalEntry.Entry entry, CanalEntry.RowData rowData) {
        StringBuilder baseSql = new StringBuilder();
        switch (eventType) {
            case INSERT:
                baseSql.append("INSERT INTO ").append(entry.getHeader().getTableName()).append(" (");
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                print(afterColumnsList);
                for (int i = 0; i < afterColumnsList.size(); i++) {
                    CanalEntry.Column column = afterColumnsList.get(i);
                    System.out.println(">>>>>>>>>>" + column);
                    if (column.getUpdated() != true) {
                        continue;
                    }
                    baseSql.append("'").append(column.getName()).append("'");
                    if (afterColumnsList.size() != i + 1) {
                        baseSql.append(",");
                    }
                }
                baseSql.append(")").append(" VALUES (");
                for (int i = 0; i < afterColumnsList.size(); i++) {
                    CanalEntry.Column column = afterColumnsList.get(i);
                    if (column.getUpdated() != true) {
                        continue;
                    }
                    baseSql.append(withValue(column));
                    if (afterColumnsList.size() != i + 1) {
                        baseSql.append(",");
                    }
                }
                baseSql.append(")");
                break;

            case UPDATE:
                baseSql.append("UPDATE  ").append(entry.getHeader().getTableName()).append(" SET ");
                List<CanalEntry.Column> beforeUpdateColumnsList = rowData.getBeforeColumnsList();
                String whereCondition = wrapWhereCondition(beforeUpdateColumnsList);
                List<CanalEntry.Column> afterUpdateColumnsList = rowData.getAfterColumnsList();
                for (int i = 0; i < afterUpdateColumnsList.size(); i++) {
                    CanalEntry.Column column = afterUpdateColumnsList.get(i);
                    if (column.getUpdated() != true) {
                        continue;
                    }
                    baseSql.append(column.getName()).append("=").append(withValue(column));
                }

                if (!Strings.isNullOrEmpty(whereCondition)) {
                    baseSql.append(whereCondition);
                }
                break;

            case ALTER:
                List<CanalEntry.Column> afterAlertColumnsList = rowData.getAfterColumnsList();
                print(afterAlertColumnsList);
                break;
            default:
                break;

        }
        return wrapEnd(baseSql.toString());

    }


    public static String wrapWhereCondition(List<CanalEntry.Column> beforeUpdateColumnsList) {
        StringBuilder whereCondition = new StringBuilder();
        for (int i = 0; i < beforeUpdateColumnsList.size(); i++) {
            CanalEntry.Column column = beforeUpdateColumnsList.get(i);
            whereCondition.append(column.getName()).append("=").append(withValue(column));
            if (beforeUpdateColumnsList.size() > 0 && beforeUpdateColumnsList.size() != i + 1) {
                whereCondition.append(" AND ");
            }
        }
        if (whereCondition.length() > 0) {
            return " WHERE " + whereCondition.toString();
        }
        return "";
    }

    public static String withValue(CanalEntry.Column column) {
        if (StringUtils.startsWith(column.getMysqlType(), "varchar")) {
            return "'" + column.getValue() + "'";
        } else if (StringUtils.startsWith(column.getMysqlType(), "bit")) {
            return "'" + column.getValue() + "'";
        } else {
            return column.getValue();
        }
    }

    public static String wrapEnd(String sql) {
        return sql + ";";
    }

    public static void print(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}
