package com.zhizus.forest.binlog2sql.hanlder;

import com.alibaba.otter.canal.protocol.CanalEntry;

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
            // TODO 判断是否真正的update=true
            case INSERT:
                baseSql.append("INSERT INTO ").append(entry.getHeader().getTableName()).append(" (");
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                print(afterColumnsList);
                for (int i = 0; i < afterColumnsList.size(); i++) {
                    CanalEntry.Column column = afterColumnsList.get(i);
                    if (column.getUpdated() != true) {
                        continue;
                    }
                    baseSql.append(column.getName());
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
                    baseSql.append(column.getValue());
                    if (afterColumnsList.size() != i + 1) {
                        baseSql.append(",");
                    }
                }
                baseSql.append(")").append(";");

            case UPDATE:
                baseSql.append("UPDATE  ").append(entry.getHeader().getTableName()).append(" set ");
                List<CanalEntry.Column> afterUpdateColumnsList = rowData.getAfterColumnsList();
                for (int i = 0; i < afterUpdateColumnsList.size(); i++) {
                    CanalEntry.Column column = afterUpdateColumnsList.get(i);
                    if (column.getUpdated() != true) {
                        continue;
                    }
                    baseSql.append(column.getName()).append("=").append(column.getValue());
                }
                print(afterUpdateColumnsList);

            case ALTER:
                List<CanalEntry.Column> afterAlertColumnsList = rowData.getAfterColumnsList();
                print(afterAlertColumnsList);
            default:
                break;

        }
        return baseSql.toString();

    }


    public static void print(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}
