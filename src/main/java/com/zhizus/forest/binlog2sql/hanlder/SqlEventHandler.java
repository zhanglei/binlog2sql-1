package com.zhizus.forest.binlog2sql.hanlder;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * Created by dempezheng on 2017/10/31.
 */
public class SqlEventHandler implements EventHandler {
    @Override
    public String handle(CanalEntry.Entry entry, CanalEntry.RowChange rowChange) {
        String sql = "";
        System.out.println("---->>" + entry);
        CanalEntry.EventType eventType = rowChange.getEventType();
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            return SqlAssembleUtil.assembleSql(eventType, entry, rowData);
        }

        return sql;
    }


}
