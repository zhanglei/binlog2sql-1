package com.zhizus.forest.binlog2sql.hanlder;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * Created by dempezheng on 2017/10/31.
 */
public class InsertEventHandler implements EventHandler {
    @Override
    public String handle(CanalEntry.Entry entry, CanalEntry.RowChange rowChage) {
        String sql = "";
        CanalEntry.EventType eventType = rowChage.getEventType();
        for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
            if (eventType == CanalEntry.EventType.INSERT) {
                return SqlEventHandlerUtils.assembleSql(eventType, entry, rowData);
            }
        }
        return sql;
    }


}
