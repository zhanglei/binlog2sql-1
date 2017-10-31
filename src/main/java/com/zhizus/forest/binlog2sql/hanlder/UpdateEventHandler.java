package com.zhizus.forest.binlog2sql.hanlder;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * Created by dempezheng on 2017/10/31.
 */
public class UpdateEventHandler implements EventHandler {
    @Override
    public String handle(CanalEntry.Entry entry, CanalEntry.RowChange rowChage) {
        return null;
    }
}
