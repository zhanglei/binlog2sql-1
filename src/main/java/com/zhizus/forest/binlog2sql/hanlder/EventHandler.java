package com.zhizus.forest.binlog2sql.hanlder;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * Created by dempezheng on 2017/10/31.
 */
public interface EventHandler {

    String handle(CanalEntry.Entry entry, CanalEntry.RowChange rowChange);
}
