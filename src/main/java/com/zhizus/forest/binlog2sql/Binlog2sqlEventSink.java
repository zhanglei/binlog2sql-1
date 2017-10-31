package com.zhizus.forest.binlog2sql;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.zhizus.forest.binlog2sql.hanlder.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by dempezheng on 2017/10/31.
 */
public class Binlog2sqlEventSink extends AbstractCanalLifeCycle implements CanalEventSink<List<CanalEntry.Entry>> {
    private final static Logger logger = LoggerFactory.getLogger(Binlog2sqlEventSink.class);
    private List<String> sqlList = Lists.newArrayList();

    private EventHandler eventHandler;

    public Binlog2sqlEventSink(EventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    @Override
    public boolean sink(List<CanalEntry.Entry> entrys, InetSocketAddress remoteAddress, String destination) throws CanalSinkException, InterruptedException {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                try {
                    final CanalEntry.RowChange rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    CanalEntry.EventType eventType = rowChage.getEventType();
                    System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                            entry.getHeader().getLogfileName(),
                            entry.getHeader().getLogfileOffset(),
                            entry.getHeader().getSchemaName(),
                            entry.getHeader().getTableName(),
                            eventType));


                    String sql = eventHandler.handle(entry, rowChage);
                    if (!Strings.isNullOrEmpty(sql)) {
                        sqlList.add(sql);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }

            }
        }

        return true;
    }

    @Override
    public void interrupt() {

    }

    public List<String> getSqlList() {
        return sqlList;
    }

}
