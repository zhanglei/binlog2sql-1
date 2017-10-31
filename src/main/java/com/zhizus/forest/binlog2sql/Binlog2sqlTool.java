package com.zhizus.forest.binlog2sql;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mysql.LocalBinlogEventParser;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.google.common.collect.Maps;
import com.zhizus.forest.binlog2sql.hanlder.SqlEventHandler;
import org.junit.Assert;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Created by dempezheng on 2017/10/31.
 */
public class Binlog2sqlTool {

    private static Map<String, LogPosition> logPositionMap = Maps.newHashMap();

    public static void main(String[] args) {
        String directory = "C:\\Users\\dempezheng.KUGOU\\Downloads";
        final LocalBinlogEventParser controller = new LocalBinlogEventParser();
        final EntryPosition startPosition = new EntryPosition("mysql-bin.000006", 4L);

        controller.setMasterInfo(new AuthenticationInfo(new InetSocketAddress("192.168.147.129", 3306), "root", "mysql"));
        controller.setConnectionCharset(Charset.forName("UTF-8"));
        controller.setDirectory(directory);
        controller.setMasterPosition(startPosition);
        SqlEventHandler handler = new SqlEventHandler();
        Binlog2sqlEventSink eventSink = new Binlog2sqlEventSink(handler);
        controller.setEventSink(eventSink);

        controller.setLogPositionManager(new AbstractLogPositionManager() {
            public LogPosition getLatestIndexBy(String destination) {
                return logPositionMap.get(destination);
            }
            public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
                System.out.println(logPosition);
                logPositionMap.put(destination, logPosition);
            }
        });

        controller.start();

        try {
            Thread.sleep(10 * 1000L);
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
        controller.stop();
        List<String> sqlStr = eventSink.getSqlList();
        sqlStr.forEach(sql -> System.out.println(sql));


    }
}
