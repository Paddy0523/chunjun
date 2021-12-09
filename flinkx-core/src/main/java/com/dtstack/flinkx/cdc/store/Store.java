package com.dtstack.flinkx.cdc.store;

import com.dtstack.flinkx.cdc.QueuesChamberlain;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Deque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/3 星期五
 */
public abstract class Store implements Runnable, Serializable {

    protected QueuesChamberlain chamberlain;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected CopyOnWriteArrayList<String> storedTableIdentifier;

    @Override
    public void run() {
        while (!closed.get()) {
            for (String table : chamberlain.getTableIdentifierFromBlockQueues()) {
                // 如果数据已经被下发了，那么就跳过
                if (storedTableIdentifier.contains(table)) {
                    continue;
                }
                // 将block的ddl数据下发到外部数据源中
                Deque<RowData> rowData = chamberlain.getQueueFromBlockQueues(table);
                RowData data = rowData.peekFirst();
                store(data);
                storedTableIdentifier.add(table);
            }
        }
    }

    public void setChamberlain(QueuesChamberlain chamberlain) {
        this.chamberlain = chamberlain;
    }

    public void setStoredTableIdentifier(CopyOnWriteArrayList<String> storedTableIdentifier) {
        this.storedTableIdentifier = storedTableIdentifier;
    }

    public void close() {
        closed.compareAndSet(false, true);
        closeSubclass();
    }

    /**
     * 存储row data.
     *
     * @param data row data
     */
    public abstract void store(RowData data);

    /**
     * open sub-class
     *
     * @throws Exception exception
     */
    public abstract void open() throws Exception;

    /** class sub-class. */
    public abstract void closeSubclass();
}
