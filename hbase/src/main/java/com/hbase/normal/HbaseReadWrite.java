package com.hbase.normal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created by ruan on 2016/5/19.
 */
public class HbaseReadWrite {

    public Configuration config;
    public HTable table;
    public HBaseAdmin admin;

    public HbaseReadWrite() {
        config = HBaseConfiguration.create();
        config.set("hbase.master", "192.168.1.118:16000");
        config.set("hbase.zookeeper.quorum", "192.168.1.118");
        try {
            table = new HTable(config, "test");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        HbaseReadWrite hbaseReadWrite = new HbaseReadWrite();
        if(args.length > 0) {
            switch (args[0]) {
                case "get":
                    if (args.length == 3) {
                        hbaseReadWrite.get(args[1], args[2], null);
                    }else if(args.length == 4) {
                        hbaseReadWrite.get(args[1], args[2], args[3]);
                    }else {
                        System.out.println("get error args");
                    }
                    break;
                case "put":hbaseReadWrite.put();
                    break;
                case "scan":
                    if (args.length == 3) {
                        hbaseReadWrite.scan(args[1], args[2], null, null);
                    }else if(args.length == 4) {
                        hbaseReadWrite.scan(args[1], args[2], args[3], null);
                    }else if(args.length == 5) {
                        hbaseReadWrite.scan(args[1], args[2], args[3], args[4]);
                    }else {
                        System.out.println("get error args");
                    }
                    break;
            }
        }else {
            System.out.println("I need args");
            hbaseReadWrite.get("e:z:02", "rzf", "m");
        }
    }

    public void put() {
        String[] begin = new String[] {"a", "b", "c", "d", "e"};
        String[] middle = new String[] {"q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};
        String[] q = new String[] {"k", "l", "m", "n"};
        for (String b : begin) {
            for (String m : middle) {
                for (int i=0;i<20;i++) {
                    String rowKey = b + ":" + m + ":" + (i<10 ? "0" + i : i);
                    Put put = new Put(Bytes.toBytes(rowKey));
                    int rcf = (int)(Math.random() * 100);
                    int rq = (int)(Math.random() * 100);
                    put.addColumn(Bytes.toBytes("rzf"), Bytes.toBytes(q[rq%q.length]), Bytes.toBytes(rcf * rq));
                    System.out.println(rowKey + ", cf:rzf, q:" + q[rq%q.length] + ", v:" + rcf*rq);
                    try {
                        table.put(put);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }

    public void get(String rowkey, String columnFamily, String column) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        if (null == column) {
            get.addFamily(Bytes.toBytes(columnFamily));
        }else {
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        }

        Scan scan = new Scan(get);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.listCells())
                System.out.println(String.format("%s, %s, %s, %s, %s",
                        Bytes.toString(CellUtil.cloneRow(cell)),
                        Bytes.toString(CellUtil.cloneFamily(cell)),
                        Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toInt(CellUtil.cloneValue(cell)),
                        cell.getTimestamp()
                ));
        }
    }

    public void scan(String startRow, String stopRow, String family, String column) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(stopRow.getBytes());
        FilterList filterList = new FilterList();
        scan.setFilter(filterList);

        /**
         * this is filter list
         * RowFilter compare rowkeys. greater then e:z:05, less then e:z:15   like startRow stopRow
         * QualifierFilter compare Qualifier. our qualifier:k l m n, so I want >= m
         * QualifierFilter compare Qualifier. our qualifier:k l m n, so I want >= m
         * ValueFilter compare value. our value less then 2000
         * SkipFilter if the qualifier has value not greater then 1000, then, this row will be skip
         */
        RowFilter growFilter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator("e:z:05".getBytes(Charset.forName("UTF-8"))));
        RowFilter browFilter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator("e:z:15".getBytes(Charset.forName("UTF-8"))));
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator("m".getBytes()));
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes(2000)));
        ValueFilter valueFilter4Sikp = new ValueFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(1000)));
        SkipFilter skipFilter = new SkipFilter(valueFilter4Sikp);

        filterList.addFilter(growFilter);
        filterList.addFilter(browFilter);
        filterList.addFilter(qualifierFilter);
        filterList.addFilter(valueFilter);
        filterList.addFilter(valueFilter4Sikp);

        filterList.addFilter(skipFilter);

        if (null != family) {
            if(null != column) {
                scan.addColumn(family.getBytes(), column.getBytes());
            }else {
                scan.addFamily(family.getBytes());
            }
        }

        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.listCells())
                System.out.println(String.format("%s, %s, %s, %s, %s",
                        Bytes.toString(CellUtil.cloneRow(cell)),
                        Bytes.toString(CellUtil.cloneFamily(cell)),
                        Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toInt(CellUtil.cloneValue(cell)),
                        cell.getTimestamp()
                ));
        }
    }
}
