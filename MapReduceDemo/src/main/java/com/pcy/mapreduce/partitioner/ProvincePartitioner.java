package com.pcy.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author PengChenyu
 * @since 2021-08-12 18:10:12
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    /**
     * Get the partition number for a given key (hence record) given the total
     * number of partitions i.e. number of reduce-tasks for the job.
     *
     * <p>Typically a hash function on a all or a subset of the key.</p>
     *
     * @param text          the key to be partioned.
     * @param flowBean      the entry value.
     * @param numPartitions the total number of partitions.
     * @return the partition number for the <code>key</code>.
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        // text 是手机号
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);
        int partition;
        switch (prePhone) {
            case "136":
                partition = 0;
                break;
            case "137":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "139":
                partition = 3;
                break;
            default:
                partition = 4;
                break;
        }
        return partition;
    }
}
