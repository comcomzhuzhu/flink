package com.zx.redistributing;

/**
 *
 * hash  mumu hash
 *
 * 八大分区策略 即 StreamPartitioner 的八个实现类
 *
 * shuffle  rebalance 从数据源出来即是  当onetoone 但并行度不一样
 *
 * rescale  分组轮询   global  全部发往第一个0并行度
 * forward 最普通的 跟随
 * broadcast  广播
 *
 * hash  keyBy触发
 *
 */
public class R1_KeyBy {
}
