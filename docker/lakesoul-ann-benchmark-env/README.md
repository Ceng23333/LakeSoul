# LakeSoul ANN Benchmark Environment

## 运行步骤

1. 编译测试类
```bash
bash compile_test_class.sh
```
这个脚本会编译所需的测试类文件。

2. 启动所有必需的容器
```bash
bash start_all.sh
```
此脚本将启动以下服务:
- PostgreSQL
- Spark
- MinIO

## PySpark ANN 测试流程
1. 准备 ANN 测试表
```bash
bash run_sql_ann_load.sh
```

### run_sql_ann_load.sh 超参数说明

* **WAREHOUSE** (`s3a://lakesoul-test-bucket/lakesoul-test`): 
  - LakeSoul表数据存储的S3位置

* **TABLE_NAME** (`mnist_sql_ann_table`):
  - 在LakeSoul中创建的表名

* **HDF5_FILE** (`/data/embeddings/fashion-mnist-784-euclidean.hdf5`):
  - 包含Fashion MNIST数据集嵌入向量的HDF5文件路径

* **EMBEDDING_DIM** (`784`):
  - 嵌入向量的维度（784对应于展平的28x28像素图像）

* **NUM_HASH_TABLES** (`784`):
  - 要创建的LSH哈希表数量。更多哈希表提高召回率但增加存储和计算需求
  - 此值较高（等于嵌入维度），表示期望高精度

* **SAMPLE_RATIO** (`1.0`):
  - 要使用的数据集比例（1.0表示使用完整数据集）
  - 可以减小以处理内存限制

* **Spark配置参数**:
  - `--driver-memory 6g`: 分配给Spark驱动程序的内存
  - `--executor-memory 6g`: 分配给Spark执行器的内存
  - `spark.memory.offHeap.enabled=true`: 启用堆外内存使用
  - `spark.memory.offHeap.size=2g`: 设置2GB的堆外内存
  - `spark.memory.fraction=0.8`: 用于执行和存储的堆空间比例
  - `spark.sql.shuffle.partitions=8`: 洗牌数据时使用的分区数


2. 运行查询测试
```bash
bash run_sql_ann_query.sh
```

### run_sql_ann_query.sh 超参数说明

* **TABLE_NAME** (`mnist_sql_ann_table`):
  - 要查询的LakeSoul表名

* **HDF5_FILE** (`/data/embeddings/fashion-mnist-784-euclidean.hdf5`):
  - 包含用作查询的测试嵌入向量的HDF5文件路径

* **QUERY_LIMIT** (`5`):
  - 从测试集中使用的查询向量数量

* **TOPK** (`10`):
  - 为每个查询返回的最近邻数量

* **WAREHOUSE** (`s3a://lakesoul-test-bucket/lakesoul-test`):
  - 存储LakeSoul表的S3位置

* **PRE_RANK_SIZE** (`100000`):
  - 在选择top-k之前预先排名的候选数量
  - 这个大值表示对候选项进行彻底搜索

* **COMPUTE_RECALL** (`true`):
  - 是否计算召回率统计（与精确搜索比较）
  - 如果计算太慢，可以设置为false



## Scala Class ANN 测试流程
1. 准备 ANN 测试表
```bash
bash prepare_ann_table.sh
```
此脚本将创建并填充用于 ANN 测试的表。

4. 运行查询测试
```bash
bash run_query.sh
```
此脚本将执行预定义的查询测试用例。

