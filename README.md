# Parquet 到 JSONL 转换工具

将 Parquet 数据文件高效转换为 JSONL (JSON Lines) 格式的命令行工具。

## 快速开始

### 构建

```bash
cargo build --release
```

### 运行

```bash
./target/release/dataset_handler -i <input.parquet> -o <output.jsonl>
```

## 命令行参数

*   `-i, --parquet-input <PARQUET_INPUT>`: 
    *   **必需**。输入的 Parquet 文件路径。
    
*   `-o, --jsonl-output <JSONL_OUTPUT>`: 
    *   **必需**。输出的 JSONL 文件路径。
    
*   `-b, --batch-size <BATCH_SIZE>`: 
    *   **可选**。每批处理的行数，影响内存使用和处理效率。
    *   默认值: `16384`
    
*   `-c, --channel-buffer <CHANNEL_BUFFER>`: 
    *   **可选**。任务间通信缓冲区大小。
    *   默认值: `10`
    
*   `--projection <PROJECTION>`: 
    *   **可选**。只转换指定的列，多列用逗号分隔。
    *   示例: `--projection "id,name,timestamp"`
    
*   `--num-threads <NUM_THREADS>`: 
    *   **可选**。设置并行处理的线程数，默认使用系统可用线程数。
    *   默认值: `0`（自动）
    
*   `--use-simd-json`: 
    *   **可选**。启用 SIMD 加速 JSON 序列化（需要编译时支持）。

## 使用示例

### 基本用法

```bash
./target/release/dataset_handler \
    -i data/input.parquet \
    -o data/output.jsonl
```

### 处理大文件 (优化内存使用)

```bash
./target/release/dataset_handler \
    -i data/large_dataset.parquet \
    -o data/output.jsonl \
    -b 8192 \
    -c 5
```

### 只选择特定列

```bash
./target/release/dataset_handler \
    -i data/events.parquet \
    -o data/filtered_events.jsonl \
    --projection "user_id,event_name,timestamp"
```

### 性能优化

```bash
./target/release/dataset_handler \
    -i data/input.parquet \
    -o data/output.jsonl \
    -b 32768 \
    --num-threads 8
```

## 性能提示

1. 增大 `batch-size` 可提高吞吐量，但会增加内存消耗
2. 在多核系统上，使用 `--num-threads` 设置合适的线程数可提高并行效率
3. 使用 `--projection` 只选择需要的列可显著减少处理时间和内存使用
4. 对于 SSD 存储，可适当增大 `-c` 参数以提高 I/O 利用率 
