# Parquet 到 JSON 转换工具

将 Parquet 数据文件高效转换为 JSON 数组格式的命令行工具。

## 快速开始

### 构建

```bash
cargo build --release
```

### 运行

```bash
./target/release/dataset_handler -p <input.parquet> -j <output.json>
```

## 命令行参数

*   `-p, --parquet-input <PARQUET_INPUT>`: 
    *   **必需**。输入的 Parquet 文件路径。
    
*   `-j, --json-output <JSON_OUTPUT>`: 
    *   **必需**。输出的 JSON 文件路径。
    
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
    *   **可选**。设置并行处理的线程数，0 表示使用默认配置。
    *   默认值: `0`（使用默认 Rayon 线程池配置）
    
*   `--use-simd-json`: 
    *   **可选**。启用 SIMD 加速 JSON 序列化（需要编译时支持）。

## 使用示例

### 基本用法

```bash
./target/release/dataset_handler \
    --parquet-input data/input.parquet \
    --json-output data/output.json
```

或使用短参数：

```bash
./target/release/dataset_handler \
    -p data/input.parquet \
    -j data/output.json
```

### 处理大文件 (优化内存使用)

```bash
./target/release/dataset_handler \
    --parquet-input data/large_dataset.parquet \
    --json-output data/output.json \
    --batch-size 8192 \
    --channel-buffer 5
```

### 只选择特定列

```bash
./target/release/dataset_handler \
    --parquet-input data/events.parquet \
    --json-output data/filtered_events.json \
    --projection "user_id,event_name,timestamp"
```

### 性能优化 (多线程处理)

```bash
./target/release/dataset_handler \
    --parquet-input data/input.parquet \
    --json-output data/output.json \
    --batch-size 32768 \
    --num-threads 8
```

### 启用 SIMD JSON 优化

```bash
./target/release/dataset_handler \
    --parquet-input data/input.parquet \
    --json-output data/output.json \
    --use-simd-json
```

### 完整参数示例

```bash
./target/release/dataset_handler \
    --parquet-input data/large_dataset.parquet \
    --json-output data/output.json \
    --batch-size 16384 \
    --channel-buffer 10 \
    --projection "id,name,value,timestamp" \
    --num-threads 4 \
    --use-simd-json
```

## 性能提示

1. **批处理大小**: 增大 `--batch-size` 可提高吞吐量，但会增加内存消耗。推荐值：8192-32768
2. **多线程处理**: 在多核系统上，使用 `--num-threads` 设置合适的线程数可提高 Rayon 并行处理效率
3. **列投影**: 使用 `--projection` 只选择需要的列可显著减少处理时间和内存使用
4. **通道缓冲**: 对于高性能存储，可适当增大 `--channel-buffer` 参数以提高 I/O 利用率
5. **SIMD 优化**: 在支持的平台上启用 `--use-simd-json` 可加速 JSON 序列化
6. **输出格式**: 工具输出完整的 JSON 数组格式，适合后续处理和解析

## 注意事项

- 输出文件采用 JSON 数组格式 `[{...}, {...}, ...]`，而非 JSONL 格式
- 所有行数据会被收集到内存中形成完整的 JSON 数组后写入
- 对于超大数据集，请考虑内存使用情况并适当调整批处理大小 
