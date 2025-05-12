# Parquet 到 JSONL 高效转换工具

这是一个基于 Rust 编写的命令行工具，用于将 Parquet 数据文件高效地转换为 JSONL (JSON Lines) 格式。它利用异步处理和并行计算来优化大规模数据集的转换速度。

## 主要特性

*   **异步 I/O**: 使用 `tokio` 进行异步文件读取和写入，避免阻塞主线程，提高 I/O 效率。
*   **并行行处理**: 使用 `rayon` 对每个 RecordBatch 内的行数据进行并行 JSON 转换，充分利用多核 CPU 资源。
*   **批处理**: 数据以可配置大小的 RecordBatch 为单位进行读取和处理，平衡内存使用和处理效率。
*   **列选择 (Projection)**: 支持选择 Parquet 文件中的特定列进行转换，减少不必要的数据处理和输出。
*   **通道缓冲**: 在读取、处理和写入任务之间使用有界通道（`tokio::sync::mpsc`）进行数据传递，实现任务解耦和背压控制。
*   **详细日志**: 在关键操作点输出日志，方便追踪程序执行状态和进行调试。
*   **错误处理**: 定义了统一的 `ConversionError` 枚举来封装不同来源的错误，并在任务失败时优雅地停止。
*   **命令行界面**: 使用 `clap` 解析命令行参数，提供用户友好的操作接口。
*   **进度指示**: 在写入任务中，会定期打印已处理的行数，提供转换进度反馈。

## 构建与运行

### 前提条件

*   Rust 工具链 (推荐最新稳定版)

### 构建

在项目根目录下执行：

```bash
cargo build --release
```

构建产物将位于 `target/release/` 目录下。

### 运行

```bash
./target/release/dataset_handler \
    -i <input_parquet_file> \
    -o <output_jsonl_file> \
    [-b <batch_size>] \
    [-c <channel_buffer_size>] \
    [--projection <col1,col2,...>]
```

## 使用方法

### 命令行参数

*   `-i, --parquet-input <PARQUET_INPUT>`:
    *   必需。指定输入的 Parquet 文件路径。
*   `-o, --jsonl-output <JSONL_OUTPUT>`:
    *   必需。指定输出的 JSONL 文件路径。
*   `-b, --batch-size <BATCH_SIZE>`:
    *   可选。Parquet 读取器一次读取的行数 (RecordBatch 大小)。
    *   默认值: `16384`
*   `-c, --channel-buffer <CHANNEL_BUFFER>`:
    *   可选。任务间通信通道的缓冲区大小。
    *   默认值: `10`
*   `--projection <PROJECTION>`:
    *   可选。指定要从 Parquet 文件中选择的列名，多个列名之间用逗号分隔。
    *   如果未提供，则转换所有列。
    *   示例: `--projection "column_a,column_c,column_f"`

### 示例

```bash
./target/release/dataset_handler \
    --parquet-input data/my_large_dataset.parquet \
    --jsonl-output output/converted_data.jsonl \
    --batch-size 32768 \
    --channel-buffer 20 \
    --projection "user_id,event_name,timestamp"
```

## 错误处理

程序定义了一个 `ConversionError` 枚举，用于统一处理来自不同操作（如文件 I/O、Parquet 解析、JSON 序列化、Arrow 库操作、通道通信等）的错误。

*   在 `reader_task` 中，如果无法打开或读取 Parquet 文件，或发送数据到通道失败，任务会记录错误并终止。
*   在 `processor_task` 中，如果从通道接收数据失败或发送处理结果失败，任务会记录错误并终止。它也会将从 `reader_task` 收到的错误向后传递。
*   在 `writer_task` 中，如果无法创建或写入输出文件，或从通道接收数据失败，任务会记录错误并终止。它同样会将从 `processor_task` 收到的错误向后传递。
*   `main` 函数会捕获 `run_conversion` 返回的最终结果，并在发生错误时打印错误信息。

## 主要依赖项

*   `arrow-array`, `arrow-schema`: 用于处理 Apache Arrow 格式数据。
*   `parquet`: 用于读取 Parquet 文件格式。
*   `tokio`: 异步运行时，用于处理并发 I/O 和任务调度。
*   `rayon`: 数据并行库，用于并行处理 RecordBatch 中的行。
*   `serde_json`: 用于 JSON 数据的序列化和反序列化。
*   `clap`: 用于解析命令行参数。

## 测试

项目包含单元测试和集成测试，用于验证核心转换逻辑、列选择、不同批处理大小以及边界情况（如空文件）的正确性。

要运行测试，请在项目根目录下执行：

```bash
cargo test
```

测试会创建临时的 Parquet 文件进行转换，并在测试完成后清理这些文件。 