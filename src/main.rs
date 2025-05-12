use std::fs::File as StdFile;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tokio::fs::File as TokioFile;
use tokio::io::{AsyncWriteExt, BufWriter as AsyncBufWriter};
use tokio::sync::mpsc;

use clap::Parser;

use arrow_array::RecordBatch;
use arrow_array::array::{
    Array, ArrayRef, BooleanArray, GenericStringArray,
    Int8Array, Int16Array, Int32Array, Int64Array,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    Float32Array, Float64Array,
    Date32Array, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    ListArray, StructArray, LargeListArray,
};
use arrow_schema::{DataType, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::ParquetError;
use arrow::record_batch::RecordBatchReader;

use serde_json::Value as JsonValue;

use rayon::prelude::*;

// --- 错误类型封装 (与 Tokio + Rayon 版本相同) ---
#[derive(Debug)]
enum ConversionError {
    Io(Arc<IoError>),
    StdIo(String),
    Parquet(ParquetError),
    Json(serde_json::Error),
    Arrow(arrow_schema::ArrowError),
    UnsupportedType(String),
    ChannelSendError(String),
    ChannelRecvError,
    TaskJoinError(tokio::task::JoinError),
}

impl From<std::io::Error> for ConversionError { fn from(err: std::io::Error) -> Self { ConversionError::StdIo(err.to_string()) } }
impl From<ParquetError> for ConversionError { fn from(err: ParquetError) -> Self { ConversionError::Parquet(err) } }
impl From<serde_json::Error> for ConversionError { fn from(err: serde_json::Error) -> Self { ConversionError::Json(err) } }
impl From<arrow_schema::ArrowError> for ConversionError { fn from(err: arrow_schema::ArrowError) -> Self { ConversionError::Arrow(err) } }
impl<T> From<mpsc::error::SendError<T>> for ConversionError {
    fn from(err: mpsc::error::SendError<T>) -> Self { ConversionError::ChannelSendError(err.to_string()) }
}
impl From<mpsc::error::TryRecvError> for ConversionError {
    fn from(_: mpsc::error::TryRecvError) -> Self { ConversionError::ChannelRecvError }
}
impl From<tokio::task::JoinError> for ConversionError {
    fn from(err: tokio::task::JoinError) -> Self { ConversionError::TaskJoinError(err)}
}

type Result<T, E = ConversionError> = std::result::Result<T, E>;

/// 将 Arrow 数组中的单个值转换为 serde_json::Value (与之前版本相同)
fn arrow_value_to_json_value(array: &dyn Array, row_index: usize) -> Result<JsonValue> {
    if array.is_null(row_index) {
        return Ok(JsonValue::Null);
    }
    match array.data_type() {
        DataType::Boolean => Ok(JsonValue::Bool(array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row_index))),
        DataType::Int8 => Ok(JsonValue::Number(serde_json::Number::from(array.as_any().downcast_ref::<Int8Array>().unwrap().value(row_index)))),
        DataType::Int16 => Ok(JsonValue::Number(serde_json::Number::from(array.as_any().downcast_ref::<Int16Array>().unwrap().value(row_index)))),
        DataType::Int32 => Ok(JsonValue::Number(serde_json::Number::from(array.as_any().downcast_ref::<Int32Array>().unwrap().value(row_index)))),
        DataType::Int64 => Ok(JsonValue::Number(serde_json::Number::from(array.as_any().downcast_ref::<Int64Array>().unwrap().value(row_index)))),
        DataType::UInt8 => Ok(JsonValue::Number(serde_json::Number::from(array.as_any().downcast_ref::<UInt8Array>().unwrap().value(row_index)))),
        DataType::UInt16 => Ok(JsonValue::Number(serde_json::Number::from(array.as_any().downcast_ref::<UInt16Array>().unwrap().value(row_index)))),
        DataType::UInt32 => Ok(JsonValue::Number(serde_json::Number::from(array.as_any().downcast_ref::<UInt32Array>().unwrap().value(row_index)))),
        DataType::UInt64 => Ok(JsonValue::Number(serde_json::Number::from(array.as_any().downcast_ref::<UInt64Array>().unwrap().value(row_index)))),
        DataType::Float32 => {
            let value = array.as_any().downcast_ref::<Float32Array>().unwrap().value(row_index) as f64;
            match serde_json::Number::from_f64(value) {
                Some(n) => Ok(JsonValue::Number(n)),
                None => Ok(JsonValue::Null)
            }
        },
        DataType::Float64 => {
            let value = array.as_any().downcast_ref::<Float64Array>().unwrap().value(row_index);
            match serde_json::Number::from_f64(value) {
                Some(n) => Ok(JsonValue::Number(n)),
                None => Ok(JsonValue::Null)
            }
        },
        DataType::Utf8 => Ok(JsonValue::String(array.as_any().downcast_ref::<GenericStringArray<i32>>().unwrap().value(row_index).to_string())),
        DataType::LargeUtf8 => Ok(JsonValue::String(array.as_any().downcast_ref::<GenericStringArray<i64>>().unwrap().value(row_index).to_string())),
        DataType::Timestamp(unit, _tz) => {
            let ts_string = match unit {
                TimeUnit::Second => array.as_any().downcast_ref::<TimestampSecondArray>().unwrap().value_as_datetime(row_index).map(|dt| dt.and_utc().to_rfc3339()),
                TimeUnit::Millisecond => array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value_as_datetime(row_index).map(|dt| dt.and_utc().to_rfc3339()),
                TimeUnit::Microsecond => array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value_as_datetime(row_index).map(|dt| dt.and_utc().to_rfc3339()),
                TimeUnit::Nanosecond => array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value_as_datetime(row_index).map(|dt| dt.and_utc().to_rfc3339()),
            };
            match ts_string { Some(s) => Ok(JsonValue::String(s)), None => Ok(JsonValue::Null), }
        }
        DataType::Date32 => Ok(array.as_any().downcast_ref::<Date32Array>().unwrap().value_as_date(row_index).map(|d| JsonValue::String(d.to_string())).unwrap_or(JsonValue::Null)),
        DataType::Date64 => Ok(array.as_any().downcast_ref::<Date64Array>().unwrap().value_as_datetime(row_index).map(|dt| JsonValue::String(dt.date().to_string())).unwrap_or(JsonValue::Null)),
        DataType::List(_field) => { // field parameter not used here, but kept for signature consistency
            let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let list_values_array = list_array.value(row_index);
            let mut json_array = Vec::with_capacity(list_values_array.len());
            for i in 0..list_values_array.len() {
                json_array.push(arrow_value_to_json_value(list_values_array.as_ref(), i)?);
            }
            Ok(JsonValue::Array(json_array))
        }
        DataType::LargeList(_field) => { // field parameter not used here
            let list_array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let list_values_array = list_array.value(row_index);
            let mut json_array = Vec::with_capacity(list_values_array.len());
            for i in 0..list_values_array.len() {
                json_array.push(arrow_value_to_json_value(list_values_array.as_ref(), i)?);
            }
            Ok(JsonValue::Array(json_array))
        }
        DataType::Struct(fields) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut json_object = serde_json::Map::new();
            for (i, field_item) in fields.iter().enumerate() {
                let field_array = struct_array.column(i);
                json_object.insert(field_item.name().clone(), arrow_value_to_json_value(field_array.as_ref(), row_index)?);
            }
            Ok(JsonValue::Object(json_object))
        }
        other_type => Err(ConversionError::UnsupportedType(format!("{:?}", other_type))),
    }
}

// Task 1: Reads Parquet file and sends RecordBatches via a channel (Optimized version)
async fn reader_task(
    parquet_path: PathBuf,
    batch_size: usize,
    projection: Option<Vec<String>>,
    tx: mpsc::Sender<Result<RecordBatch>>,
) -> Result<()> {
    println!("[Reader] Task started.");
    let result = tokio::task::spawn_blocking(move || {
        // 使用标准文件I/O，因为ParquetReader需要满足ChunkReader trait
        let file = match StdFile::open(&parquet_path) {
            Ok(f) => f,
            Err(e) => return Err(ConversionError::StdIo(format!("Failed to open parquet file {:?}: {}", parquet_path, e)))
        };
        
        // 检查文件大小，用于日志记录
        let file_size = match file.metadata() {
            Ok(metadata) => {
                let size_mb = metadata.len() as f64 / (1024.0 * 1024.0);
                println!("[Reader] Opening Parquet file of size: {:.2} MB", size_mb);
                metadata.len()
            },
            Err(e) => {
                eprintln!("[Reader] Warning: Could not get file size for {:?}: {}", parquet_path, e);
                0 // 默认值，继续处理
            }
        };
        
        // 初始化Parquet读取器
        let mut builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
            Ok(b) => b,
            Err(e) => return Err(ConversionError::Parquet(e))
        };

        // 更高效的列投影处理
        if let Some(cols_to_select) = &projection {
            let schema = builder.schema().clone();
            let fields = schema.fields();
            let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
            
            // 构建投影索引
            let mut projection_indices = Vec::with_capacity(cols_to_select.len());
            let mut missing_columns = Vec::new();
            
            for col_name in cols_to_select {
                if let Some(position) = field_names.iter().position(|&name| name == col_name) {
                    projection_indices.push(position);
                } else {
                    missing_columns.push(col_name.as_str());
                }
            }
            
            // 记录找不到的列
            if !missing_columns.is_empty() {
                eprintln!("[Reader] Warning: The following projection columns were not found in Parquet schema: {:?}", missing_columns);
                eprintln!("[Reader] Available columns are: {:?}", field_names);
            }
            
            if !projection_indices.is_empty() {
                let parquet_schema_descr_clone = builder.parquet_schema();
                let projection_mask = parquet::arrow::ProjectionMask::roots(
                    &parquet_schema_descr_clone,
                    projection_indices,
                );
                
                // 应用投影
                builder = builder.with_projection(projection_mask);
            }
        }

        // 构建reader并优化批处理大小
        let mut reader = match builder.with_batch_size(batch_size).build() {
            Ok(r) => r,
            Err(e) => return Err(ConversionError::Arrow(e.into()))
        };
        
        // 打印有效Schema信息
        println!("[Reader] Parquet reader initialized. Effective Arrow schema: {:?}",
                 reader.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        
        // 启动预取逻辑
        let mut prefetch_batch: Option<Result<RecordBatch>> = None;
        let mut batch_count = 0;
        
        // 预取第一个批次
        prefetch_batch = match reader.next() {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(e)) => Some(Err(ConversionError::Arrow(e))),
            None => None
        };
        
        // 主循环：处理当前批次并预取下一个批次
        loop {
            match prefetch_batch.take() {
                Some(Ok(record_batch)) => {
                    batch_count += 1;
                    
                    // 预取下一个批次
                    let next_batch = match reader.next() {
                        Some(Ok(batch)) => Some(Ok(batch)),
                        Some(Err(e)) => Some(Err(ConversionError::Arrow(e))),
                        None => None
                    };
                    
                    // 发送当前批次
                    if tx.blocking_send(Ok(record_batch)).is_err() {
                        eprintln!("[Reader] Receiver dropped for record batches. Stopping.");
                        break;
                    }
                    
                    // 更新预取批次
                    prefetch_batch = next_batch;
                    
                    // 如果没有更多批次，完成处理
                    if prefetch_batch.is_none() {
                        println!("[Reader] Finished reading {} batches.", batch_count);
                        break;
                    }
                }
                Some(Err(e)) => {
                    eprintln!("[Reader] Error reading record batch: {:?}", e);
                    if tx.blocking_send(Err(e)).is_err() {
                        eprintln!("[Reader] Receiver dropped while sending error. Stopping.");
                    }
                    break;
                }
                None => {
                    // 没有数据可读
                    println!("[Reader] No data to read (empty file or all filtered out). Batches read: {}", batch_count);
                    break;
                }
            }
        }
        
        Ok::<(), ConversionError>(())
    }).await?;
    result
}

// Task 2: Receives RecordBatches, processes them PARALLELLY, and sends JSON strings
async fn processor_task(
    mut rx: mpsc::Receiver<Result<RecordBatch>>,
    tx: mpsc::Sender<Result<Vec<Result<String>>>>,
    total_rows_processed_tx: mpsc::Sender<u64>,
    use_simd_json: bool,
) -> Result<()> {
    println!("[Processor] Task started (parallel row processing with Rayon).");
    let mut batch_number = 0;
    
    // 预分配一个大的缓冲区，避免频繁分配内存
    let mut batch_buffer: Vec<u8> = Vec::with_capacity(1024 * 1024); // 1MB 初始缓冲区
    
    while let Some(batch_result) = rx.recv().await {
        match batch_result {
            Ok(record_batch) => {
                batch_number += 1;
                let num_rows_in_batch = record_batch.num_rows();
                if num_rows_in_batch == 0 {
                    if tx.send(Ok(Vec::new())).await.is_err() {
                        eprintln!("[Processor] Receiver dropped for processed batches. Stopping.");
                        break;
                    }
                    continue;
                }

                // 估算每行数据的大小，以便更准确地预分配内存
                let estimated_row_size = record_batch.schema().fields().len() * 50; // 假设每个字段平均50字节
                
                // Offload PARALLEL processing of the batch to a blocking thread pool (Rayon)
                let processed_data_result = tokio::task::spawn_blocking(move || {
                    let schema_ref = record_batch.schema();
                    let column_count = schema_ref.fields().len();
                    
                    // 提前获取所有列的引用，避免在循环中重复获取
                    let columns: Vec<&dyn Array> = (0..column_count)
                        .map(|i| record_batch.column(i).as_ref())
                        .collect();
                    
                    // 提前获取所有列名，避免在循环中重复克隆
                    let column_names: Vec<String> = schema_ref.fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect();

                    // Process rows in parallel using Rayon
                    let json_strings_results: Vec<Result<String>> = if use_simd_json {
                        #[cfg(all(target_arch = "x86_64", feature = "simd-json"))]
                        {
                            // SIMD优化的JSON处理（需要simd-json feature开启）
                            // 注意：这里是伪代码，实际项目中需要添加simd-json依赖并配置feature
                            (0..num_rows_in_batch)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let mut json_map = serde_json::Map::new();
                                    for col_idx in 0..column_count {
                                        let json_val = arrow_value_to_json_value(columns[col_idx], row_idx)?;
                                        json_map.insert(column_names[col_idx].clone(), json_val);
                                    }
                                    let final_json_object = JsonValue::Object(json_map);
                                    // 理论上可以使用simd-json，这里仍使用serde_json
                                    serde_json::to_string(&final_json_object).map_err(ConversionError::Json)
                                })
                                .collect()
                        }
                        #[cfg(not(all(target_arch = "x86_64", feature = "simd-json")))]
                        {
                            // 回退到默认实现
                            (0..num_rows_in_batch)
                                .into_par_iter()
                                .map(|row_idx| {
                                    let mut json_map = serde_json::Map::with_capacity(column_count);
                                    for col_idx in 0..column_count {
                                        let json_val = arrow_value_to_json_value(columns[col_idx], row_idx)?;
                                        json_map.insert(column_names[col_idx].clone(), json_val);
                                    }
                                    let final_json_object = JsonValue::Object(json_map);
                                    serde_json::to_string(&final_json_object).map_err(ConversionError::Json)
                                })
                                .collect()
                        }
                    } else {
                        // 原始实现但有内存优化
                        (0..num_rows_in_batch)
                            .into_par_iter()
                            .map(|row_idx| {
                                // IIFE to handle errors for each row's conversion gracefully
                                let conversion_result_for_row: Result<String> = (|| {
                                    // 预先分配合适大小的Map减少重分配
                                    let mut json_map = serde_json::Map::with_capacity(column_count);
                                    for col_idx in 0..column_count {
                                        let json_val = arrow_value_to_json_value(columns[col_idx], row_idx)?;
                                        json_map.insert(column_names[col_idx].clone(), json_val);
                                    }
                                    let final_json_object = JsonValue::Object(json_map);
                                    serde_json::to_string(&final_json_object).map_err(ConversionError::Json)
                                })();
                                conversion_result_for_row
                            })
                            .collect()
                    };

                    (json_strings_results, num_rows_in_batch)
                }).await?; // Propagate JoinError from spawn_blocking

                let (json_strings_results, rows_in_this_batch) = processed_data_result;

                if total_rows_processed_tx.send(rows_in_this_batch as u64).await.is_err(){
                    eprintln!("[Processor] Failed to send row count. Progress tracking might be affected.");
                }

                if tx.send(Ok(json_strings_results)).await.is_err() {
                    eprintln!("[Processor] Receiver dropped for processed batches. Stopping.");
                    break;
                }
                 if batch_number % 2 == 0 {
                    println!("[Processor] Processed batch #{} ({} rows) in parallel", batch_number, rows_in_this_batch);
                }
            }
            Err(e) => {
                eprintln!("[Processor] Received an error from reader task: {:?}", e);
                if tx.send(Err(e)).await.is_err() {
                     eprintln!("[Processor] Receiver dropped while sending error. Stopping.");
                }
                break;
            }
        }
    }
    println!("[Processor] Task finished.");
    Ok(())
}

// Task 3: Receives processed JSON strings and writes them to a file asynchronously (Improved version)
async fn writer_task(
    output_path: PathBuf,
    mut rx: mpsc::Receiver<Result<Vec<Result<String>>>>,
    mut progress_rx: mpsc::Receiver<u64>,
) -> Result<u64> {
    println!("[Writer] Task started. Output to: {:?}", output_path);
    let mut total_rows_acknowledged: u64 = 0; // Tracks rows confirmed by processor
    
    // 使用更大的buffer size优化写入性能
    let buffer_capacity = 8 * 1024 * 1024; // 8MB buffer
    let mut file = AsyncBufWriter::with_capacity(
        buffer_capacity,
        TokioFile::create(&output_path).await
            .map_err(|e| ConversionError::StdIo(format!("Failed to create output file {:?}: {}", output_path, e)))?
    );
    
    let mut batch_count = 0;
    let mut log_rows_written_since_last_log: u64 = 0;
    let mut total_rows_written: u64 = 0; // 添加记录实际写入的行数
    
    // 预分配大字符串缓冲区，用于批量写入
    let mut write_buffer = String::with_capacity(1024 * 1024); // 1MB 初始大小
    let flush_threshold = 4 * 1024 * 1024; // 当缓冲区超过4MB时刷新
    
    let mut data_rx_active = true;
    let mut progress_rx_active = true;

    while data_rx_active || progress_rx_active {
        tokio::select! {
            biased;
            // Data channel branch
            received_data = rx.recv(), if data_rx_active => {
                match received_data {
                    Some(Ok(batch_json_strings_results)) => {
                        batch_count +=1;
                        let mut rows_in_batch_successfully_written = 0;
                        
                        // 批量组合JSON字符串，然后一次性写入
                        write_buffer.clear(); // 确保缓冲区是空的
                        
                        for result_item in batch_json_strings_results {
                            match result_item {
                                Ok(json_string) => {
                                    write_buffer.push_str(&json_string);
                                    write_buffer.push('\n');
                                    rows_in_batch_successfully_written += 1;
                                    
                                    // 当缓冲区足够大时，执行一次写入并清空缓冲区
                                    if write_buffer.len() >= flush_threshold {
                                        file.write_all(write_buffer.as_bytes()).await?;
                                        // 重要：写入后清空缓冲区，避免重复写入
                                        write_buffer.clear();
                                    }
                                }
                                Err(e_row) => { // Renamed to e_row to avoid any ambiguity
                                    eprintln!("[Writer] Error converting a row to JSON, skipping row: {:?}", e_row);
                                }
                            }
                        }
                        
                        // 写入剩余的缓冲区内容
                        if !write_buffer.is_empty() {
                            file.write_all(write_buffer.as_bytes()).await?;
                            // 写入后必须清空，避免后续处理再次写入相同内容
                            write_buffer.clear();
                        }
                        
                        log_rows_written_since_last_log += rows_in_batch_successfully_written;
                        total_rows_written += rows_in_batch_successfully_written;
                        
                        // 大型批次处理后执行一次flush减少内存压力
                        if rows_in_batch_successfully_written > 100000 {
                            file.flush().await?;
                        }
                    }
                    Some(Err(e)) => { // Error received from processor task over the channel
                        eprintln!("[Writer] Received an error from processor task: {:?}", e);
                        file.flush().await.map_err(|io_err| ConversionError::StdIo(format!("Failed to flush writer on error: {}", io_err)))?;
                        return Err(e); // Propagate the error from processor
                    }
                    None => { // Data channel (rx) is now closed
                        println!("[Writer] Processor data channel closed. No more data batches expected.");
                        // 确保所有数据都写入磁盘
                        if !write_buffer.is_empty() {
                            file.write_all(write_buffer.as_bytes()).await?;
                            write_buffer.clear();
                        }
                        file.flush().await?;
                        data_rx_active = false; // Stop polling this branch
                    }
                }
            }

            // Progress channel branch
            update = progress_rx.recv(), if progress_rx_active => {
                match update {
                    Some(rows_processed_update) => {
                        total_rows_acknowledged += rows_processed_update;
                        // Original logging condition for progress:
                        if log_rows_written_since_last_log > 0 || batch_count % 10 == 0 {
                             println!("[Writer] Progress: {} total rows acknowledged by processor. Batches received by writer: {}", total_rows_acknowledged, batch_count);
                             log_rows_written_since_last_log = 0;
                        }
                    }
                    None => { // Progress channel (progress_rx) is now closed
                        println!("[Writer] Progress channel closed.");
                        progress_rx_active = false; // Stop polling this branch
                    }
                }
            }
            // No `else` branch is needed here. The `while data_rx_active || progress_rx_active`
            // condition ensures the loop terminates correctly when both channels are inactive.
            // `tokio::select!` requires at least one pollable branch if no `else` is present,
            // which is guaranteed by the loop condition.
        }
    }
    
    // 最终确保所有数据都写入
    file.flush().await?;
    println!("[Writer] Task finished. Total rows acknowledged by processor: {}. Total rows written: {}.", 
        total_rows_acknowledged, total_rows_written);
    
    // 返回实际写入的行数，而不是被处理器确认的行数
    // 这样确保测试能准确校验实际写入的行数
    Ok(total_rows_written)
}

// run_conversion (Identical to Tokio + Rayon version, orchestrates tasks)
async fn run_conversion(
    parquet_input_path: PathBuf,
    jsonl_output_path: PathBuf,
    batch_size: usize,
    projection: Option<Vec<String>>,
    channel_buffer_size: usize,
    num_threads: usize,
    use_simd_json: bool,
) -> Result<u64> {
    let overall_start_time = Instant::now();
    
    // 配置Rayon线程池大小（如果指定）
    if num_threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
            .map_err(|e| ConversionError::StdIo(format!("Failed to configure thread pool: {}", e)))?;
        println!("[Main] Configured Rayon thread pool with {} threads", num_threads);
    } else {
        println!("[Main] Using default Rayon thread pool configuration");
    }
    
    let (rb_tx, rb_rx) = mpsc::channel::<Result<RecordBatch>>(channel_buffer_size);
    let (pb_tx, pb_rx) = mpsc::channel::<Result<Vec<Result<String>>>>(channel_buffer_size);
    let (progress_tx, progress_rx) = mpsc::channel::<u64>(channel_buffer_size * 2);

    let reader_handle = tokio::spawn(reader_task(parquet_input_path.clone(), batch_size, projection.clone(), rb_tx));
    let processor_handle = tokio::spawn(processor_task(rb_rx, pb_tx, progress_tx, use_simd_json));
    let writer_handle = tokio::spawn(writer_task(jsonl_output_path.clone(), pb_rx, progress_rx));

    // Await task completion and handle results
    match reader_handle.await? {
        Ok(_) => println!("[Main] Reader task completed successfully."),
        Err(e) => {eprintln!("[Main] Reader task failed: {:?}", e); return Err(e); }
    }
    match processor_handle.await? {
        Ok(_) => println!("[Main] Processor task completed successfully."),
        Err(e) => {eprintln!("[Main] Processor task failed: {:?}", e); return Err(e); }
    }
    let total_rows_written = match writer_handle.await? {
        Ok(rows) => {println!("[Main] Writer task completed successfully."); rows },
        Err(e) => {eprintln!("[Main] Writer task failed: {:?}", e); return Err(e); }
    };

    let duration = overall_start_time.elapsed();
    println!("Asynchronous conversion (Tokio only, parallel processing per batch) completed in {:.2?}.", duration);
    if duration.as_secs_f64() > 0.0 && total_rows_written > 0 {
        println!("Average processing speed: {:.2} rows/sec.", total_rows_written as f64 / duration.as_secs_f64());
    }
    Ok(total_rows_written)
}

// create_dummy_parquet_file (Identical to previous version)
fn create_dummy_parquet_file(file_path_str: &str, num_rows: usize, num_cols: usize) -> Result<(), String> {
    use arrow_array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arrow_schema::{Schema, Field as ArrowField, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::time::{SystemTime, UNIX_EPOCH};

    let file_path = Path::new(file_path_str);
    let mut fields = Vec::new();
    for i in 0..num_cols {
        match i % 3 {
            0 => fields.push(Arc::new(ArrowField::new(format!("col_int_{}", i), DataType::Int64, true))),
            1 => fields.push(Arc::new(ArrowField::new(format!("col_str_{}", i), DataType::Utf8, true))),
            _ => fields.push(Arc::new(ArrowField::new(format!("col_ts_{}",i), DataType::Timestamp(TimeUnit::Nanosecond, None), true))),
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let file = StdFile::create(file_path).map_err(|e| format!("Dummy create error: {}", e))?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(|e| format!("Dummy writer error: {}", e))?;
    
    // Ensure chunk_size is at least 1 to prevent panic with step_by(0) when num_rows is 0.
    let chunk_size = 8192.min(num_rows).max(1);

    for chunk_start in (0..num_rows).step_by(chunk_size) {
        let current_chunk_size = chunk_size.min(num_rows - chunk_start);
        let mut columns: Vec<ArrayRef> = Vec::new();
        for i in 0..num_cols {
            match i % 3 {
                0 => columns.push(Arc::new((0..current_chunk_size).map(|j| Some((chunk_start + j) as i64 * (i + 1) as i64)).collect::<Int64Array>())),
                1 => columns.push(Arc::new((0..current_chunk_size).map(|j| Some(format!("row_{}_col_{}", chunk_start + j, i))).collect::<StringArray>())),
                _ => {
                    let start_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
                    columns.push(Arc::new((0..current_chunk_size).map(|j| Some(start_ts + (j * 1_000_000) as i64 )).collect::<TimestampNanosecondArray>()));
                }
            }
        }
        let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| format!("Dummy batch error: {}", e))?;
        writer.write(&batch).map_err(|e| format!("Dummy write error: {}", e))?;
    }
    writer.close().map_err(|e| format!("Dummy close error: {}", e))?;
    Ok(())
}

// 新增：定义命令行参数结构体
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(short, long, value_parser)]
    parquet_input: PathBuf,

    #[clap(short, long, value_parser)]
    jsonl_output: PathBuf,

    #[clap(short, long, default_value_t = 16384)]
    batch_size: usize,

    #[clap(short, long, default_value_t = 10)]
    channel_buffer: usize,

    #[clap(long, value_delimiter = ',', num_args = 0..)] // 允许逗号分隔的列表，或者不提供
    projection: Option<Vec<String>>,
    
    #[clap(long, default_value_t = 0)]
    num_threads: usize,
    
    #[clap(long, default_value_t = false)]
    use_simd_json: bool,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse(); // 解析命令行参数

    let parquet_input_path = cli.parquet_input;
    let jsonl_output_path = cli.jsonl_output;
    let batch_size = cli.batch_size;
    let channel_buffer_size = cli.channel_buffer;
    let projection = cli.projection;
    let num_threads = cli.num_threads;
    let use_simd_json = cli.use_simd_json;

    println!("Starting Asynchronous Parquet to JSONL conversion (Tokio only, parallel batch processing)...");
    match run_conversion(
        parquet_input_path,
        jsonl_output_path,
        batch_size,
        projection,
        channel_buffer_size,
        num_threads,
        use_simd_json,
    ).await {
        Ok(rows) => println!("[Main] Overall conversion completed successfully. Total rows effectively processed: {}", rows),
        Err(e) => eprintln!("[Main] Overall conversion failed: {:?}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_run_conversion_successfully() {
        let test_parquet_input = PathBuf::from("test_input_conversion.parquet");
        let test_jsonl_output = PathBuf::from("test_output_conversion.jsonl");
        let num_rows = 100;
        let num_cols = 3;
        let batch_size = 5000;
        let channel_buffer_size = 5;

        // 1. Create dummy parquet file
        match create_dummy_parquet_file(test_parquet_input.to_str().unwrap(), num_rows, num_cols) {
            Ok(_) => println!("[Test] Dummy Parquet file created for test."),
            Err(e) => {
                // Clean up before panicking
                let _ = fs::remove_file(&test_parquet_input);
                let _ = fs::remove_file(&test_jsonl_output);
                panic!("[Test] Failed to create dummy Parquet file: {:?}", e);
            }
        }

        // 2. Run conversion
        let conversion_result = run_conversion(
            test_parquet_input.clone(),
            test_jsonl_output.clone(),
            batch_size,
            None, // No projection for this test
            channel_buffer_size,
            0, // No Rayon threads
            false, // No SIMD JSON
        )
        .await;

        // 3. Assert success and row count
        match conversion_result {
            Ok(processed_rows) => {
                assert_eq!(
                    processed_rows,
                    num_rows as u64,
                    "[Test] Number of processed rows does not match expected."
                );
                println!("[Test] Conversion successful. Processed {} rows.", processed_rows);

                // Optional: Verify output file content (e.g., count lines)
                if let Ok(output_content) = fs::read_to_string(&test_jsonl_output) {
                    let lines: Vec<&str> = output_content.lines().collect();
                    assert_eq!(lines.len(), num_rows, "[Test] JSONL output line count mismatch.");
                     // Basic check for non-empty lines if rows > 0
                    if num_rows > 0 {
                        assert!(!lines[0].is_empty(), "[Test] First line of JSONL output is empty.");
                    }
                    println!("[Test] JSONL output file has {} lines as expected.", lines.len());
                } else {
                    // Clean up before failing
                    let _ = fs::remove_file(&test_parquet_input);
                    let _ = fs::remove_file(&test_jsonl_output);
                    panic!("[Test] Could not read output JSONL file for verification.");
                }
            }
            Err(e) => {
                // Clean up before panicking
                let _ = fs::remove_file(&test_parquet_input);
                let _ = fs::remove_file(&test_jsonl_output);
                panic!("[Test] Conversion failed: {:?}", e);
            }
        }

        // 4. Clean up files
        let _ = fs::remove_file(&test_parquet_input);
        let _ = fs::remove_file(&test_jsonl_output);
        println!("[Test] Cleaned up temporary test files.");
    }

    #[tokio::test]
    async fn test_run_conversion_with_projection() {
        let test_parquet_input = PathBuf::from("test_input_projection.parquet");
        let test_jsonl_output = PathBuf::from("test_output_projection.jsonl");
        let num_rows = 50;
        let num_cols = 5; // col_int_0, col_str_1, col_ts_2, col_int_3, col_str_4
        let batch_size = 20;
        let channel_buffer_size = 5;
        let projection = Some(vec![
            "col_int_0".to_string(),
            "col_ts_2".to_string(),
            "col_str_4".to_string(),
        ]);
        let expected_columns = vec!["col_int_0", "col_ts_2", "col_str_4"];

        if let Err(e) = create_dummy_parquet_file(test_parquet_input.to_str().unwrap(), num_rows, num_cols) {
            let _ = fs::remove_file(&test_parquet_input); panic!("[TestProjection] Failed to create dummy Parquet: {:?}", e);
        }

        let result = run_conversion(
            test_parquet_input.clone(),
            test_jsonl_output.clone(),
            batch_size,
            projection,
            channel_buffer_size,
            0, // No Rayon threads
            false, // No SIMD JSON
        ).await;

        match result {
            Ok(processed_rows) => {
                assert_eq!(processed_rows, num_rows as u64, "[TestProjection] Row count mismatch.");
                if let Ok(output_content) = fs::read_to_string(&test_jsonl_output) {
                    let lines: Vec<&str> = output_content.lines().collect();
                    assert_eq!(lines.len(), num_rows, "[TestProjection] Output line count mismatch.");
                    if num_rows > 0 {
                        let first_line_json: JsonValue = serde_json::from_str(lines[0])
                            .expect("[TestProjection] Failed to parse first line of JSON output.");
                        if let JsonValue::Object(map) = first_line_json {
                            assert_eq!(map.len(), expected_columns.len(), "[TestProjection] Projected column count mismatch.");
                            for col_name in expected_columns {
                                assert!(map.contains_key(col_name), "[TestProjection] Expected column '{}' not found.", col_name);
                            }
                        } else {
                            panic!("[TestProjection] First line of output is not a JSON object.");
                        }
                    }
                } else {
                    panic!("[TestProjection] Could not read output JSONL.");
                }
            }
            Err(e) => panic!("[TestProjection] Conversion failed: {:?}", e),
        }

        let _ = fs::remove_file(&test_parquet_input);
        let _ = fs::remove_file(&test_jsonl_output);
        println!("[TestProjection] Test completed and files cleaned up.");
    }

    #[tokio::test]
    async fn test_run_conversion_zero_rows() {
        let test_parquet_input = PathBuf::from("test_input_zero_rows.parquet");
        let test_jsonl_output = PathBuf::from("test_output_zero_rows.jsonl");
        let num_rows = 0;
        let num_cols = 3;
        let batch_size = 10;
        let channel_buffer_size = 5;

        if let Err(e) = create_dummy_parquet_file(test_parquet_input.to_str().unwrap(), num_rows, num_cols) {
            let _ = fs::remove_file(&test_parquet_input); panic!("[TestZeroRows] Failed to create dummy Parquet: {:?}", e);
        }

        let result = run_conversion(
            test_parquet_input.clone(),
            test_jsonl_output.clone(),
            batch_size,
            None,
            channel_buffer_size,
            0, // No Rayon threads
            false, // No SIMD JSON
        ).await;

        match result {
            Ok(processed_rows) => {
                assert_eq!(processed_rows, 0, "[TestZeroRows] Processed rows should be 0.");
                if let Ok(output_content) = fs::read_to_string(&test_jsonl_output) {
                    assert!(output_content.is_empty(), "[TestZeroRows] Output file should be empty for zero input rows.");
                } else {
                    // It's also okay if the file was created but is empty.
                    // If create_dummy_parquet_file with 0 rows doesn't create a file, this branch might not be hit.
                    // The primary check is processed_rows == 0.
                    // If the file *is* created (e.g., by TokioFile::create), then it should be empty.
                    // Check if file exists and is empty.
                    if test_jsonl_output.exists() {
                         let metadata = fs::metadata(&test_jsonl_output).expect("[TestZeroRows] Failed to get metadata for output file.");
                         assert_eq!(metadata.len(), 0, "[TestZeroRows] Output file was created but is not empty.");
                    } else {
                        // If the file isn't created at all for 0 rows, that's also acceptable.
                        println!("[TestZeroRows] Output file was not created, which is acceptable for 0 rows processed if writer doesn't create empty files.");
                    }
                }
            }
            Err(e) => panic!("[TestZeroRows] Conversion failed: {:?}", e),
        }

        let _ = fs::remove_file(&test_parquet_input);
        let _ = fs::remove_file(&test_jsonl_output);
        println!("[TestZeroRows] Test completed and files cleaned up.");
    }

    async fn run_batch_size_test_case(num_rows: usize, batch_size: usize, test_id: &str) {
        let test_parquet_input = PathBuf::from(format!("test_input_batch_{}.parquet", test_id));
        let test_jsonl_output = PathBuf::from(format!("test_output_batch_{}.jsonl", test_id));
        let num_cols = 2;
        let channel_buffer_size = (num_rows / batch_size.max(1)).max(2).min(20); // Dynamic buffer

        if let Err(e) = create_dummy_parquet_file(test_parquet_input.to_str().unwrap(), num_rows, num_cols) {
            let _ = fs::remove_file(&test_parquet_input); panic!("[TestBatch-{}] Failed to create dummy Parquet: {:?}", test_id, e);
        }

        let result = run_conversion(
            test_parquet_input.clone(),
            test_jsonl_output.clone(),
            batch_size,
            None,
            channel_buffer_size,
            0, // No Rayon threads
            false, // No SIMD JSON
        ).await;

        match result {
            Ok(processed_rows) => {
                assert_eq!(processed_rows, num_rows as u64, "[TestBatch-{}] Row count mismatch. Expected {}, got {}", test_id, num_rows, processed_rows);
                if num_rows > 0 {
                    if let Ok(output_content) = fs::read_to_string(&test_jsonl_output) {
                        let lines: Vec<&str> = output_content.lines().collect();
                        assert_eq!(lines.len(), num_rows, "[TestBatch-{}] Output line count mismatch.", test_id);
                    } else {
                        panic!("[TestBatch-{}] Could not read output JSONL.", test_id);
                    }
                } else { // num_rows == 0
                     if let Ok(output_content) = fs::read_to_string(&test_jsonl_output) {
                        assert!(output_content.is_empty(), "[TestBatch-{}] Output file should be empty for zero input rows.", test_id);
                    } else if test_jsonl_output.exists() {
                        let metadata = fs::metadata(&test_jsonl_output).expect("[TestBatch] Failed to get metadata");
                        assert_eq!(metadata.len(), 0, "[TestBatch-{}] Output file for 0 rows not empty.", test_id);
                    }
                }
            }
            Err(e) => panic!("[TestBatch-{}] Conversion failed: {:?}", test_id, e),
        }

        let _ = fs::remove_file(&test_parquet_input);
        let _ = fs::remove_file(&test_jsonl_output);
        println!("[TestBatch-{}] Test completed and files cleaned up.", test_id);
    }

    #[tokio::test]
    async fn test_run_conversion_batch_size_one() {
        run_batch_size_test_case(10, 1, "bs_one").await;
    }

    #[tokio::test]
    async fn test_run_conversion_batch_size_equals_rows() {
        run_batch_size_test_case(100, 100, "bs_eq_rows").await;
    }

    #[tokio::test]
    async fn test_run_conversion_batch_size_greater_than_rows() {
        run_batch_size_test_case(30, 100, "bs_gt_rows").await;
    }
     #[tokio::test]
    async fn test_run_conversion_batch_size_prime_rows_small_batch() {
        run_batch_size_test_case(17, 3, "bs_prime_small").await; // Prime number of rows, small batch
    }

    #[tokio::test]
    async fn test_run_conversion_batch_size_large_prime_rows() {
        run_batch_size_test_case(97, 10, "bs_large_prime").await; // Larger prime number of rows
    }
}