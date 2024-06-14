#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <iostream>
#include <memory>
#include <chrono>
#include <arrow/util/cpu_info.h>
#include <arrow/util/thread_pool.h>

arrow::Status ProcessParquetFile(const std::string& file_path, double& total_open_duration, double& total_read_duration, double& total_duration) {
    auto start = std::chrono::high_resolution_clock::now();
    arrow::MemoryPool* pool;
    ARROW_RETURN_NOT_OK(mimalloc_memory_pool(&pool));

    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(file_path));

    parquet::ArrowReaderProperties arrow_reader_properties = parquet::default_arrow_reader_properties();
    arrow_reader_properties.set_pre_buffer(true);
    arrow_reader_properties.set_use_threads(true);

    parquet::ReaderProperties reader_properties = parquet::ReaderProperties(pool);

    auto open_start = std::chrono::high_resolution_clock::now();
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    parquet::arrow::FileReaderBuilder reader_builder;
    reader_builder.properties(arrow_reader_properties);

    ARROW_RETURN_NOT_OK(reader_builder.Open(input));
    ARROW_RETURN_NOT_OK(reader_builder.Build(&arrow_reader));

    auto open_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> open_duration = open_end - open_start;
    total_open_duration += open_duration.count();

    auto read_start = std::chrono::high_resolution_clock::now();
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));
    auto read_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> read_duration = read_end - read_start;
    total_read_duration += read_duration.count();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    total_duration += duration.count();

    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    if (argc < 2 || argc > 3) {
        std::cerr << "Usage: " << argv[0] << " <parquet_file_path> [iterations]" << std::endl;
        return 1;
    }

    std::string file_path = argv[1];

    arrow::SetCpuThreadPoolCapacity(1);

    double total_open_duration = 0.0;
    double total_read_duration = 0.0;
    double total_duration = 0.0;
    int iterations = (argc == 3) ? std::stoi(argv[2]) : 100;

    for(int i=0; i<iterations; i++) {
        arrow::Status status = ProcessParquetFile(file_path, total_open_duration, total_read_duration, total_duration);
        if (!status.ok()) {
            std::cerr << "Error: " << status.ToString() << std::endl;
            return 1;
        }
    }

    double average_open_duration = total_open_duration / iterations;
    double average_read_duration = total_read_duration / iterations;
    double average_duration = total_duration / iterations;

    std::cout << "Average time to open file: " << average_open_duration << " seconds" << std::endl;
    std::cout << "Average time to read file: " << average_read_duration << " seconds" << std::endl;
    std::cout << "Average total time: " << average_duration << " seconds" << std::endl;

    int num_cores = arrow::internal::CpuInfo::GetInstance()->num_cores();
    std::cout << "Number of CPU cores available: " << num_cores << std::endl;
    int num_threads = arrow::GetCpuThreadPoolCapacity();
    std::cout << "Number of threads in the Arrow thread pool: " << num_threads << std::endl;

    return 0;
}
