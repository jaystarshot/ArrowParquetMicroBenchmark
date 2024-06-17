import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.ipc as ipc
import time
import sys
import os

def process_parquet_file(file_path, output_file_path):
    start = time.time()

    # Open the Parquet file
    open_start = time.time()
    try:
        parquet_file = pq.ParquetFile(file_path)
    except Exception as e:
        print(f"Error opening file: {e}")
        return None, None, None, None
    open_end = time.time()
    open_duration = open_end - open_start

    # Read the table
    read_start = time.time()
    try:
        table = parquet_file.read()
    except Exception as e:
        print(f"Error reading file: {e}")
        return None, None, None, None
    read_end = time.time()
    read_duration = read_end - read_start

    # Write the table to an IPC file with Zstd compression
    write_start = time.time()
    try:
        write_options = ipc.IpcWriteOptions(compression='zstd')
        with pa.OSFile(output_file_path, 'wb') as sink:
            with ipc.new_file(sink, table.schema, options=write_options) as writer:
                writer.write_table(table)
    except Exception as e:
        print(f"Error writing IPC file: {e}")
        return None, None, None, None
    write_end = time.time()
    write_duration = write_end - write_start

    end = time.time()
    total_duration = end - start

    return open_duration, read_duration, write_duration, total_duration

def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <parquet_file_path> <output_ipc_file_path>")
        return 1

    file_path = sys.argv[1]
    output_file_path = sys.argv[2]

    # Set Arrow to use only one thread
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["ARROW_NUM_THREADS"] = "1"

    print(f"Number of CPU cores available: {os.cpu_count()}")
    print(f"Number of threads used by Arrow: {pa.cpu_count()}")

    open_duration, read_duration, write_duration, total_duration = process_parquet_file(file_path, output_file_path)

    if open_duration is not None:
        print(f"Time to open file: {open_duration} seconds")
        print(f"Time to read table: {read_duration} seconds")
        print(f"Time to write IPC file: {write_duration} seconds")
        print(f"Total time: {total_duration} seconds")
    else:
        print("An error occurred during processing.")

if __name__ == "__main__":
    main()
