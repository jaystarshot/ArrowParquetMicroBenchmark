import pyarrow as pa
import pyarrow.ipc as ipc
import time
import sys
import os

def process_ipc_file(file_path):
    start = time.time()

    # Open the IPC file
    open_start = time.time()
    try:
        source = open(file_path, 'rb')
        ipc_file = ipc.RecordBatchFileReader(source)
    except Exception as e:
        print(f"Error opening file: {e}")
        return None, None, None, None
    open_end = time.time()
    open_duration = open_end - open_start

    print(ipc_file.num_record_batches)
    # Read the table
    read_start = time.time()
    try:
        # Read all record batches and concatenate them into a table
        batches = [ipc_file.get_batch(i) for i in range(ipc_file.num_record_batches)]
        table = pa.Table.from_batches(batches)
        num_rows = table.num_rows
    except Exception as e:
        print(f"Error reading file: {e}")
        return None, None, None, None
    finally:
        source.close()

    read_end = time.time()
    read_duration = read_end - read_start

    end = time.time()
    total_duration = end - start

    return open_duration, read_duration, total_duration, num_rows

def main():
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print(f"Usage: {sys.argv[0]} <ipc_file_path> [iterations]")
        return 1

    file_path = sys.argv[1]
    iterations = int(sys.argv[2]) if len(sys.argv) == 3 else 10

    # Set Arrow to use only one thread
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["ARROW_NUM_THREADS"] = "1"

    print(f"Number of CPU cores available: {os.cpu_count()}")
    print(f"Number of threads used by Arrow: {pa.cpu_count()}")

    total_open_duration = 0.0
    total_read_duration = 0.0
    total_duration = 0.0
    total_rows = 0

    valid_iterations = 0

    for _ in range(iterations):
        open_duration, read_duration, duration, num_rows = process_ipc_file(file_path)
        if open_duration is not None:
            total_open_duration += open_duration
            total_read_duration += read_duration
            total_duration += duration
            total_rows += num_rows
            valid_iterations += 1

    if valid_iterations > 0:
        average_open_duration = total_open_duration / valid_iterations
        average_read_duration = total_read_duration / valid_iterations
        average_duration = total_duration / valid_iterations
        average_rows = total_rows // valid_iterations

        print(f"Average time to open file: {average_open_duration} seconds")
        print(f"Average time to read table: {average_read_duration} seconds")
        print(f"Average total time: {average_duration} seconds")
        print(f"Average number of rows: {average_rows}")
    else:
        print("No valid iterations to calculate averages.")

if __name__ == "__main__":
    main()
