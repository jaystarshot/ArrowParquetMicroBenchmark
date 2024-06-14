# Define the project name and build type
PROJECT_NAME = CArrowBenchmark
BUILD_TYPE = Release

# Define the build directory
BUILD_DIR = build

# Default target to build the project
all: $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) .. && make

# Create the build directory if it doesn't exist
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Clean target to remove build artifacts
clean:
	rm -rf $(BUILD_DIR)

.PHONY: all clean
