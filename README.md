## Project Title: Distributed Query Execution System with Ray

### Description
This project provides a platform to execute complex database queries on distributed data using the Ray framework. Ray is a high-performance distributed computing library that allows you to write scalable and distributed programs with ease. This project supports various operations such as Scanning, Filtering, Joining, Grouping, and more. It also allows execution in two modes - pull-based and push-based evaluation - giving users the flexibility to choose the best mode for their use-case.

### Functionality
The system takes in two data files as input and supports three types of queries:

1. **Query 1:** Joins the two datasets on specific user-defined columns, filters based on provided user-defined values, and then computes an average on a specific column. The result will be written to a CSV file.
2. **Query 2:** This is an extension of query 1, but with the additional step of ordering the results by the computed average and selecting the top entry.
3. **Query 3:** Executes a join and filter operation like in Query 1, and then generates a histogram based on a specified column.

The query type, columns to join, values to filter, column to average or create a histogram are all configurable via command-line arguments.

### Dependencies
- Python 3
- Ray (for distributed computing)
- argparse (for command-line argument parsing)
- csv (for writing results to CSV files)

### Installation
1. Install Python 3 from the official [website](https://www.python.org/downloads/).
2. Install the Ray library using pip:
   ```
   pip install ray
   ```

### Usage
Execute the script from the command line, specifying the required and optional arguments as follows:

#### Arguments:
- `--query`: The type of query to execute (`1`, `2`, or `3`). (Default: `1`)
- `--ff`: The path to the first input data file.
- `--mf`: The path to the second input data file.
- `--uid`: The value to use for filtering in the first data file. (Default: `99`)
- `--mid`: The value to use for filtering in the second data file. (Default: `99`)
- `--pull`: Whether to use pull-based (`1`) vs push-based (`0`) evaluation. (Default: `1`)
- `--output`: The path to the output file. (Default: `output`)

#### Example Command:
```bash
python script.py --query 1 --ff path/to/file1.csv --mf path/to/file2.csv --uid 10 --mid 20 --pull 1 --output output.csv
```

### Output
The result of the executed query will be written to a CSV file at the specified output path. The file will contain the result of the query operation - either average values, top value, or a histogram, based on the selected query type.

### Notes
- Ensure that the input files for the two data relations are correctly formatted.
- The implementation assumes remote functions like `Scan`, `Select`, `Join`, `GroupBy`, etc., are defined elsewhere in the code.
- Adjust sleep time in the push-based evaluation depending on the size of the data files and the resources available.

### Future Enhancements
- Support for additional query types can be added in future iterations.
- Better error handling and data validation can be incorporated for robustness.
- The system can be enhanced to support multiple input files and more complex join and filter operations.

---