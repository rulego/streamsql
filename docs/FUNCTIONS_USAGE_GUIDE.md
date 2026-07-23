# StreamSQL Function usage guide

StreamSQL Rich built-in functions that can perform various calculations on data. All functions support use in streaming environments, with some supporting incremental computation to improve performance.

## 📊 Aggregation Function

The aggregate function performs calculations on a set of values and returns a single value. Aggregation functions can only be used in the following expressions:
- SELECT list of SELECT statements (subqueries or external queries)
- HAVING Clause

### SUM - Summation function
**Grammar**: `sum(col)`  
**Description**: Returns the sum of values in the group. Null values are not involved in the calculation.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, sum(temperature) as total_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### AVG - Average Function
**Grammar**: `avg(col)`  
**Description**: Returns the average of the values in the group. Null values are not involved in the calculation.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, avg(temperature) as avg_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### COUNT - Counting Function
**Grammar**: `count(*)`  
**Description**: Returns the number of rows in the group.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, count(*) as record_count 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### MIN - Minimum value function
**Grammar**: `min(col)`  
**Description**: Returns the minimum value of the value in the group. Null values are not involved in the calculation.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, min(temperature) as min_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### MAX - Maximum value function
**Grammar**: `max(col)`  
**Description**: Returns the maximum value of the value in the group. Null values are not involved in the calculation.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, max(temperature) as max_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### COLLECT - Collection function
**Grammar**: `collect(col)`  
**Description**: Retrieves an array of column values for all messages in the current window.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, collect(temperature) as temp_values 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### LAST_VALUE - The last value function
**Grammar**: `last_value(col)`  
**Description**: Returns the value of the last row in the group.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, last_value(temperature) as last_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### MERGE_AGG - Merge Aggregation Function
**Grammar**: `merge_agg(col)`  
**Description**: Merge values from a group into a single value. For object types, merge all key-value pairs; For other types, connect with commas.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, merge_agg(status) as all_status 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### DEDUPLICATE - Deduplication function
**Grammar**: `deduplicate(col, false)`  
**Description**: Returns the result of deduplication from the current group, usually used in windows. The second parameter specifies whether to return all results.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, deduplicate(temperature, true) as unique_temps 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### STDDEV - Standard Deviation Function
**Grammar**: `stddev(col)`  
**Description**: Returns the population standard deviation of all values in the group. Null values are not involved in the calculation.  
**Incremental Computation**: ✅ Supported (optimized using the Welford algorithm)  
**Example**:
```sql
SELECT device, stddev(temperature) as temp_stddev 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### STDDEVS - Sample Standard Deviation Function
**Grammar**: `stddevs(col)`  
**Description**: Returns the sample standard deviation of all values in the group. Null values are not involved in the calculation.  
**Incremental Computation**: ✅ Supported (optimized using the Welford algorithm)  
**Example**:
```sql
SELECT device, stddevs(temperature) as temp_sample_stddev 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### VAR - Variance Function
**Grammar**: `var(col)`  
**Description**: Returns the population variance of all values in the group. Null values are not involved in the calculation.  
**Incremental Computation**: ✅ Supported (optimized using the Welford algorithm)  
**Example**:
```sql
SELECT device, var(temperature) as temp_variance 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### VARS - Sample variance function
**Grammar**: `vars(col)`  
**Description**: Returns the sample variance of all non-NULL values in the group.  
**Incremental Computation**: ✅ Supported (optimized using the Welford algorithm)  
**Example**:
```sql
SELECT device, vars(temperature) as temp_sample_variance 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### MEDIAN - Median function
**Grammar**: `median(col)`  
**Description**: Returns the median of all values in the group. Null values are not involved in the calculation.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, median(temperature) as temp_median 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### PERCENTILE - Percentile Function
**Grammar**: `percentile(col, 0.5)`  
**Description**: Returns the specified percentile of all values in the group. The second parameter specifies the percentile value, with a range of 0.0 ~ 1.0.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, percentile(temperature, 0.95) as temp_p95 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

## 🔍 Analyze the function

Analysis functions are used to perform complex analytical calculations within data streams, supporting state management and access to historical data.

### LAG - Lag function
**Grammar**: `lag(col, offset, default_value)`  
**Description**: Returns the value of the N-th line before the current line. offset Specify the offset, default_value is the default value.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, temperature, lag(temperature, 1) as prev_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### LATEST - Latest value function
**Grammar**: `latest(col)`  
**Description**: Returns the latest value for the specified column.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, latest(temperature) as current_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### CHANGED_COL - Change column function
**Grammar**: `changed_col(row_data)`  
**Description**: Returns the column name array where it changed.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, changed_col(*) as changed_columns 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### HAD_CHANGED - Change detection function
**Grammar**: `had_changed(col)`  
**Description**: Check whether the value of a specified column has changed, and return a boolean value.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, had_changed(status) as status_changed 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

## 🪟 Window function

The window function provides information related to the window.

### WINDOW_START - Window start time
**Grammar**: `window_start()`  
**Description**: Returns the start time of the current window.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, window_start() as window_begin, avg(temperature) as avg_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### WINDOW_END - Window closing time
**Grammar**: `window_end()`  
**Description**: Returns the end time of the current window.  
**Incremental Calculation**: ✅ Supported  
**Example**:
```sql
SELECT device, window_end() as window_finish, avg(temperature) as avg_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

## 🧮 Mathematical Functions

Mathematical functions are used for numerical calculations.

### ABS - Absolute value function
**Grammar**: `abs(number)`  
**Description**: Returns the absolute value of the value.  

### SQRT - Square root function
**Grammar**: `sqrt(number)`  
**Description**: Returns the square root of the value.  

### POWER - Power Function
**Grammar**: `power(base, exponent)`  
**Description**: Returns the specified power of the base.  
 
### CEILING - The uptake function
**Grammar**: `ceiling(number)`  
**Description**: Returns the smallest integer greater than or equal to the specified value.  

### FLOOR - Downturn function
**Grammar**: `floor(number)`  
**Description**: Returns the largest integer less than or equal to the specified value.  
 
### ROUND - Rounding function
**Grammar**: `round(number, [precision])`  
**Description**: Rounding values to a specified decimal place.  
 
### MOD - Taking the modulus function
**Grammar**: `mod(dividend, divisor)`  
**Description**: Returns the remainder of the division operation.  
 
### RAND - Random Number Function
**Grammar**: `rand()`  
**Description**: Returns a random number between 0 and 1.  

### SIGN - Symbolic function
**Grammar**: `sign(number)`  
**Description**: Returns the symbol for a value (-1, 0, or 1).  
 
### Trigonometric Functions

#### SIN - Sine function
**Grammar**: `sin(number)`  
**Description**: Returns the sine of an angle in radians.  

#### COS - Cosine function
**Grammar**: `cos(number)`  
**Description**: Returns the cosine of an angle in radians.  
 
#### TAN - Tangent function
**Grammar**: `tan(number)`  
**Description**: Returns the tangent of an angle in radians.  
 
#### ASIN - Arcsine function
**Grammar**: `asin(number)`  
**Description**: Returns the arcsine value of the value (radian system).  
 
#### ACOS - Inverse cosine function
**Grammar**: `acos(number)`  
**Description**: Returns the arccosine value of the value (radian system).  
 
#### ATAN - arctangent function
**Grammar**: `atan(number)`  
**Description**: Returns the arctangent value of the value (in radians).  
 
#### ATAN2 - Two-parameter arctangent function
**Grammar**: `atan2(y, x)`  
**Description**: Returns the arctangent of y/x in radians.  
 
### Hyperbolic Functions

#### SINH - Hyperbolic Sine Function
**Grammar**: `sinh(number)`  
**Description**: Returns the hyperbolic sine value of the value.  
 
#### COSH - Hyperbolic cosine function
**Grammar**: `cosh(number)`  
**Description**: Returns the hyperbolic cosine value of the value.  
 
#### TANH - Hyperbolic tangent function
**Grammar**: `tanh(number)`  
**Description**: Returns the hyperbolic tangent value of the value.  
 
### Logarithmic and exponential functions

#### EXP - Exponential Function
**Grammar**: `exp(number)`  
**Description**: Returns e raised to the specified power.  
 
#### LN - Natural logarithmic function
**Grammar**: `ln(number)`  
**Description**: Returns the natural logarithm of the value.  
 
#### LOG - Logarithmic function
**Grammar**: `log(base, number)`  
**Description**: Returns the logarithm of the specified base.  
 
#### LOG10 - Common logarithmic functions
**Grammar**: `log10(number)`  
**Description**: Returns the commonly used logarithm of the value (base 10).  
 
#### LOG2 - Binary logarithmic function
**Grammar**: `log2(number)`  
**Description**: Returns the binary logarithm of the value (base 2).  
 
### Bitwise Arithmetic Function

#### BIT_AND - Bits and functions
**Grammar**: `bit_and(number1, number2)`  
**Description**: Perform bits and operations on two integers.  
 
#### BIT_OR - Bits or functions
**Grammar**: `bit_or(number1, number2)`  
**Description**: Executes bits or operations on two integers.  
 
#### BIT_XOR - Bitwise XOR function
**Grammar**: `bit_xor(number1, number2)`  
**Description**: Performs bitwise OR operations on two integers.  
 
#### BIT_NOT - Bits are nonfunctional
**Grammar**: `bit_not(number)`  
**Description**: Performs bitwise non-operations on integers.  
 
## 📝 String function

String functions are used for text processing.

### UPPER - Uppercase Conversion Function
**Grammar**: `upper(str)`  
**Description**: Converts strings to uppercase.  
 
### LOWER - Lowercase conversion function
**Grammar**: `lower(str)`  
**Description**: Converts strings to lowercase.  
 
### CONCAT - String concatenation function
**Grammar**: `concat(str1, str2,.)`  
**Description**: Concatenate multiple strings.  
 
### LENGTH - String length function
**Grammar**: `length(str)`  
**Description**: Returns the length of the string.  
 
### SUBSTRING - Substring function
**Grammar**: `substring(str, start, [length])`  
**Description**: Extracting substrings from a string.  
 
### TRIM - Remove the space function
**Grammar**: `trim(str)`  
**Description**: Remove spaces at both ends of a string.  
 
### LTRIM - Remove the space function on the left
**Grammar**: `ltrim(str)`  
**Description**: Remove the space on the left side of the string.  
 
### RTRIM - Remove the space function on the right
**Grammar**: `rtrim(str)`  
**Description**: Remove the space on the right side of the string.  

### FORMAT - Formatting function
**Grammar**: `format(format_str,.)`  
**Description**: Format the string according to the specified format.  

### ENDSWITH - End-check function
**Grammar**: `endswith(str, suffix)`  
**Description**: Check whether the string ends with the specified suffix.  

### STARTSWITH - Opening check function
**Grammar**: `startswith(str, prefix)`  
**Description**: Check whether the string starts with a specified prefix.  

### INDEXOF - Find the position function
**Grammar**: `indexof(str, substring)`  
**Description**: Returns the position of the substring within the string.  
 
### REPLACE - Replace the function
**Grammar**: `replace(str, old_str, new_str)`  
**Description**: Replace the specified content in the string.  

### SPLIT - Partition function
**Grammar**: `split(str, delimiter)`  
**Description**: Split strings by separators.  
 
### LPAD - Left padding function
**Grammar**: `lpad(str, length, pad_str)`  
**Description**: Fill the left side of the string with characters to a specified length.  
 
### RPAD - Right padding function
**Grammar**: `rpad(str, length, pad_str)`  
**Description**: Fill the right side of the string with characters to a specified length.  
 
### Regular Expression Functions

#### REGEXP_MATCHES - Regular matching function
**Grammar**: `regexp_matches(str, pattern)`  
**Description**: Check if the string matches a regular expression.  

#### REGEXP_REPLACE - Regular replacement function
**Grammar**: `regexp_replace(str, pattern, replacement)`  
**Description**: Replace string content with regular expressions.  
 
#### REGEXP_SUBSTRING - Regular extraction function
**Grammar**: `regexp_substring(str, pattern)`  
**Description**: Use regular expressions to extract string content.  

## 🔄 Type conversion function

The type conversion function is used for data type conversion.

### CAST - Type conversion function
**Grammar**: `cast(value as type)`  
**Description**: Converts values to specified types.  
 
### HEX2DEC - Hexadecimal to decimal function
**Grammar**: `hex2dec(hex_str)`  
**Description**: Converts hexadecimal strings to decimal numbers.  
 
### DEC2HEX - Decimal to hexadecimal function
**Grammar**: `dec2hex(number)`  
**Description**: Converts decimal numbers into hexadecimal strings.  

### ENCODE - Encoding function
**Grammar**: `encode(str, encoding)`  
**Description**: Encodes strings according to the specified encoding method.  
 
### DECODE - Decode function
**Grammar**: `decode(str, encoding)`  
**Description**: Decode the string according to the specified encoding method.  
 
### CONVERT_TZ - Time zone conversion function
**Grammar**: `convert_tz(datetime, from_tz, to_tz)`  
**Description**: Convert date and time from one time zone to another.  

### TO_SECONDS - Convert to seconds function
**Grammar**: `to_seconds(datetime)`  
**Description**: Convert date and time into seconds.  

### CHR - Character function
**Grammar**: `chr(number)`  
**Description**: Convert ASCII codes to characters.  
 
### TRUNC - Truncation function
**Grammar**: `trunc(number, [precision])`  
**Description**: Truncate values to specified precision.  
 
### URL_ENCODE - URL Encoding Function
**Grammar**: `url_encode(str)`  
**Description**: Perform URL encoding on strings.  
 
### URL_DECODE - URL Decode function
**Grammar**: `url_decode(str)`  
**Description**: Perform URL decoding of strings.  
 
## ⏰ Time and date function

The time-date function is used to process time and date data.

### NOW - Current time function
**Grammar**: `now()`  
**Description**: Returns the current date and time.  
 
### CURRENT_TIME - Current time function
**Grammar**: `current_time()`  
**Description**: Returns the current time.  
 
### CURRENT_DATE - Current date function
**Grammar**: `current_date()`  
**Description**: Returns the current date.  
 
## 🔗 JSON Function

JSON functions are used to handle JSON data.

### TO_JSON - Convert to JSON function
**Grammar**: `to_json(value)`  
**Description**: Converts values into JSON strings.  
 
### FROM_JSON - Parsing functions from JSON
**Grammar**: `from_json(json_str)`  
**Description**: Parsing values from JSON strings.  
 
### JSON_EXTRACT - JSON Extraction Function
**Grammar**: `json_extract(json_source, path)`  
**Description**: Extract the value of a specified path from a JSON string, Map, or Array. Supports nested objects and array indexes.

**Parameters**:
- `json_source`: Input data, which can be a JSON format string or an object of type Map or Array
- `path`: Extracts paths, supports `.` access to fields, `[]` access to array indexes or Map Key

**Example**:
```sql
-- Extract the basic field
json_extract('{"name": "Alice"}', 'name') -- Back to "Alice"
json_extract('{"name": "Alice"}', '$.name') -- Back to "Alice"

-- Extract nested fields
json_extract('{"user": {"address": {"city": "New York"}}}', 'user.address.city') -- Back to "New York"
json_extract('{"user": {"address": {"city": "New York"}}}', '$.user.address.city') -- Back to "New York"

-- Extract array elements
json_extract('[10, 20, 30]', '[1]') -- Back to 20
json_extract('[10, 20, 30]', '$[1]') -- Back to 20

-- Complex nested extraction
json_extract('{"users": [{"name": "Alice"}, {"name": "Bob"}]}', 'users[1].name') -- Back to "Bob"
```
 
### JSON_VALID - JSON Validation Function
**Grammar**: `json_valid(json_str)`  
**Description**: Verify whether the string is a valid JSON.  

### JSON_TYPE - JSON type function
**Grammar**: `json_type(json_str)`  
**Description**: Returns the type of JSON value.  

### JSON_LENGTH - JSON length function
**Grammar**: `json_length(json_str)`  
**Description**: Returns the length of a JSON array or object.  

## 🔐 Hash function

Hash functions are used to generate the hash value of data.

### MD5 - MD5 Hash function
**Grammar**: `md5(str)`  
**Description**: Generates the MD5 hash of a string.  
 
### SHA1 - SHA1 Hash function
**Grammar**: `sha1(str)`  
**Description**: Generates the SHA-1 hash of a string.  

### SHA256 - SHA256 Hash function
**Grammar**: `sha256(str)`  
**Description**: Generates the SHA-256 hash of a string.  

### SHA512 - SHA512 Hash function
**Grammar**: `sha512(str)`  
**Description**: Generates the SHA-512 hash of a string.  

## 📋 Array functions

Array functions are used to process array data.

### ARRAY_LENGTH - Array length function
**Grammar**: `array_length(array)`  
**Description**: Returns the length of the array.  

### ARRAY_CONTAINS - Array inclusion function
**Grammar**: `array_contains(array, value)`  
**Description**: Check whether the array contains specified values.  
 
### ARRAY_POSITION - Array position function
**Grammar**: `array_position(array, value)`  
**Description**: The position of the returned value in the array.  

### ARRAY_REMOVE - Array removal function
**Grammar**: `array_remove(array, value)`  
**Description**: Remove specified values from an array.  
 

### ARRAY_DISTINCT - Array deduplication function
**Grammar**: `array_distinct(array)`  
**Description**: Returns the deduplication result of the array.  
 
### ARRAY_INTERSECT - Array Intersection Function
**Grammar**: `array_intersect(array1, array2)`  
**Description**: Returns the intersection of two arrays.  
 
### ARRAY_UNION - Array union function
**Grammar**: `array_union(array1, array2)`  
**Description**: Returns the union of two arrays.  
 
### ARRAY_EXCEPT - Array difference set function
**Grammar**: `array_except(array1, array2)`  
**Description**: Returns the difference between two arrays.  
 
## 🔍 Type Check Function

The type check function is used to check data types.

### IS_NULL - Null check function
**Grammar**: `is_null(value)`  
**Description**: Check if the value is NULL.  
 
### IS_NOT_NULL - Non-null check function
**Grammar**: `is_not_null(value)`  
**Description**: Check if the value is not NULL.  
 

### IS_NUMERIC - Numeric Check Function
**Grammar**: `is_numeric(value)`  
**Description**: Check whether the value is of the numeric type.  
 
### IS_STRING - String Check Function
**Grammar**: `is_string(value)`  
**Description**: Check whether the value is of the string type.  
 
### IS_BOOL - Boolean check function
**Grammar**: `is_bool(value)`  
**Description**: Check whether the value is of boolean type.  
 
### IS_ARRAY - Array check function
**Grammar**: `is_array(value)`  
**Description**: Check whether the value is of array type.  
 
### IS_OBJECT - Object Checking Function
**Grammar**: `is_object(value)`  
**Description**: Check whether the value is of the object type.  
 
## ❓ Conditional Function

Conditional functions are used for conditional judgment and value selection.

### IF_NULL - Null Value Handler Function
**Grammar**: `if_null(value, default_value)`  
**Description**: If the value is NULL, returns the default value; otherwise, returns the original value.  
 
### COALESCE - Merge function
**Grammar**: `coalesce(value1, value2,.)`  
**Description**: Returns the first non-NULL value.  

### NULL_IF - Null Value Conversion Function
**Grammar**: `null_if(value1, value2)`  
**Description**: If two values are equal, return NULL; otherwise, return the first value.  

### GREATEST - Maximum value function
**Grammar**: `greatest(value1, value2,.)`  
**Description**: Returns the maximum value in the parameters.  

### LEAST - Minimum value function
**Grammar**: `least(value1, value2,.)`  
**Description**: Returns the minimum value in the argument.  

### CASE_WHEN - Conditional Selection Function
**Grammar**: `case_when(condition, value_if_true, value_if_false)`  
**Description**: Returns different values based on conditions.  

## 📊 Multiline Functions

Multi-line functions are used to process multi-line data.

### UNNEST - Expansion Function
**Grammar**: `unnest(array)`  
**Description**: Expand the array into multiple rows.  
 
## 🪟 Extend window function

The extended window function provides more window-related features.

### ROW_NUMBER - Line number function
**Grammar**: `row_number() OVER (ORDER BY col)`  
**Description**: Assign a unique row number to each row in the result set.  
**Incremental Calculation**: ✅ Supported  

### FIRST_VALUE - Head-value function
**Grammar**: `first_value(col) OVER (ORDER BY col)`  
**Description**: Returns the value of the first row in the window.  
**Incremental Calculation**: ✅ Supported  

### LEAD - Preceding Function
**Grammar**: `lead(col, offset, default_value) OVER (ORDER BY col)`  
**Description**: Returns the value of the N line after the current line.  
**Incremental Calculation**: ✅ Supported  

### NTH_VALUE - The N th valued function
**Grammar**: `nth_value(col, n) OVER (ORDER BY col)`  
**Description**: Returns the value from row N in the window.  
**Incremental Calculation**: ✅ Supported  

## 🔧 Expression Function

Expression functions are used for dynamic expression calculations.

### EXPRESSION - Expression function
**Grammar**: `expression(expr_str)`  
**Description**: Dynamically computes an expression string.  

### EXPR - Expression shorthand function
**Grammar**: `expr(expr_str)`  
**Describe the abbreviated form of the**:expression function.  
 
## ⚡ Advantages in incremental computing performance

Functions supporting incremental computing have the following performance advantages:

### Memory Efficiency
- **Traditional batch computing**: Needs to store all data within the window, memory usage O(n)
- **Incremental Computation**: Only stores necessary state information, memory uses O(1) or O(log n)

### Computational Efficiency
- **Traditional batch computing**: All data is recalculated each time a window is triggered, with time complexity O(n)
- **Incremental Calculation**: Only processes newly added data, time complexity O(1)

### Real-time capability
- **Traditional batch computing**: Only outputs results at the end of the window
- **Incremental Calculation**: Can output intermediate results in real time

## 🔧 Custom Function Extensions

StreamSQL Supports custom function extensions; see examples in `functions/custom_example.go` for details. It can be achieved:
- Custom aggregation functions (supports incremental calculations)
- Custom analysis functions (supports state management)
- Custom mathematical functions
- Custom string function

By implementing the corresponding interfaces, custom functions can be seamlessly integrated into StreamSQL's function system.
