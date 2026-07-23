# StreamSQL Comprehensive test demonstration

This example provides a unified entry point to test and validate various functional features of StreamSQL.

## Feature Override

### 1. Basic data filtering
- Simple WHERE conditional filtering
- Real-time data stream processing
- Result callback processing

### 2. Aggregate analysis
- Scrolling window aggregation (TumblingWindow)
- Multiple aggregate functions: AVG, COUNT, MAX, MIN
- Grouping by field

### 3. Sliding window
- Sliding window analysis (SlidingWindow)
- Window size and sliding spacing configuration
- Continuous data stream processing

### 4. Nested field access
- Multi-layer nested object access
- Handling complex data structures
- Nested field conditional filtering

### 5. Custom functions
- Mathematical functions (square, circle_area)
- Transformation Function (f_to_c)
- Function registration and usage

### 6. Complex queries
- Multiple feature combinations
- Nested fields + custom functions + aggregation
- Complex business scenario simulation

## Operating Mode

```bash
cd examples\comprehensive-test
go run main.go
```

## Expected Output

The program will sequentially execute six test scenarios, each producing corresponding results:

1. **Basic Filtration Test**: Displays alarms for equipment with temperatures above 25°C
2. **Aggregate Analysis Test**: Displays temperature statistics for each device
3. **Sliding Window Test**: Displays temperature analysis inside the sliding window
4. **Nested Field Testing**: Displays field extraction for complex data structures
5. **Custom Function Test**: Displays the calculation results of the custom function
6. **Complex Query Test**: Displays query results for comprehensive functions

## Test Data

- **Sensor Data**: Contains information such as device ID, temperature, humidity, etc
- **Nested Structure**: Multi-layer nesting of device information, location information, and sensor data
- **Random Data**: Generates simulated real sensor data streams using random numbers

## Custom function description

### square(x)
- **Function**: Calculate the square of a value
- **Parameter**: Numeric value
- **Returns**: squared value

### f_to_c(fahrenheit)
- **Function**: Fahrenheit to Celsius
- **Parameter**: Fahrenheit temperature value
- **Return**: Celsius temperature value
- **Formula**:(F - 32) × 5/9

### circle_area(radius)
- **Function**: Calculate the area of a circle
- **Parameter**: Radius
- **Return**: Area of the circle
- **Formula**: π × r²

## Notes

1. **Window Trigger**: Aggregated queries need to wait for window time to arrive or to trigger manually
2. **Data Format**: Ensure the input data format is correct, especially the structure of nested fields
3. **Function Registration**: Custom functions need to be registered before use
4. **Resource Cleanup**: Use defer to ensure the StreamSQL instance closes correctly

## Extended Suggestions

- More custom functions can be added
- More complex window configurations can be tested
- Error handling and anomaly data testing can be added
- Performance testing and stress testing can be integrated
