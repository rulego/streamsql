# StreamSQL Example of non-aggregated scenario usage

This example demonstrates various applications of StreamSQL in non-aggregated scenarios, including real-time data conversion, filtering, cleaning, and more.

## Running Example

```bash
cd examples/non-aggregation
go run main.go
```

## Scenario description

### 1. Real-time data cleaning and standardization

**Scenario Description**: Cleaning and standardizing dirty input data, including:
- Null Value Handling (COALESCE)
- String normalization (UPPER, TRIM)
- Numerical Precision Processing (ROUND)
- Status code conversion (CASE WHEN)
- Invalid data filtering (WHERE condition)

**Applicable Scenarios**:
- IoT Equipment data cleaning
- Log standardization
- Data quality assurance

### 2. Data enrichment and computation fields

**Scenario Description**: Calculate and add new fields based on raw data, including:
- Unit conversion (Celsius to Fahrenheit)
- Classification label generation (temperature grading)
- String concatenation (full identifier)
- Add timestamps
- Ratio calculation

**Applicable Scenarios**:
- Data preprocessing
- Application of business rules
- Indicator calculation

### 3. Real-time alerts and event filtering

**Scenario Description**: Real-time detection of abnormal data and generation of alarm events, including:
- Threshold detection
- Alarm level classification
- Alert message generation
- Timestamp records

**Applicable Scenarios**:
- Monitoring system
- Anomaly detection
- Real-time alerts

### 4. Data format conversion

**Scenario Description**: Convert data into different formats, including:
- JSON Format output
- CSV Format output
- Custom format conversion

**Applicable Scenarios**:
- Data interface adaptation
- Multi-system integration
- Data export

### 5. Condition-based data routing

**Scenario Description**: Determine the routing target based on the data content, including:
- Conditional routing rules
- Priority classification
- Themed distribution

**Applicable Scenarios**:
- Message queue routing
- Data distribution
- Load balancing

### 6. Nested field processing

**Scenario Description**: Handling complex nested JSON data, including:
- Deep field extraction
- Nested field combinations
- Conditional judgment

**Applicable Scenarios**:
- JSON Data processing
- Parsing complex data structures
- API Data transformation

## Core Features

### Real-time processing
- Every data entry is processed instantly, with no waiting windows
- Ultra-low latency, suitable for real-time scenarios
- Supports high-throughput data streams

### Rich function support
- String handling: UPPER, LOWER, TRIM, CONCAT, SUBSTRING, etc
- Mathematical calculation: ROUND, CAST, arithmetic operations, etc
- Conditional judgment: CASE WHEN, COALESCE, IF, etc
- Time function: NOW, DATE_FORMAT, etc
- Type conversion: CAST, TO_JSON, etc

### Flexible field operations
- Field selection and aliases
- Nested Field Access (Dotted Syntax)
- Calculate field generation
- Expression calculation

### Powerful filtration capabilities
- WHERE Conditional filtering
- Support for complex expressions
- Multi-conditional combination (AND, OR)
- Pattern matching (LIKE syntax)

## Performance Features

- **Low latency**: Each data item is processed as output immediately
- **High Throughput**: Supports high-frequency data streams
- **Memory-friendly**: No need for caching data, instant processing
- **CPU Efficient**: Simple data conversion operations
