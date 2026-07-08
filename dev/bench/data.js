window.BENCHMARK_DATA = {
  "lastUpdate": 1783507398392,
  "repoUrl": "https://github.com/rulego/streamsql",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "rulego@outlook.com",
            "name": "rulego-team",
            "username": "whki"
          },
          "committer": {
            "email": "rulego@outlook.com",
            "name": "rulego-team",
            "username": "whki"
          },
          "distinct": true,
          "id": "ab84bbcbd51a18e488a1712f9c0b6dafe614ff1d",
          "message": "test: stress 测试改等结果通道空闲再 GC，修复慢机器泄漏误报",
          "timestamp": "2026-07-08T18:32:11+08:00",
          "tree_id": "e2c2b44c59ff8d82cd49c0d4b47b67a73579cfb2",
          "url": "https://github.com/rulego/streamsql/commit/ab84bbcbd51a18e488a1712f9c0b6dafe614ff1d"
        },
        "date": 1783507057962,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 391.1,
            "unit": "ns/op\t         0 drop_rate_%\t   2556682 ops/sec\t        11.43 process_rate_%\t      65 B/op\t       0 allocs/op",
            "extra": "6594127 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 391.1,
            "unit": "ns/op",
            "extra": "6594127 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "6594127 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 2556682,
            "unit": "ops/sec",
            "extra": "6594127 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 11.43,
            "unit": "process_rate_%",
            "extra": "6594127 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 65,
            "unit": "B/op",
            "extra": "6594127 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "6594127 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 429.4,
            "unit": "ns/op\t   2328897 pure_ops/sec\t      81 B/op\t       0 allocs/op",
            "extra": "6475909 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 429.4,
            "unit": "ns/op",
            "extra": "6475909 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 2328897,
            "unit": "pure_ops/sec",
            "extra": "6475909 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 81,
            "unit": "B/op",
            "extra": "6475909 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "6475909 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 431.5,
            "unit": "ns/op\t        68.10 data_chan_usage_%\t   2317327 ops/sec\t        13.25 process_rate_%\t         1.200 result_chan_usage_%\t    832118 results\t      75 B/op\t       0 allocs/op",
            "extra": "6280135 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 431.5,
            "unit": "ns/op",
            "extra": "6280135 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 68.1,
            "unit": "data_chan_usage_%",
            "extra": "6280135 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 2317327,
            "unit": "ops/sec",
            "extra": "6280135 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 13.25,
            "unit": "process_rate_%",
            "extra": "6280135 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 1.2,
            "unit": "result_chan_usage_%",
            "extra": "6280135 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 832118,
            "unit": "results",
            "extra": "6280135 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 75,
            "unit": "B/op",
            "extra": "6280135 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "6280135 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 236,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "10100917 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 236,
            "unit": "ns/op",
            "extra": "10100917 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "10100917 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "10100917 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 302.8,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7883242 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 302.8,
            "unit": "ns/op",
            "extra": "7883242 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7883242 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7883242 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1600,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1511004 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1600,
            "unit": "ns/op",
            "extra": "1511004 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1511004 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1511004 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 612.5,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3876492 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 612.5,
            "unit": "ns/op",
            "extra": "3876492 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3876492 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3876492 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 258.5,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9192570 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 258.5,
            "unit": "ns/op",
            "extra": "9192570 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9192570 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9192570 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 15.83,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "158949800 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 15.83,
            "unit": "ns/op",
            "extra": "158949800 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "158949800 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "158949800 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 217,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "727571608 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 217,
            "unit": "ns/op",
            "extra": "727571608 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "727571608 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "727571608 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 917,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2552634 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 917,
            "unit": "ns/op",
            "extra": "2552634 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2552634 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2552634 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 823.5,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2898650 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 823.5,
            "unit": "ns/op",
            "extra": "2898650 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2898650 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2898650 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 386.4,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6225036 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 386.4,
            "unit": "ns/op",
            "extra": "6225036 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6225036 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6225036 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 408.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5941118 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 408.9,
            "unit": "ns/op",
            "extra": "5941118 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5941118 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5941118 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 62473,
            "unit": "ns/op\t   64860 B/op\t     307 allocs/op",
            "extra": "36482 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 62473,
            "unit": "ns/op",
            "extra": "36482 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64860,
            "unit": "B/op",
            "extra": "36482 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "36482 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 16258,
            "unit": "ns/op\t   19640 B/op\t      11 allocs/op",
            "extra": "156134 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16258,
            "unit": "ns/op",
            "extra": "156134 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19640,
            "unit": "B/op",
            "extra": "156134 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "156134 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 497.7,
            "unit": "ns/op\t     332 B/op\t       5 allocs/op",
            "extra": "4862368 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 497.7,
            "unit": "ns/op",
            "extra": "4862368 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 332,
            "unit": "B/op",
            "extra": "4862368 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4862368 times\n4 procs"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rulego@outlook.com",
            "name": "rulego-team",
            "username": "whki"
          },
          "committer": {
            "email": "rulego@outlook.com",
            "name": "rulego-team",
            "username": "whki"
          },
          "distinct": true,
          "id": "1aed5320f150921f696699b9e9e0e992a37b735c",
          "message": "test: stress 测试改等结果通道空闲再 GC，修复慢机器泄漏误报",
          "timestamp": "2026-07-08T18:40:37+08:00",
          "tree_id": "e5f937e4d048b0a0815e480724cb619314b36102",
          "url": "https://github.com/rulego/streamsql/commit/1aed5320f150921f696699b9e9e0e992a37b735c"
        },
        "date": 1783507397968,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 300.2,
            "unit": "ns/op\t         0 drop_rate_%\t   3330685 ops/sec\t        14.18 process_rate_%\t      69 B/op\t       0 allocs/op",
            "extra": "8266731 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 300.2,
            "unit": "ns/op",
            "extra": "8266731 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "8266731 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 3330685,
            "unit": "ops/sec",
            "extra": "8266731 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 14.18,
            "unit": "process_rate_%",
            "extra": "8266731 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 69,
            "unit": "B/op",
            "extra": "8266731 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8266731 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 307.4,
            "unit": "ns/op\t   3253103 pure_ops/sec\t      68 B/op\t       0 allocs/op",
            "extra": "7339915 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 307.4,
            "unit": "ns/op",
            "extra": "7339915 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 3253103,
            "unit": "pure_ops/sec",
            "extra": "7339915 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "7339915 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "7339915 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 291.8,
            "unit": "ns/op\t        91.95 data_chan_usage_%\t   3427279 ops/sec\t        10.64 process_rate_%\t         4.200 result_chan_usage_%\t    860001 results\t      59 B/op\t       0 allocs/op",
            "extra": "8084178 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 291.8,
            "unit": "ns/op",
            "extra": "8084178 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 91.95,
            "unit": "data_chan_usage_%",
            "extra": "8084178 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 3427279,
            "unit": "ops/sec",
            "extra": "8084178 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 10.64,
            "unit": "process_rate_%",
            "extra": "8084178 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 4.2,
            "unit": "result_chan_usage_%",
            "extra": "8084178 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 860001,
            "unit": "results",
            "extra": "8084178 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 59,
            "unit": "B/op",
            "extra": "8084178 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8084178 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 229.5,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "10476960 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 229.5,
            "unit": "ns/op",
            "extra": "10476960 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "10476960 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "10476960 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 292.1,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8276084 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 292.1,
            "unit": "ns/op",
            "extra": "8276084 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8276084 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8276084 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1655,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1450563 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1655,
            "unit": "ns/op",
            "extra": "1450563 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1450563 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1450563 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 597.9,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3986462 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 597.9,
            "unit": "ns/op",
            "extra": "3986462 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3986462 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3986462 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 250,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9615136 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 250,
            "unit": "ns/op",
            "extra": "9615136 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9615136 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9615136 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 16.67,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "143928236 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16.67,
            "unit": "ns/op",
            "extra": "143928236 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "143928236 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "143928236 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.03,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "635237149 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.03,
            "unit": "ns/op",
            "extra": "635237149 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "635237149 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "635237149 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 1004,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2364004 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 1004,
            "unit": "ns/op",
            "extra": "2364004 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2364004 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2364004 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 881.3,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2731976 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 881.3,
            "unit": "ns/op",
            "extra": "2731976 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2731976 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2731976 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 340.9,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6914606 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 340.9,
            "unit": "ns/op",
            "extra": "6914606 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6914606 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6914606 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 428.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5525673 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 428.6,
            "unit": "ns/op",
            "extra": "5525673 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5525673 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5525673 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 62179,
            "unit": "ns/op\t   64861 B/op\t     307 allocs/op",
            "extra": "38658 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 62179,
            "unit": "ns/op",
            "extra": "38658 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64861,
            "unit": "B/op",
            "extra": "38658 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "38658 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 15994,
            "unit": "ns/op\t   19642 B/op\t      11 allocs/op",
            "extra": "145851 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 15994,
            "unit": "ns/op",
            "extra": "145851 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19642,
            "unit": "B/op",
            "extra": "145851 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "145851 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 512.6,
            "unit": "ns/op\t     336 B/op\t       5 allocs/op",
            "extra": "4522078 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 512.6,
            "unit": "ns/op",
            "extra": "4522078 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 336,
            "unit": "B/op",
            "extra": "4522078 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4522078 times\n4 procs"
          }
        ]
      }
    ]
  }
}