window.BENCHMARK_DATA = {
  "lastUpdate": 1784768694575,
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
          "id": "1278e3ef20abd4c161a4ddc42ab190677ec3918d",
          "message": "test: stress 测试改等结果通道空闲再 GC，修复慢机器泄漏误报",
          "timestamp": "2026-07-08T18:45:15+08:00",
          "tree_id": "f388f6d91bcefbcbc7ae945119216fc1cc6e1860",
          "url": "https://github.com/rulego/streamsql/commit/1278e3ef20abd4c161a4ddc42ab190677ec3918d"
        },
        "date": 1783507680204,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 278.7,
            "unit": "ns/op\t         0 drop_rate_%\t   3587554 ops/sec\t         9.977 process_rate_%\t      54 B/op\t       0 allocs/op",
            "extra": "8301580 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 278.7,
            "unit": "ns/op",
            "extra": "8301580 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "8301580 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 3587554,
            "unit": "ops/sec",
            "extra": "8301580 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 9.977,
            "unit": "process_rate_%",
            "extra": "8301580 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 54,
            "unit": "B/op",
            "extra": "8301580 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8301580 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 282.4,
            "unit": "ns/op\t   3540844 pure_ops/sec\t      67 B/op\t       0 allocs/op",
            "extra": "8343656 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 282.4,
            "unit": "ns/op",
            "extra": "8343656 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 3540844,
            "unit": "pure_ops/sec",
            "extra": "8343656 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 67,
            "unit": "B/op",
            "extra": "8343656 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8343656 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 288.9,
            "unit": "ns/op\t        93.91 data_chan_usage_%\t   3461497 ops/sec\t        10.31 process_rate_%\t         4.200 result_chan_usage_%\t    846692 results\t      58 B/op\t       0 allocs/op",
            "extra": "8209488 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 288.9,
            "unit": "ns/op",
            "extra": "8209488 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 93.91,
            "unit": "data_chan_usage_%",
            "extra": "8209488 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 3461497,
            "unit": "ops/sec",
            "extra": "8209488 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 10.31,
            "unit": "process_rate_%",
            "extra": "8209488 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 4.2,
            "unit": "result_chan_usage_%",
            "extra": "8209488 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 846692,
            "unit": "results",
            "extra": "8209488 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 58,
            "unit": "B/op",
            "extra": "8209488 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8209488 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 243.7,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9926835 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 243.7,
            "unit": "ns/op",
            "extra": "9926835 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9926835 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9926835 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 311.6,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7618646 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 311.6,
            "unit": "ns/op",
            "extra": "7618646 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7618646 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7618646 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1765,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1352816 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1765,
            "unit": "ns/op",
            "extra": "1352816 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1352816 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1352816 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 640.6,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3746924 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 640.6,
            "unit": "ns/op",
            "extra": "3746924 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3746924 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3746924 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 265.6,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8788915 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 265.6,
            "unit": "ns/op",
            "extra": "8788915 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8788915 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8788915 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 16.8,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "134769319 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16.8,
            "unit": "ns/op",
            "extra": "134769319 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "134769319 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "134769319 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.011,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "634784246 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.011,
            "unit": "ns/op",
            "extra": "634784246 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "634784246 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "634784246 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 1013,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2388512 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 1013,
            "unit": "ns/op",
            "extra": "2388512 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2388512 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2388512 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 874.3,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2754330 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 874.3,
            "unit": "ns/op",
            "extra": "2754330 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2754330 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2754330 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 346.7,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6871052 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 346.7,
            "unit": "ns/op",
            "extra": "6871052 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6871052 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6871052 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 423,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5651294 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 423,
            "unit": "ns/op",
            "extra": "5651294 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5651294 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5651294 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 62893,
            "unit": "ns/op\t   64857 B/op\t     307 allocs/op",
            "extra": "37887 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 62893,
            "unit": "ns/op",
            "extra": "37887 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64857,
            "unit": "B/op",
            "extra": "37887 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "37887 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 16246,
            "unit": "ns/op\t   19642 B/op\t      11 allocs/op",
            "extra": "149808 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16246,
            "unit": "ns/op",
            "extra": "149808 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19642,
            "unit": "B/op",
            "extra": "149808 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "149808 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 529,
            "unit": "ns/op\t     332 B/op\t       5 allocs/op",
            "extra": "4350159 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 529,
            "unit": "ns/op",
            "extra": "4350159 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 332,
            "unit": "B/op",
            "extra": "4350159 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4350159 times\n4 procs"
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
          "id": "2bdc9c91416427cc7e21b7c9f1e6ccc1d65d77f6",
          "message": "fix(stream): Start/Stop 加锁，消除并发 lifecycle 数据竞争",
          "timestamp": "2026-07-08T18:55:18+08:00",
          "tree_id": "68c103b0730f86a3c9a01510e6a1414f1baee18e",
          "url": "https://github.com/rulego/streamsql/commit/2bdc9c91416427cc7e21b7c9f1e6ccc1d65d77f6"
        },
        "date": 1783508285619,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 297,
            "unit": "ns/op\t         0 drop_rate_%\t   3367542 ops/sec\t        12.01 process_rate_%\t      61 B/op\t       0 allocs/op",
            "extra": "7998530 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 297,
            "unit": "ns/op",
            "extra": "7998530 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "7998530 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 3367542,
            "unit": "ops/sec",
            "extra": "7998530 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 12.01,
            "unit": "process_rate_%",
            "extra": "7998530 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 61,
            "unit": "B/op",
            "extra": "7998530 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "7998530 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 278.7,
            "unit": "ns/op\t   3587751 pure_ops/sec\t      64 B/op\t       0 allocs/op",
            "extra": "8169130 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 278.7,
            "unit": "ns/op",
            "extra": "8169130 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 3587751,
            "unit": "pure_ops/sec",
            "extra": "8169130 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "8169130 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8169130 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 292.6,
            "unit": "ns/op\t        94.29 data_chan_usage_%\t   3417398 ops/sec\t        10.09 process_rate_%\t         2.600 result_chan_usage_%\t    781977 results\t      58 B/op\t       0 allocs/op",
            "extra": "7749338 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 292.6,
            "unit": "ns/op",
            "extra": "7749338 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 94.29,
            "unit": "data_chan_usage_%",
            "extra": "7749338 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 3417398,
            "unit": "ops/sec",
            "extra": "7749338 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 10.09,
            "unit": "process_rate_%",
            "extra": "7749338 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 2.6,
            "unit": "result_chan_usage_%",
            "extra": "7749338 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 781977,
            "unit": "results",
            "extra": "7749338 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 58,
            "unit": "B/op",
            "extra": "7749338 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "7749338 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 243.1,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9896803 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 243.1,
            "unit": "ns/op",
            "extra": "9896803 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9896803 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9896803 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 307.4,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7839498 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 307.4,
            "unit": "ns/op",
            "extra": "7839498 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7839498 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7839498 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1716,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1403702 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1716,
            "unit": "ns/op",
            "extra": "1403702 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1403702 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1403702 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 636.3,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3694778 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 636.3,
            "unit": "ns/op",
            "extra": "3694778 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3694778 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3694778 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 265.7,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8974107 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 265.7,
            "unit": "ns/op",
            "extra": "8974107 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8974107 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8974107 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 17.22,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "143153625 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 17.22,
            "unit": "ns/op",
            "extra": "143153625 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "143153625 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "143153625 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.105,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "635652069 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.105,
            "unit": "ns/op",
            "extra": "635652069 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "635652069 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "635652069 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 1005,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2377863 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 1005,
            "unit": "ns/op",
            "extra": "2377863 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2377863 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2377863 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 854,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2779099 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 854,
            "unit": "ns/op",
            "extra": "2779099 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2779099 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2779099 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 342.6,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "7028215 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 342.6,
            "unit": "ns/op",
            "extra": "7028215 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "7028215 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "7028215 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 425.3,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5677328 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 425.3,
            "unit": "ns/op",
            "extra": "5677328 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5677328 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5677328 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 63410,
            "unit": "ns/op\t   64867 B/op\t     307 allocs/op",
            "extra": "37906 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 63410,
            "unit": "ns/op",
            "extra": "37906 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64867,
            "unit": "B/op",
            "extra": "37906 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "37906 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 16228,
            "unit": "ns/op\t   19642 B/op\t      11 allocs/op",
            "extra": "148603 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16228,
            "unit": "ns/op",
            "extra": "148603 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19642,
            "unit": "B/op",
            "extra": "148603 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "148603 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 526.7,
            "unit": "ns/op\t     334 B/op\t       5 allocs/op",
            "extra": "4579360 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 526.7,
            "unit": "ns/op",
            "extra": "4579360 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 334,
            "unit": "B/op",
            "extra": "4579360 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4579360 times\n4 procs"
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
          "id": "d94a399ea61f8f42a459b467e0494e2dfad63ce1",
          "message": "fix(stream): Start/Stop 加锁，消除并发 lifecycle 数据竞争",
          "timestamp": "2026-07-08T19:07:03+08:00",
          "tree_id": "ecae4226a83ad94b23dc9ae2562eafbdac06eddc",
          "url": "https://github.com/rulego/streamsql/commit/d94a399ea61f8f42a459b467e0494e2dfad63ce1"
        },
        "date": 1783508975067,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 271.5,
            "unit": "ns/op\t         0 drop_rate_%\t   3682766 ops/sec\t         7.305 process_rate_%\t      44 B/op\t       0 allocs/op",
            "extra": "8132187 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 271.5,
            "unit": "ns/op",
            "extra": "8132187 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "8132187 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 3682766,
            "unit": "ops/sec",
            "extra": "8132187 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 7.305,
            "unit": "process_rate_%",
            "extra": "8132187 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 44,
            "unit": "B/op",
            "extra": "8132187 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8132187 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 334.7,
            "unit": "ns/op\t   2987731 pure_ops/sec\t      62 B/op\t       0 allocs/op",
            "extra": "8756058 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 334.7,
            "unit": "ns/op",
            "extra": "8756058 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 2987731,
            "unit": "pure_ops/sec",
            "extra": "8756058 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 62,
            "unit": "B/op",
            "extra": "8756058 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8756058 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 348.5,
            "unit": "ns/op\t        71.42 data_chan_usage_%\t   2869435 ops/sec\t         9.054 process_rate_%\t         1.800 result_chan_usage_%\t    788075 results\t      61 B/op\t       0 allocs/op",
            "extra": "8704512 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 348.5,
            "unit": "ns/op",
            "extra": "8704512 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 71.42,
            "unit": "data_chan_usage_%",
            "extra": "8704512 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 2869435,
            "unit": "ops/sec",
            "extra": "8704512 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 9.054,
            "unit": "process_rate_%",
            "extra": "8704512 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 1.8,
            "unit": "result_chan_usage_%",
            "extra": "8704512 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 788075,
            "unit": "results",
            "extra": "8704512 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 61,
            "unit": "B/op",
            "extra": "8704512 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8704512 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 206.9,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "11284242 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 206.9,
            "unit": "ns/op",
            "extra": "11284242 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "11284242 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "11284242 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 262.5,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9082292 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 262.5,
            "unit": "ns/op",
            "extra": "9082292 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9082292 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9082292 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1514,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1583440 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1514,
            "unit": "ns/op",
            "extra": "1583440 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1583440 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1583440 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 528.4,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "4555616 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 528.4,
            "unit": "ns/op",
            "extra": "4555616 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "4555616 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4555616 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 232.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "10228660 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 232.2,
            "unit": "ns/op",
            "extra": "10228660 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "10228660 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "10228660 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 16.24,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "147577746 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16.24,
            "unit": "ns/op",
            "extra": "147577746 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "147577746 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "147577746 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.264,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "562838510 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.264,
            "unit": "ns/op",
            "extra": "562838510 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "562838510 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "562838510 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 841.7,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2834552 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 841.7,
            "unit": "ns/op",
            "extra": "2834552 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2834552 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2834552 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 739.2,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "3243182 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 739.2,
            "unit": "ns/op",
            "extra": "3243182 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "3243182 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3243182 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 287.5,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "8284005 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 287.5,
            "unit": "ns/op",
            "extra": "8284005 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "8284005 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "8284005 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 363.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "6565722 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 363.8,
            "unit": "ns/op",
            "extra": "6565722 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "6565722 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6565722 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 57954,
            "unit": "ns/op\t   64860 B/op\t     307 allocs/op",
            "extra": "41736 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 57954,
            "unit": "ns/op",
            "extra": "41736 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64860,
            "unit": "B/op",
            "extra": "41736 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "41736 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 15326,
            "unit": "ns/op\t   19642 B/op\t      11 allocs/op",
            "extra": "157113 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 15326,
            "unit": "ns/op",
            "extra": "157113 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19642,
            "unit": "B/op",
            "extra": "157113 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "157113 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 516,
            "unit": "ns/op\t     331 B/op\t       5 allocs/op",
            "extra": "4573393 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 516,
            "unit": "ns/op",
            "extra": "4573393 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 331,
            "unit": "B/op",
            "extra": "4573393 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4573393 times\n4 procs"
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
          "id": "f9e819650ebe0575ff105da9ec6044c96c10e4a2",
          "message": "feat: 流-表 JOIN 支持聚合与窗口（富化接入窗口路径，LEFT 未匹配归 NULL 分组）",
          "timestamp": "2026-07-09T09:19:17+08:00",
          "tree_id": "c74261cf08297a394294828c4c800ea6b7d72b04",
          "url": "https://github.com/rulego/streamsql/commit/f9e819650ebe0575ff105da9ec6044c96c10e4a2"
        },
        "date": 1783560308360,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 361.2,
            "unit": "ns/op\t         0 drop_rate_%\t   2768692 ops/sec\t        11.91 process_rate_%\t      68 B/op\t       0 allocs/op",
            "extra": "8569866 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 361.2,
            "unit": "ns/op",
            "extra": "8569866 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "8569866 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 2768692,
            "unit": "ops/sec",
            "extra": "8569866 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 11.91,
            "unit": "process_rate_%",
            "extra": "8569866 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "8569866 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8569866 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 288.3,
            "unit": "ns/op\t        95.17 data_chan_usage_%\t   3468291 ops/sec\t        10.39 process_rate_%\t        20.20 result_chan_usage_%\t    815778 results\t      58 B/op\t       0 allocs/op",
            "extra": "7848391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 288.3,
            "unit": "ns/op",
            "extra": "7848391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 95.17,
            "unit": "data_chan_usage_%",
            "extra": "7848391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 3468291,
            "unit": "ops/sec",
            "extra": "7848391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 10.39,
            "unit": "process_rate_%",
            "extra": "7848391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 20.2,
            "unit": "result_chan_usage_%",
            "extra": "7848391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 815778,
            "unit": "results",
            "extra": "7848391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 58,
            "unit": "B/op",
            "extra": "7848391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "7848391 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 242.9,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9958110 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 242.9,
            "unit": "ns/op",
            "extra": "9958110 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9958110 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9958110 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 300.3,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7929825 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 300.3,
            "unit": "ns/op",
            "extra": "7929825 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7929825 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7929825 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1692,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1419069 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1692,
            "unit": "ns/op",
            "extra": "1419069 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1419069 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1419069 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 648.1,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3764884 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 648.1,
            "unit": "ns/op",
            "extra": "3764884 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3764884 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3764884 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 266.9,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8782802 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 266.9,
            "unit": "ns/op",
            "extra": "8782802 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8782802 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8782802 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 17.2,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "143585691 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 17.2,
            "unit": "ns/op",
            "extra": "143585691 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "143585691 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "143585691 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.047,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "633888025 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.047,
            "unit": "ns/op",
            "extra": "633888025 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "633888025 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "633888025 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 999.3,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2408716 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 999.3,
            "unit": "ns/op",
            "extra": "2408716 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2408716 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2408716 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 862.7,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2777071 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 862.7,
            "unit": "ns/op",
            "extra": "2777071 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2777071 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2777071 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 342.2,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "7017565 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 342.2,
            "unit": "ns/op",
            "extra": "7017565 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "7017565 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "7017565 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 419.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5685558 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 419.7,
            "unit": "ns/op",
            "extra": "5685558 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5685558 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5685558 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 62262,
            "unit": "ns/op\t   64850 B/op\t     307 allocs/op",
            "extra": "38937 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 62262,
            "unit": "ns/op",
            "extra": "38937 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64850,
            "unit": "B/op",
            "extra": "38937 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "38937 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 16362,
            "unit": "ns/op\t   19639 B/op\t      11 allocs/op",
            "extra": "150831 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16362,
            "unit": "ns/op",
            "extra": "150831 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19639,
            "unit": "B/op",
            "extra": "150831 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "150831 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 518.9,
            "unit": "ns/op\t     336 B/op\t       5 allocs/op",
            "extra": "4636585 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 518.9,
            "unit": "ns/op",
            "extra": "4636585 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 336,
            "unit": "B/op",
            "extra": "4636585 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4636585 times\n4 procs"
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
          "id": "d068321237ef18e9637137da692261165f990d43",
          "message": "feat: schema 接入 opt-in 校验（SetSchema 触发 Emit/EmitSync 校验，不设零开销）",
          "timestamp": "2026-07-09T09:44:46+08:00",
          "tree_id": "75894a3b8d09118940dee3e55c679ded77639c66",
          "url": "https://github.com/rulego/streamsql/commit/d068321237ef18e9637137da692261165f990d43"
        },
        "date": 1783561645926,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 376.2,
            "unit": "ns/op\t         0 drop_rate_%\t   2658180 ops/sec\t        11.82 process_rate_%\t      69 B/op\t       0 allocs/op",
            "extra": "8225668 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 376.2,
            "unit": "ns/op",
            "extra": "8225668 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "8225668 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 2658180,
            "unit": "ops/sec",
            "extra": "8225668 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 11.82,
            "unit": "process_rate_%",
            "extra": "8225668 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 69,
            "unit": "B/op",
            "extra": "8225668 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8225668 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 318.6,
            "unit": "ns/op\t        74.11 data_chan_usage_%\t   3138486 ops/sec\t        10.88 process_rate_%\t         1.000 result_chan_usage_%\t    884083 results\t      61 B/op\t       0 allocs/op",
            "extra": "8128552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 318.6,
            "unit": "ns/op",
            "extra": "8128552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 74.11,
            "unit": "data_chan_usage_%",
            "extra": "8128552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 3138486,
            "unit": "ops/sec",
            "extra": "8128552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 10.88,
            "unit": "process_rate_%",
            "extra": "8128552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 1,
            "unit": "result_chan_usage_%",
            "extra": "8128552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 884083,
            "unit": "results",
            "extra": "8128552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 61,
            "unit": "B/op",
            "extra": "8128552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8128552 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 250.1,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9533515 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 250.1,
            "unit": "ns/op",
            "extra": "9533515 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9533515 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9533515 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 308.7,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7815279 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 308.7,
            "unit": "ns/op",
            "extra": "7815279 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7815279 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7815279 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1736,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1382138 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1736,
            "unit": "ns/op",
            "extra": "1382138 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1382138 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1382138 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 650.5,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3722990 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 650.5,
            "unit": "ns/op",
            "extra": "3722990 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3722990 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3722990 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 276.1,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8779107 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 276.1,
            "unit": "ns/op",
            "extra": "8779107 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8779107 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8779107 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 16.74,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "143392933 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16.74,
            "unit": "ns/op",
            "extra": "143392933 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "143392933 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "143392933 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 3.953,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "635435070 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 3.953,
            "unit": "ns/op",
            "extra": "635435070 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "635435070 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "635435070 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 1019,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2331477 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 1019,
            "unit": "ns/op",
            "extra": "2331477 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2331477 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2331477 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 862.6,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2767364 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 862.6,
            "unit": "ns/op",
            "extra": "2767364 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2767364 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2767364 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 342.6,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6988774 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 342.6,
            "unit": "ns/op",
            "extra": "6988774 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6988774 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6988774 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 431.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5596557 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 431.6,
            "unit": "ns/op",
            "extra": "5596557 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5596557 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5596557 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 62216,
            "unit": "ns/op\t   64867 B/op\t     307 allocs/op",
            "extra": "38592 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 62216,
            "unit": "ns/op",
            "extra": "38592 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64867,
            "unit": "B/op",
            "extra": "38592 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "38592 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 16260,
            "unit": "ns/op\t   19643 B/op\t      11 allocs/op",
            "extra": "149205 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16260,
            "unit": "ns/op",
            "extra": "149205 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19643,
            "unit": "B/op",
            "extra": "149205 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "149205 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 525.4,
            "unit": "ns/op\t     333 B/op\t       5 allocs/op",
            "extra": "4499760 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 525.4,
            "unit": "ns/op",
            "extra": "4499760 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 333,
            "unit": "B/op",
            "extra": "4499760 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4499760 times\n4 procs"
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
          "id": "90b4e725e6ba4ecec5f6ce84b4875e8340b873f1",
          "message": "feat: 新增 WithSchema(opt-in 校验) 与 WithLogger 选项",
          "timestamp": "2026-07-09T10:21:01+08:00",
          "tree_id": "82cd68c7b30b82985ccc9f37cd632d10c487a791",
          "url": "https://github.com/rulego/streamsql/commit/90b4e725e6ba4ecec5f6ce84b4875e8340b873f1"
        },
        "date": 1783563846855,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 291.1,
            "unit": "ns/op\t         0 drop_rate_%\t   3435231 ops/sec\t        11.99 process_rate_%\t      62 B/op\t       0 allocs/op",
            "extra": "8014983 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 291.1,
            "unit": "ns/op",
            "extra": "8014983 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "8014983 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 3435231,
            "unit": "ops/sec",
            "extra": "8014983 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 11.99,
            "unit": "process_rate_%",
            "extra": "8014983 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 62,
            "unit": "B/op",
            "extra": "8014983 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8014983 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 280.7,
            "unit": "ns/op\t   3562534 pure_ops/sec\t      66 B/op\t       0 allocs/op",
            "extra": "8189466 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 280.7,
            "unit": "ns/op",
            "extra": "8189466 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 3562534,
            "unit": "pure_ops/sec",
            "extra": "8189466 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 66,
            "unit": "B/op",
            "extra": "8189466 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8189466 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 315.1,
            "unit": "ns/op\t        74.39 data_chan_usage_%\t   3173387 ops/sec\t        10.36 process_rate_%\t         1.000 result_chan_usage_%\t    826517 results\t      60 B/op\t       0 allocs/op",
            "extra": "7976412 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 315.1,
            "unit": "ns/op",
            "extra": "7976412 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 74.39,
            "unit": "data_chan_usage_%",
            "extra": "7976412 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 3173387,
            "unit": "ops/sec",
            "extra": "7976412 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 10.36,
            "unit": "process_rate_%",
            "extra": "7976412 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 1,
            "unit": "result_chan_usage_%",
            "extra": "7976412 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 826517,
            "unit": "results",
            "extra": "7976412 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 60,
            "unit": "B/op",
            "extra": "7976412 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "7976412 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 249.1,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9529117 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 249.1,
            "unit": "ns/op",
            "extra": "9529117 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9529117 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9529117 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 314.5,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7767250 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 314.5,
            "unit": "ns/op",
            "extra": "7767250 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7767250 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7767250 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1759,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1359764 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1759,
            "unit": "ns/op",
            "extra": "1359764 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1359764 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1359764 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 653.4,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3676935 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 653.4,
            "unit": "ns/op",
            "extra": "3676935 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3676935 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3676935 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 275.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8787640 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 275.2,
            "unit": "ns/op",
            "extra": "8787640 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8787640 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8787640 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 16.81,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "142328244 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16.81,
            "unit": "ns/op",
            "extra": "142328244 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "142328244 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "142328244 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.115,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "635914806 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.115,
            "unit": "ns/op",
            "extra": "635914806 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "635914806 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "635914806 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 1014,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2354172 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 1014,
            "unit": "ns/op",
            "extra": "2354172 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2354172 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2354172 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 867.1,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2762415 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 867.1,
            "unit": "ns/op",
            "extra": "2762415 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2762415 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2762415 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 344.1,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6914064 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 344.1,
            "unit": "ns/op",
            "extra": "6914064 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6914064 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6914064 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 423.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5694318 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 423.4,
            "unit": "ns/op",
            "extra": "5694318 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5694318 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5694318 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 63579,
            "unit": "ns/op\t   64867 B/op\t     307 allocs/op",
            "extra": "37992 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 63579,
            "unit": "ns/op",
            "extra": "37992 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64867,
            "unit": "B/op",
            "extra": "37992 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "37992 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 16365,
            "unit": "ns/op\t   19641 B/op\t      11 allocs/op",
            "extra": "148128 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16365,
            "unit": "ns/op",
            "extra": "148128 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19641,
            "unit": "B/op",
            "extra": "148128 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "148128 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 534.4,
            "unit": "ns/op\t     332 B/op\t       5 allocs/op",
            "extra": "4336215 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 534.4,
            "unit": "ns/op",
            "extra": "4336215 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 332,
            "unit": "B/op",
            "extra": "4336215 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4336215 times\n4 procs"
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
          "id": "d12949ea45b012952836a207f9fe2c6cbcab068b",
          "message": "refactor: logger 按实例线程化（去全局串扰/竞态）；移除 zero_data_loss、persist 误导性 no-op",
          "timestamp": "2026-07-09T11:12:05+08:00",
          "tree_id": "23dce041a37d96bc4ff4866f5e0e24c10e72bda2",
          "url": "https://github.com/rulego/streamsql/commit/d12949ea45b012952836a207f9fe2c6cbcab068b"
        },
        "date": 1783566868853,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 294.5,
            "unit": "ns/op\t         0 drop_rate_%\t   3395496 ops/sec\t         8.717 process_rate_%\t      51 B/op\t       0 allocs/op",
            "extra": "8111775 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 294.5,
            "unit": "ns/op",
            "extra": "8111775 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "8111775 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 3395496,
            "unit": "ops/sec",
            "extra": "8111775 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 8.717,
            "unit": "process_rate_%",
            "extra": "8111775 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 51,
            "unit": "B/op",
            "extra": "8111775 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8111775 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 265.5,
            "unit": "ns/op\t   3766848 pure_ops/sec\t      59 B/op\t       0 allocs/op",
            "extra": "8205037 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 265.5,
            "unit": "ns/op",
            "extra": "8205037 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 3766848,
            "unit": "pure_ops/sec",
            "extra": "8205037 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 59,
            "unit": "B/op",
            "extra": "8205037 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8205037 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 278.9,
            "unit": "ns/op\t        97.21 data_chan_usage_%\t   3585663 ops/sec\t         8.915 process_rate_%\t         2.800 result_chan_usage_%\t    739256 results\t      52 B/op\t       0 allocs/op",
            "extra": "8292063 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 278.9,
            "unit": "ns/op",
            "extra": "8292063 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 97.21,
            "unit": "data_chan_usage_%",
            "extra": "8292063 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 3585663,
            "unit": "ops/sec",
            "extra": "8292063 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 8.915,
            "unit": "process_rate_%",
            "extra": "8292063 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 2.8,
            "unit": "result_chan_usage_%",
            "extra": "8292063 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 739256,
            "unit": "results",
            "extra": "8292063 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 52,
            "unit": "B/op",
            "extra": "8292063 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8292063 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 218.9,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "10848747 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 218.9,
            "unit": "ns/op",
            "extra": "10848747 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "10848747 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "10848747 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 280.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8514691 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 280.2,
            "unit": "ns/op",
            "extra": "8514691 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8514691 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8514691 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1535,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1566640 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1535,
            "unit": "ns/op",
            "extra": "1566640 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1566640 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1566640 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 570.7,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "4249477 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 570.7,
            "unit": "ns/op",
            "extra": "4249477 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "4249477 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4249477 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 248.1,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9785790 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 248.1,
            "unit": "ns/op",
            "extra": "9785790 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9785790 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9785790 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 16.36,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "146477454 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16.36,
            "unit": "ns/op",
            "extra": "146477454 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "146477454 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "146477454 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.256,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "563702329 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.256,
            "unit": "ns/op",
            "extra": "563702329 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "563702329 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "563702329 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 846,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2825252 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 846,
            "unit": "ns/op",
            "extra": "2825252 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2825252 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2825252 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 744.2,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "3137860 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 744.2,
            "unit": "ns/op",
            "extra": "3137860 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "3137860 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3137860 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 292.4,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "8166580 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 292.4,
            "unit": "ns/op",
            "extra": "8166580 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "8166580 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "8166580 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 366.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "6557494 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 366.9,
            "unit": "ns/op",
            "extra": "6557494 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "6557494 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6557494 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 58002,
            "unit": "ns/op\t   64862 B/op\t     307 allocs/op",
            "extra": "41590 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 58002,
            "unit": "ns/op",
            "extra": "41590 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64862,
            "unit": "B/op",
            "extra": "41590 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "41590 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 15596,
            "unit": "ns/op\t   19640 B/op\t      11 allocs/op",
            "extra": "156601 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 15596,
            "unit": "ns/op",
            "extra": "156601 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19640,
            "unit": "B/op",
            "extra": "156601 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "156601 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 523.8,
            "unit": "ns/op\t     334 B/op\t       5 allocs/op",
            "extra": "4455757 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 523.8,
            "unit": "ns/op",
            "extra": "4455757 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 334,
            "unit": "B/op",
            "extra": "4455757 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4455757 times\n4 procs"
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
          "id": "d92c67d33f91057f461614505f0a99eecb29c5b0",
          "message": "fix(stream): overflow 策略修复——Block 超时即丢、Drop 去死分支；丢弃计数拆 input/output",
          "timestamp": "2026-07-09T11:36:14+08:00",
          "tree_id": "2a0824e6634ce973f506423fbacc636f6d9f223c",
          "url": "https://github.com/rulego/streamsql/commit/d92c67d33f91057f461614505f0a99eecb29c5b0"
        },
        "date": 1783568322565,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 499.2,
            "unit": "ns/op\t         0 drop_rate_%\t   2003371 ops/sec\t        66.67 process_rate_%\t     267 B/op\t       2 allocs/op",
            "extra": "4738401 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 499.2,
            "unit": "ns/op",
            "extra": "4738401 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "4738401 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 2003371,
            "unit": "ops/sec",
            "extra": "4738401 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 66.67,
            "unit": "process_rate_%",
            "extra": "4738401 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 267,
            "unit": "B/op",
            "extra": "4738401 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "4738401 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 302.4,
            "unit": "ns/op\t         0 drop_rate_%\t   3306663 ops/sec\t        10.33 process_rate_%\t      62 B/op\t       0 allocs/op",
            "extra": "16403408 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 302.4,
            "unit": "ns/op",
            "extra": "16403408 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "16403408 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 3306663,
            "unit": "ops/sec",
            "extra": "16403408 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 10.33,
            "unit": "process_rate_%",
            "extra": "16403408 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 62,
            "unit": "B/op",
            "extra": "16403408 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "16403408 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 284.3,
            "unit": "ns/op\t   3517720 pure_ops/sec\t      67 B/op\t       0 allocs/op",
            "extra": "7975867 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 284.3,
            "unit": "ns/op",
            "extra": "7975867 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 3517720,
            "unit": "pure_ops/sec",
            "extra": "7975867 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 67,
            "unit": "B/op",
            "extra": "7975867 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "7975867 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 677,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1477199 ops/sec\t       100.0 process_rate_%\t         0 result_chan_usage_%\t   7051616 results\t     400 B/op\t       4 allocs/op",
            "extra": "7051616 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 677,
            "unit": "ns/op",
            "extra": "7051616 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "7051616 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1477199,
            "unit": "ops/sec",
            "extra": "7051616 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "7051616 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "7051616 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 7051616,
            "unit": "results",
            "extra": "7051616 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 400,
            "unit": "B/op",
            "extra": "7051616 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "7051616 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 303.5,
            "unit": "ns/op\t        79.37 data_chan_usage_%\t   3295287 ops/sec\t         9.646 process_rate_%\t         0.8000 result_chan_usage_%\t    796024 results\t      56 B/op\t       0 allocs/op",
            "extra": "8252620 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 303.5,
            "unit": "ns/op",
            "extra": "8252620 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 79.37,
            "unit": "data_chan_usage_%",
            "extra": "8252620 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 3295287,
            "unit": "ops/sec",
            "extra": "8252620 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 9.646,
            "unit": "process_rate_%",
            "extra": "8252620 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0.8,
            "unit": "result_chan_usage_%",
            "extra": "8252620 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 796024,
            "unit": "results",
            "extra": "8252620 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 56,
            "unit": "B/op",
            "extra": "8252620 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8252620 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 251.8,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9554686 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 251.8,
            "unit": "ns/op",
            "extra": "9554686 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9554686 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9554686 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 315.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7636306 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 315.2,
            "unit": "ns/op",
            "extra": "7636306 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7636306 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7636306 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1727,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1384870 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1727,
            "unit": "ns/op",
            "extra": "1384870 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1384870 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1384870 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 655.2,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3571488 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 655.2,
            "unit": "ns/op",
            "extra": "3571488 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3571488 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3571488 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 276,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8689274 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 276,
            "unit": "ns/op",
            "extra": "8689274 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8689274 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8689274 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 16.74,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "143446645 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16.74,
            "unit": "ns/op",
            "extra": "143446645 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "143446645 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "143446645 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 3.981,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "633984549 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 3.981,
            "unit": "ns/op",
            "extra": "633984549 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "633984549 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "633984549 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 1019,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2335092 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 1019,
            "unit": "ns/op",
            "extra": "2335092 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2335092 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2335092 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 862.4,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2743711 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 862.4,
            "unit": "ns/op",
            "extra": "2743711 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2743711 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2743711 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 342.3,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6991459 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 342.3,
            "unit": "ns/op",
            "extra": "6991459 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6991459 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6991459 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 419.3,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5717613 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 419.3,
            "unit": "ns/op",
            "extra": "5717613 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5717613 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5717613 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 62461,
            "unit": "ns/op\t   64863 B/op\t     307 allocs/op",
            "extra": "38587 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 62461,
            "unit": "ns/op",
            "extra": "38587 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64863,
            "unit": "B/op",
            "extra": "38587 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "38587 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 15890,
            "unit": "ns/op\t   19641 B/op\t      11 allocs/op",
            "extra": "151602 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 15890,
            "unit": "ns/op",
            "extra": "151602 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19641,
            "unit": "B/op",
            "extra": "151602 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "151602 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 519.7,
            "unit": "ns/op\t     336 B/op\t       5 allocs/op",
            "extra": "4632802 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 519.7,
            "unit": "ns/op",
            "extra": "4632802 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 336,
            "unit": "B/op",
            "extra": "4632802 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4632802 times\n4 procs"
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
          "id": "73ada71636fb066d1f9c8d2eccf139b48801d0cd",
          "message": "fix(schema): Default 真正填值进 row（原仅压住 required 报错）",
          "timestamp": "2026-07-09T13:56:53+08:00",
          "tree_id": "8afb73b995f9a3fc5f73764eff10397e7b656555",
          "url": "https://github.com/rulego/streamsql/commit/73ada71636fb066d1f9c8d2eccf139b48801d0cd"
        },
        "date": 1783578025488,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 501.7,
            "unit": "ns/op\t         0 drop_rate_%\t   1993266 ops/sec\t        66.67 process_rate_%\t     270 B/op\t       2 allocs/op",
            "extra": "4760912 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 501.7,
            "unit": "ns/op",
            "extra": "4760912 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "4760912 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 1993266,
            "unit": "ops/sec",
            "extra": "4760912 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 66.67,
            "unit": "process_rate_%",
            "extra": "4760912 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 270,
            "unit": "B/op",
            "extra": "4760912 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "4760912 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 734.4,
            "unit": "ns/op\t         0 drop_rate_%\t   1361577 ops/sec\t        93.10 process_rate_%\t     358 B/op\t       3 allocs/op",
            "extra": "5294005 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 734.4,
            "unit": "ns/op",
            "extra": "5294005 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "5294005 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 1361577,
            "unit": "ops/sec",
            "extra": "5294005 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 93.1,
            "unit": "process_rate_%",
            "extra": "5294005 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 358,
            "unit": "B/op",
            "extra": "5294005 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "5294005 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 643.7,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1553587 ops/sec\t       100.0 process_rate_%\t         0 result_chan_usage_%\t   4873714 results\t     403 B/op\t       4 allocs/op",
            "extra": "4873714 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 643.7,
            "unit": "ns/op",
            "extra": "4873714 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "4873714 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1553587,
            "unit": "ops/sec",
            "extra": "4873714 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "4873714 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "4873714 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 4873714,
            "unit": "results",
            "extra": "4873714 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 403,
            "unit": "B/op",
            "extra": "4873714 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "4873714 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 221.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "10614976 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 221.2,
            "unit": "ns/op",
            "extra": "10614976 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "10614976 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "10614976 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 281.8,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8611711 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 281.8,
            "unit": "ns/op",
            "extra": "8611711 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8611711 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8611711 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1571,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1532264 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1571,
            "unit": "ns/op",
            "extra": "1532264 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1532264 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1532264 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 566.1,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "4220160 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 566.1,
            "unit": "ns/op",
            "extra": "4220160 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "4220160 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4220160 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 248.6,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9840993 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 248.6,
            "unit": "ns/op",
            "extra": "9840993 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9840993 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9840993 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 16.61,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "147694192 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16.61,
            "unit": "ns/op",
            "extra": "147694192 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "147694192 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "147694192 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.271,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "563080548 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.271,
            "unit": "ns/op",
            "extra": "563080548 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "563080548 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "563080548 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 847.4,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2831048 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 847.4,
            "unit": "ns/op",
            "extra": "2831048 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2831048 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2831048 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 738.5,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "3226700 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 738.5,
            "unit": "ns/op",
            "extra": "3226700 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "3226700 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3226700 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 289.4,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "8310440 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 289.4,
            "unit": "ns/op",
            "extra": "8310440 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "8310440 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "8310440 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 367.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "6496035 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 367.7,
            "unit": "ns/op",
            "extra": "6496035 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "6496035 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6496035 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 58316,
            "unit": "ns/op\t   64864 B/op\t     307 allocs/op",
            "extra": "41139 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 58316,
            "unit": "ns/op",
            "extra": "41139 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64864,
            "unit": "B/op",
            "extra": "41139 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "41139 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 15682,
            "unit": "ns/op\t   19641 B/op\t      11 allocs/op",
            "extra": "160345 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 15682,
            "unit": "ns/op",
            "extra": "160345 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19641,
            "unit": "B/op",
            "extra": "160345 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "160345 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 519.2,
            "unit": "ns/op\t     330 B/op\t       5 allocs/op",
            "extra": "4572688 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 519.2,
            "unit": "ns/op",
            "extra": "4572688 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 330,
            "unit": "B/op",
            "extra": "4572688 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4572688 times\n4 procs"
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
          "distinct": false,
          "id": "73ada71636fb066d1f9c8d2eccf139b48801d0cd",
          "message": "fix(schema): Default 真正填值进 row（原仅压住 required 报错）",
          "timestamp": "2026-07-09T13:56:53+08:00",
          "tree_id": "8afb73b995f9a3fc5f73764eff10397e7b656555",
          "url": "https://github.com/rulego/streamsql/commit/73ada71636fb066d1f9c8d2eccf139b48801d0cd"
        },
        "date": 1783578898573,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 494.8,
            "unit": "ns/op\t         0 drop_rate_%\t   2021037 ops/sec\t        66.67 process_rate_%\t     266 B/op\t       2 allocs/op",
            "extra": "4872585 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 494.8,
            "unit": "ns/op",
            "extra": "4872585 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "4872585 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 2021037,
            "unit": "ops/sec",
            "extra": "4872585 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 66.67,
            "unit": "process_rate_%",
            "extra": "4872585 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 266,
            "unit": "B/op",
            "extra": "4872585 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "4872585 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 548.8,
            "unit": "ns/op\t         0 drop_rate_%\t   1822154 ops/sec\t        62.89 process_rate_%\t     240 B/op\t       2 allocs/op",
            "extra": "5325529 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 548.8,
            "unit": "ns/op",
            "extra": "5325529 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "5325529 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 1822154,
            "unit": "ops/sec",
            "extra": "5325529 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 62.89,
            "unit": "process_rate_%",
            "extra": "5325529 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "5325529 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5325529 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 548.4,
            "unit": "ns/op\t   1823259 pure_ops/sec\t     315 B/op\t       2 allocs/op",
            "extra": "5355967 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 548.4,
            "unit": "ns/op",
            "extra": "5355967 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 1823259,
            "unit": "pure_ops/sec",
            "extra": "5355967 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 315,
            "unit": "B/op",
            "extra": "5355967 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5355967 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 643,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1555154 ops/sec\t       100.0 process_rate_%\t         0 result_chan_usage_%\t   5044162 results\t     399 B/op\t       4 allocs/op",
            "extra": "5044162 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 643,
            "unit": "ns/op",
            "extra": "5044162 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "5044162 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1555154,
            "unit": "ops/sec",
            "extra": "5044162 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "5044162 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "5044162 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 5044162,
            "unit": "results",
            "extra": "5044162 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 399,
            "unit": "B/op",
            "extra": "5044162 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "5044162 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 730.6,
            "unit": "ns/op\t        86.64 data_chan_usage_%\t   1368753 ops/sec\t        91.94 process_rate_%\t         0 result_chan_usage_%\t   4942212 results\t     359 B/op\t       3 allocs/op",
            "extra": "5375391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 730.6,
            "unit": "ns/op",
            "extra": "5375391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 86.64,
            "unit": "data_chan_usage_%",
            "extra": "5375391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 1368753,
            "unit": "ops/sec",
            "extra": "5375391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 91.94,
            "unit": "process_rate_%",
            "extra": "5375391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "5375391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 4942212,
            "unit": "results",
            "extra": "5375391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 359,
            "unit": "B/op",
            "extra": "5375391 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "5375391 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 248.8,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9667077 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 248.8,
            "unit": "ns/op",
            "extra": "9667077 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9667077 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9667077 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 316.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7630233 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 316.2,
            "unit": "ns/op",
            "extra": "7630233 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7630233 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7630233 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1743,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1366389 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1743,
            "unit": "ns/op",
            "extra": "1366389 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1366389 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1366389 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 653.2,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3606378 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 653.2,
            "unit": "ns/op",
            "extra": "3606378 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3606378 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3606378 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 274.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8782141 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 274.2,
            "unit": "ns/op",
            "extra": "8782141 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8782141 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8782141 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 17.17,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "144140286 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 17.17,
            "unit": "ns/op",
            "extra": "144140286 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "144140286 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "144140286 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.096,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "632157814 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.096,
            "unit": "ns/op",
            "extra": "632157814 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "632157814 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "632157814 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 1010,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2389120 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 1010,
            "unit": "ns/op",
            "extra": "2389120 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2389120 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2389120 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 855.9,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2771428 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 855.9,
            "unit": "ns/op",
            "extra": "2771428 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2771428 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2771428 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 340.5,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6995444 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 340.5,
            "unit": "ns/op",
            "extra": "6995444 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6995444 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6995444 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 419.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5689107 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 419.6,
            "unit": "ns/op",
            "extra": "5689107 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5689107 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5689107 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 62730,
            "unit": "ns/op\t   64864 B/op\t     307 allocs/op",
            "extra": "38131 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 62730,
            "unit": "ns/op",
            "extra": "38131 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64864,
            "unit": "B/op",
            "extra": "38131 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "38131 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 16107,
            "unit": "ns/op\t   19639 B/op\t      11 allocs/op",
            "extra": "150180 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16107,
            "unit": "ns/op",
            "extra": "150180 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19639,
            "unit": "B/op",
            "extra": "150180 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "150180 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 524,
            "unit": "ns/op\t     331 B/op\t       5 allocs/op",
            "extra": "4424733 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 524,
            "unit": "ns/op",
            "extra": "4424733 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 331,
            "unit": "B/op",
            "extra": "4424733 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4424733 times\n4 procs"
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
          "id": "dec8b10a597ab3a25abd78cfcd6f3c203fea6fcc",
          "message": "fix: 对抗审计修复——row_number/lead 崩溃与静默nil、未知窗口函数报错、watermark防远未来ts毒化、函数表达式参数求值、CreateLegacyAggregator panic→nil",
          "timestamp": "2026-07-10T11:36:17+08:00",
          "tree_id": "4896a6db19c9f84cb56fcaedea55a987589bd62a",
          "url": "https://github.com/rulego/streamsql/commit/dec8b10a597ab3a25abd78cfcd6f3c203fea6fcc"
        },
        "date": 1783654732995,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 522.1,
            "unit": "ns/op\t         0 drop_rate_%\t   1915178 ops/sec\t        66.67 process_rate_%\t     270 B/op\t       2 allocs/op",
            "extra": "6780447 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 522.1,
            "unit": "ns/op",
            "extra": "6780447 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "6780447 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 1915178,
            "unit": "ops/sec",
            "extra": "6780447 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 66.67,
            "unit": "process_rate_%",
            "extra": "6780447 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 270,
            "unit": "B/op",
            "extra": "6780447 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "6780447 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 552.8,
            "unit": "ns/op\t         0 drop_rate_%\t   1809099 ops/sec\t        62.01 process_rate_%\t     235 B/op\t       2 allocs/op",
            "extra": "4392475 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 552.8,
            "unit": "ns/op",
            "extra": "4392475 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "4392475 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 1809099,
            "unit": "ops/sec",
            "extra": "4392475 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 62.01,
            "unit": "process_rate_%",
            "extra": "4392475 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 235,
            "unit": "B/op",
            "extra": "4392475 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "4392475 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 514.6,
            "unit": "ns/op\t   1942926 pure_ops/sec\t     316 B/op\t       2 allocs/op",
            "extra": "5571056 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 514.6,
            "unit": "ns/op",
            "extra": "5571056 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 1942926,
            "unit": "pure_ops/sec",
            "extra": "5571056 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 316,
            "unit": "B/op",
            "extra": "5571056 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5571056 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 526.8,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1898389 ops/sec\t        66.67 process_rate_%\t         0 result_chan_usage_%\t   3077789 results\t     269 B/op\t       2 allocs/op",
            "extra": "4616684 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 526.8,
            "unit": "ns/op",
            "extra": "4616684 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "4616684 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1898389,
            "unit": "ops/sec",
            "extra": "4616684 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 66.67,
            "unit": "process_rate_%",
            "extra": "4616684 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "4616684 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 3077789,
            "unit": "results",
            "extra": "4616684 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 269,
            "unit": "B/op",
            "extra": "4616684 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "4616684 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 221,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "10922296 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 221,
            "unit": "ns/op",
            "extra": "10922296 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "10922296 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "10922296 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 277.6,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8642192 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 277.6,
            "unit": "ns/op",
            "extra": "8642192 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8642192 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8642192 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1551,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1543587 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1551,
            "unit": "ns/op",
            "extra": "1543587 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1543587 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1543587 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 561.6,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "4315466 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 561.6,
            "unit": "ns/op",
            "extra": "4315466 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "4315466 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4315466 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 248.5,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9764235 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 248.5,
            "unit": "ns/op",
            "extra": "9764235 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9764235 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9764235 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 16.5,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "146114653 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16.5,
            "unit": "ns/op",
            "extra": "146114653 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "146114653 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "146114653 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.259,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "565298302 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.259,
            "unit": "ns/op",
            "extra": "565298302 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "565298302 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "565298302 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 854.6,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2797806 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 854.6,
            "unit": "ns/op",
            "extra": "2797806 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2797806 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2797806 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 747.7,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "3204825 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 747.7,
            "unit": "ns/op",
            "extra": "3204825 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "3204825 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3204825 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 292.5,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "8152318 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 292.5,
            "unit": "ns/op",
            "extra": "8152318 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "8152318 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "8152318 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 370.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "6444946 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 370.7,
            "unit": "ns/op",
            "extra": "6444946 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "6444946 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6444946 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 58440,
            "unit": "ns/op\t   64863 B/op\t     307 allocs/op",
            "extra": "41097 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 58440,
            "unit": "ns/op",
            "extra": "41097 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 64863,
            "unit": "B/op",
            "extra": "41097 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 307,
            "unit": "allocs/op",
            "extra": "41097 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 15587,
            "unit": "ns/op\t   19640 B/op\t      11 allocs/op",
            "extra": "148717 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 15587,
            "unit": "ns/op",
            "extra": "148717 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19640,
            "unit": "B/op",
            "extra": "148717 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "148717 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 529.4,
            "unit": "ns/op\t     332 B/op\t       5 allocs/op",
            "extra": "4540330 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 529.4,
            "unit": "ns/op",
            "extra": "4540330 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 332,
            "unit": "B/op",
            "extra": "4540330 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4540330 times\n4 procs"
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
          "id": "14887ed10b329c79d360e2a2d4c0b220431b1706",
          "message": "feat(analytic): 支持 PARTITION BY 连接字段、多调用表达式、HAVING 聚合、GROUP BY 表达式",
          "timestamp": "2026-07-12T09:28:59+08:00",
          "tree_id": "ac454e1d6ad8913bd0c5d3f9b43f253054bebdbf",
          "url": "https://github.com/rulego/streamsql/commit/14887ed10b329c79d360e2a2d4c0b220431b1706"
        },
        "date": 1783820260303,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 496.9,
            "unit": "ns/op\t         0 data_chan_usage_%\t   2012496 ops/sec\t        66.67 process_rate_%\t         0 result_chan_usage_%\t   3153566 results\t     267 B/op\t       2 allocs/op",
            "extra": "4730349 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 496.9,
            "unit": "ns/op",
            "extra": "4730349 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "4730349 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 2012496,
            "unit": "ops/sec",
            "extra": "4730349 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 66.67,
            "unit": "process_rate_%",
            "extra": "4730349 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "4730349 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 3153566,
            "unit": "results",
            "extra": "4730349 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 267,
            "unit": "B/op",
            "extra": "4730349 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "4730349 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 745.4,
            "unit": "ns/op\t        87.04 data_chan_usage_%\t   1341486 ops/sec\t        90.18 process_rate_%\t         0.8000 result_chan_usage_%\t   3997006 results\t     351 B/op\t       3 allocs/op",
            "extra": "4432192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 745.4,
            "unit": "ns/op",
            "extra": "4432192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 87.04,
            "unit": "data_chan_usage_%",
            "extra": "4432192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 1341486,
            "unit": "ops/sec",
            "extra": "4432192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 90.18,
            "unit": "process_rate_%",
            "extra": "4432192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0.8,
            "unit": "result_chan_usage_%",
            "extra": "4432192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 3997006,
            "unit": "results",
            "extra": "4432192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 351,
            "unit": "B/op",
            "extra": "4432192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "4432192 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 261.5,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9060481 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 261.5,
            "unit": "ns/op",
            "extra": "9060481 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9060481 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9060481 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 334,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7140462 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 334,
            "unit": "ns/op",
            "extra": "7140462 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7140462 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7140462 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1717,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1402326 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1717,
            "unit": "ns/op",
            "extra": "1402326 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1402326 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1402326 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 654,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3658807 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 654,
            "unit": "ns/op",
            "extra": "3658807 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3658807 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3658807 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 290.8,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8276437 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 290.8,
            "unit": "ns/op",
            "extra": "8276437 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8276437 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8276437 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 16.98,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "136025689 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16.98,
            "unit": "ns/op",
            "extra": "136025689 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "136025689 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "136025689 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.005,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "635571756 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.005,
            "unit": "ns/op",
            "extra": "635571756 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "635571756 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "635571756 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 995.9,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2425838 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 995.9,
            "unit": "ns/op",
            "extra": "2425838 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2425838 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2425838 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 850.5,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2801632 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 850.5,
            "unit": "ns/op",
            "extra": "2801632 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2801632 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2801632 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 345.6,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6742288 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 345.6,
            "unit": "ns/op",
            "extra": "6742288 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6742288 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6742288 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 428.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5562238 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 428.9,
            "unit": "ns/op",
            "extra": "5562238 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5562238 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5562238 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 63032,
            "unit": "ns/op\t   65045 B/op\t     315 allocs/op",
            "extra": "37561 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 63032,
            "unit": "ns/op",
            "extra": "37561 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 65045,
            "unit": "B/op",
            "extra": "37561 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 315,
            "unit": "allocs/op",
            "extra": "37561 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 15967,
            "unit": "ns/op\t   19650 B/op\t      11 allocs/op",
            "extra": "150825 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 15967,
            "unit": "ns/op",
            "extra": "150825 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19650,
            "unit": "B/op",
            "extra": "150825 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "150825 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 525.8,
            "unit": "ns/op\t     335 B/op\t       5 allocs/op",
            "extra": "4423903 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 525.8,
            "unit": "ns/op",
            "extra": "4423903 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 335,
            "unit": "B/op",
            "extra": "4423903 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4423903 times\n4 procs"
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
          "id": "47ef8a7d1532fedcec5f03620a9fb63c852a3ae0",
          "message": "feat(analytic): 支持 PARTITION BY 连接字段、多调用表达式、HAVING 聚合、GROUP BY 表达式",
          "timestamp": "2026-07-12T10:02:12+08:00",
          "tree_id": "3ee50f6c8e9ca0bbe82ec4399882f4ea7680ed19",
          "url": "https://github.com/rulego/streamsql/commit/47ef8a7d1532fedcec5f03620a9fb63c852a3ae0"
        },
        "date": 1783821932753,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 563.4,
            "unit": "ns/op\t   1774831 pure_ops/sec\t     315 B/op\t       2 allocs/op",
            "extra": "5381264 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 563.4,
            "unit": "ns/op",
            "extra": "5381264 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 1774831,
            "unit": "pure_ops/sec",
            "extra": "5381264 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 315,
            "unit": "B/op",
            "extra": "5381264 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5381264 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 660.9,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1513040 ops/sec\t       100.0 process_rate_%\t         0 result_chan_usage_%\t   4726573 results\t     397 B/op\t       4 allocs/op",
            "extra": "4726573 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 660.9,
            "unit": "ns/op",
            "extra": "4726573 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "4726573 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1513040,
            "unit": "ops/sec",
            "extra": "4726573 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "4726573 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "4726573 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 4726573,
            "unit": "results",
            "extra": "4726573 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 397,
            "unit": "B/op",
            "extra": "4726573 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "4726573 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 748.4,
            "unit": "ns/op\t        86.45 data_chan_usage_%\t   1336167 ops/sec\t        93.89 process_rate_%\t        10.60 result_chan_usage_%\t   6642009 results\t     367 B/op\t       3 allocs/op",
            "extra": "7074267 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 748.4,
            "unit": "ns/op",
            "extra": "7074267 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 86.45,
            "unit": "data_chan_usage_%",
            "extra": "7074267 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 1336167,
            "unit": "ops/sec",
            "extra": "7074267 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 93.89,
            "unit": "process_rate_%",
            "extra": "7074267 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 10.6,
            "unit": "result_chan_usage_%",
            "extra": "7074267 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 6642009,
            "unit": "results",
            "extra": "7074267 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 367,
            "unit": "B/op",
            "extra": "7074267 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7074267 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 259.1,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9144304 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 259.1,
            "unit": "ns/op",
            "extra": "9144304 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9144304 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9144304 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 337.6,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7132956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 337.6,
            "unit": "ns/op",
            "extra": "7132956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7132956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7132956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1729,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1384952 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1729,
            "unit": "ns/op",
            "extra": "1384952 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1384952 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1384952 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 656.4,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3656032 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 656.4,
            "unit": "ns/op",
            "extra": "3656032 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3656032 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3656032 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 292.7,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8323928 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 292.7,
            "unit": "ns/op",
            "extra": "8323928 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8323928 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8323928 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 17.46,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "142133755 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 17.46,
            "unit": "ns/op",
            "extra": "142133755 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "142133755 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "142133755 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 3.948,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "636089899 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 3.948,
            "unit": "ns/op",
            "extra": "636089899 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "636089899 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "636089899 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 990.3,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2423728 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 990.3,
            "unit": "ns/op",
            "extra": "2423728 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2423728 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2423728 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 849.8,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2778938 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 849.8,
            "unit": "ns/op",
            "extra": "2778938 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2778938 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2778938 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 341.3,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "7029582 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 341.3,
            "unit": "ns/op",
            "extra": "7029582 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "7029582 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "7029582 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 430.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5566614 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 430.1,
            "unit": "ns/op",
            "extra": "5566614 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5566614 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5566614 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 63053,
            "unit": "ns/op\t   65055 B/op\t     315 allocs/op",
            "extra": "38022 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 63053,
            "unit": "ns/op",
            "extra": "38022 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 65055,
            "unit": "B/op",
            "extra": "38022 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 315,
            "unit": "allocs/op",
            "extra": "38022 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 16200,
            "unit": "ns/op\t   19652 B/op\t      11 allocs/op",
            "extra": "149673 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16200,
            "unit": "ns/op",
            "extra": "149673 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 19652,
            "unit": "B/op",
            "extra": "149673 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "149673 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 526.5,
            "unit": "ns/op\t     333 B/op\t       5 allocs/op",
            "extra": "4582682 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 526.5,
            "unit": "ns/op",
            "extra": "4582682 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 333,
            "unit": "B/op",
            "extra": "4582682 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4582682 times\n4 procs"
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
          "id": "39fdb9abb917ee40d69137d57c792d3942f8c119",
          "message": "test(e2e): 补 event-time 多时间戳聚合回归（文档化 epoch 对齐）",
          "timestamp": "2026-07-12T23:05:03+08:00",
          "tree_id": "5f5548d186f1e1fe45eeee747e8981453d00a224",
          "url": "https://github.com/rulego/streamsql/commit/39fdb9abb917ee40d69137d57c792d3942f8c119"
        },
        "date": 1783868852108,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 670.8,
            "unit": "ns/op\t         0 drop_rate_%\t   1490795 ops/sec\t       100.0 process_rate_%\t     398 B/op\t       4 allocs/op",
            "extra": "3530242 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 670.8,
            "unit": "ns/op",
            "extra": "3530242 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "3530242 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 1490795,
            "unit": "ops/sec",
            "extra": "3530242 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "3530242 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 398,
            "unit": "B/op",
            "extra": "3530242 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "3530242 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 558.7,
            "unit": "ns/op\t   1789804 pure_ops/sec\t     315 B/op\t       2 allocs/op",
            "extra": "5436501 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 558.7,
            "unit": "ns/op",
            "extra": "5436501 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 1789804,
            "unit": "pure_ops/sec",
            "extra": "5436501 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 315,
            "unit": "B/op",
            "extra": "5436501 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5436501 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 511.9,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1953525 ops/sec\t        66.67 process_rate_%\t         0 result_chan_usage_%\t   3087593 results\t     266 B/op\t       2 allocs/op",
            "extra": "4631389 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 511.9,
            "unit": "ns/op",
            "extra": "4631389 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "4631389 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1953525,
            "unit": "ops/sec",
            "extra": "4631389 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 66.67,
            "unit": "process_rate_%",
            "extra": "4631389 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "4631389 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 3087593,
            "unit": "results",
            "extra": "4631389 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 266,
            "unit": "B/op",
            "extra": "4631389 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "4631389 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 742.6,
            "unit": "ns/op\t        86.26 data_chan_usage_%\t   1346611 ops/sec\t        91.94 process_rate_%\t        76.60 result_chan_usage_%\t   4919293 results\t     358 B/op\t       3 allocs/op",
            "extra": "5350604 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 742.6,
            "unit": "ns/op",
            "extra": "5350604 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 86.26,
            "unit": "data_chan_usage_%",
            "extra": "5350604 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 1346611,
            "unit": "ops/sec",
            "extra": "5350604 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 91.94,
            "unit": "process_rate_%",
            "extra": "5350604 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 76.6,
            "unit": "result_chan_usage_%",
            "extra": "5350604 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 4919293,
            "unit": "results",
            "extra": "5350604 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 358,
            "unit": "B/op",
            "extra": "5350604 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "5350604 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 265.7,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9010435 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 265.7,
            "unit": "ns/op",
            "extra": "9010435 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9010435 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9010435 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 327,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7281464 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 327,
            "unit": "ns/op",
            "extra": "7281464 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7281464 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7281464 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1774,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1354989 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1774,
            "unit": "ns/op",
            "extra": "1354989 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1354989 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1354989 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 698.4,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3455517 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 698.4,
            "unit": "ns/op",
            "extra": "3455517 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3455517 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3455517 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 288.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8413250 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 288.2,
            "unit": "ns/op",
            "extra": "8413250 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8413250 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8413250 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 17.13,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "141513931 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 17.13,
            "unit": "ns/op",
            "extra": "141513931 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "141513931 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "141513931 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.049,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "636672296 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.049,
            "unit": "ns/op",
            "extra": "636672296 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "636672296 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "636672296 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 993.3,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2422538 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 993.3,
            "unit": "ns/op",
            "extra": "2422538 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2422538 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2422538 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 850.1,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2752536 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 850.1,
            "unit": "ns/op",
            "extra": "2752536 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2752536 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2752536 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 346,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "7037188 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 346,
            "unit": "ns/op",
            "extra": "7037188 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "7037188 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "7037188 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 430.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5623742 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 430.1,
            "unit": "ns/op",
            "extra": "5623742 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5623742 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5623742 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 47880,
            "unit": "ns/op\t   45384 B/op\t     303 allocs/op",
            "extra": "50371 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 47880,
            "unit": "ns/op",
            "extra": "50371 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 45384,
            "unit": "B/op",
            "extra": "50371 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 303,
            "unit": "allocs/op",
            "extra": "50371 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 4.685,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "512676703 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.685,
            "unit": "ns/op",
            "extra": "512676703 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "512676703 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "512676703 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 514.9,
            "unit": "ns/op\t     335 B/op\t       5 allocs/op",
            "extra": "4423065 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 514.9,
            "unit": "ns/op",
            "extra": "4423065 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 335,
            "unit": "B/op",
            "extra": "4423065 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4423065 times\n4 procs"
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
          "id": "d26a1a4fd75daacc2fa15646eb80dfe996dd4993",
          "message": "test(cep): 补压力测试(goroutine/堆/分区 LRU/WITHIN sweeper + 吞吐 benchmark)+ 文档场景 SQL 回归",
          "timestamp": "2026-07-14T11:56:06+08:00",
          "tree_id": "6df4bbd5ad03acd64849307560da592daa6e6cc1",
          "url": "https://github.com/rulego/streamsql/commit/d26a1a4fd75daacc2fa15646eb80dfe996dd4993"
        },
        "date": 1784001669672,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 690.4,
            "unit": "ns/op\t         0 drop_rate_%\t   1448456 ops/sec\t       100.0 process_rate_%\t     399 B/op\t       4 allocs/op",
            "extra": "4641644 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 690.4,
            "unit": "ns/op",
            "extra": "4641644 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "4641644 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 1448456,
            "unit": "ops/sec",
            "extra": "4641644 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "4641644 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 399,
            "unit": "B/op",
            "extra": "4641644 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "4641644 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 743.6,
            "unit": "ns/op\t         0 drop_rate_%\t   1344871 ops/sec\t        93.22 process_rate_%\t     358 B/op\t       3 allocs/op",
            "extra": "5296452 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 743.6,
            "unit": "ns/op",
            "extra": "5296452 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "5296452 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 1344871,
            "unit": "ops/sec",
            "extra": "5296452 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 93.22,
            "unit": "process_rate_%",
            "extra": "5296452 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 358,
            "unit": "B/op",
            "extra": "5296452 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "5296452 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 561.9,
            "unit": "ns/op\t   1779423 pure_ops/sec\t     314 B/op\t       2 allocs/op",
            "extra": "5247386 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 561.9,
            "unit": "ns/op",
            "extra": "5247386 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 1779423,
            "unit": "pure_ops/sec",
            "extra": "5247386 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 314,
            "unit": "B/op",
            "extra": "5247386 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5247386 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 684.6,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1460737 ops/sec\t       100.0 process_rate_%\t         0 result_chan_usage_%\t   3595641 results\t     400 B/op\t       4 allocs/op",
            "extra": "3595641 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 684.6,
            "unit": "ns/op",
            "extra": "3595641 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "3595641 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1460737,
            "unit": "ops/sec",
            "extra": "3595641 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "3595641 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "3595641 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 3595641,
            "unit": "results",
            "extra": "3595641 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 400,
            "unit": "B/op",
            "extra": "3595641 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "3595641 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 384.5,
            "unit": "ns/op\t        69.55 data_chan_usage_%\t   2600727 ops/sec\t        31.12 process_rate_%\t         0.2000 result_chan_usage_%\t   1627527 results\t     121 B/op\t       1 allocs/op",
            "extra": "5230335 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 384.5,
            "unit": "ns/op",
            "extra": "5230335 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 69.55,
            "unit": "data_chan_usage_%",
            "extra": "5230335 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 2600727,
            "unit": "ops/sec",
            "extra": "5230335 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 31.12,
            "unit": "process_rate_%",
            "extra": "5230335 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0.2,
            "unit": "result_chan_usage_%",
            "extra": "5230335 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 1627527,
            "unit": "results",
            "extra": "5230335 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 121,
            "unit": "B/op",
            "extra": "5230335 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5230335 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 260.3,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8977520 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 260.3,
            "unit": "ns/op",
            "extra": "8977520 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8977520 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8977520 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 326.7,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7406457 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 326.7,
            "unit": "ns/op",
            "extra": "7406457 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7406457 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7406457 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1749,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1373593 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1749,
            "unit": "ns/op",
            "extra": "1373593 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1373593 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1373593 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 668.9,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3530841 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 668.9,
            "unit": "ns/op",
            "extra": "3530841 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3530841 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3530841 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 293.6,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8209365 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 293.6,
            "unit": "ns/op",
            "extra": "8209365 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8209365 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8209365 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 16.84,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "142207904 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 16.84,
            "unit": "ns/op",
            "extra": "142207904 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "142207904 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "142207904 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 3.96,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "627381898 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 3.96,
            "unit": "ns/op",
            "extra": "627381898 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "627381898 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "627381898 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 987,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2432068 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 987,
            "unit": "ns/op",
            "extra": "2432068 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2432068 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2432068 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 838.1,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2818258 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 838.1,
            "unit": "ns/op",
            "extra": "2818258 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2818258 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2818258 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 338.7,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "7073828 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 338.7,
            "unit": "ns/op",
            "extra": "7073828 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "7073828 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "7073828 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 424.3,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5659270 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 424.3,
            "unit": "ns/op",
            "extra": "5659270 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5659270 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5659270 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 47391,
            "unit": "ns/op\t   45405 B/op\t     303 allocs/op",
            "extra": "50550 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 47391,
            "unit": "ns/op",
            "extra": "50550 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 45405,
            "unit": "B/op",
            "extra": "50550 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 303,
            "unit": "allocs/op",
            "extra": "50550 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 4.671,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "512829372 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.671,
            "unit": "ns/op",
            "extra": "512829372 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "512829372 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "512829372 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 533.3,
            "unit": "ns/op\t     332 B/op\t       5 allocs/op",
            "extra": "4657520 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 533.3,
            "unit": "ns/op",
            "extra": "4657520 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 332,
            "unit": "B/op",
            "extra": "4657520 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4657520 times\n4 procs"
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
          "id": "926273d1df5e1e57d20c8cce6d6dcf7034cca913",
          "message": "fix(test/cep): 压测 drain 改背压，避免 dataChan 满丢弃导致超时",
          "timestamp": "2026-07-14T12:12:27+08:00",
          "tree_id": "f031bd616e958dab4c242b12e33e97c645c56ba2",
          "url": "https://github.com/rulego/streamsql/commit/926273d1df5e1e57d20c8cce6d6dcf7034cca913"
        },
        "date": 1784002470728,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 600.9,
            "unit": "ns/op\t         0 drop_rate_%\t   1664042 ops/sec\t       100.0 process_rate_%\t     413 B/op\t       4 allocs/op",
            "extra": "4001349 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 600.9,
            "unit": "ns/op",
            "extra": "4001349 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "4001349 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 1664042,
            "unit": "ops/sec",
            "extra": "4001349 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "4001349 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 413,
            "unit": "B/op",
            "extra": "4001349 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "4001349 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 629.2,
            "unit": "ns/op\t         0 drop_rate_%\t   1589338 ops/sec\t        94.66 process_rate_%\t     363 B/op\t       3 allocs/op",
            "extra": "6127960 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 629.2,
            "unit": "ns/op",
            "extra": "6127960 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "6127960 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 1589338,
            "unit": "ops/sec",
            "extra": "6127960 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 94.66,
            "unit": "process_rate_%",
            "extra": "6127960 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 363,
            "unit": "B/op",
            "extra": "6127960 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "6127960 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 453.2,
            "unit": "ns/op\t   2206474 pure_ops/sec\t     319 B/op\t       2 allocs/op",
            "extra": "6437450 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 453.2,
            "unit": "ns/op",
            "extra": "6437450 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 2206474,
            "unit": "pure_ops/sec",
            "extra": "6437450 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 319,
            "unit": "B/op",
            "extra": "6437450 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "6437450 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 634.6,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1575859 ops/sec\t       100.0 process_rate_%\t         0 result_chan_usage_%\t   5092952 results\t     418 B/op\t       4 allocs/op",
            "extra": "5092952 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 634.6,
            "unit": "ns/op",
            "extra": "5092952 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "5092952 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1575859,
            "unit": "ops/sec",
            "extra": "5092952 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "5092952 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "5092952 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 5092952,
            "unit": "results",
            "extra": "5092952 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 418,
            "unit": "B/op",
            "extra": "5092952 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "5092952 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 472.1,
            "unit": "ns/op\t        77.21 data_chan_usage_%\t   2118350 ops/sec\t        61.72 process_rate_%\t         1.800 result_chan_usage_%\t   3207727 results\t     239 B/op\t       2 allocs/op",
            "extra": "5197640 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 472.1,
            "unit": "ns/op",
            "extra": "5197640 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 77.21,
            "unit": "data_chan_usage_%",
            "extra": "5197640 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 2118350,
            "unit": "ops/sec",
            "extra": "5197640 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 61.72,
            "unit": "process_rate_%",
            "extra": "5197640 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 1.8,
            "unit": "result_chan_usage_%",
            "extra": "5197640 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 3207727,
            "unit": "results",
            "extra": "5197640 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 239,
            "unit": "B/op",
            "extra": "5197640 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5197640 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 177.4,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "13356226 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 177.4,
            "unit": "ns/op",
            "extra": "13356226 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "13356226 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "13356226 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 214.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "11227572 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 214.2,
            "unit": "ns/op",
            "extra": "11227572 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "11227572 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "11227572 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1111,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "2159707 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1111,
            "unit": "ns/op",
            "extra": "2159707 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "2159707 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "2159707 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 440.3,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "5483332 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 440.3,
            "unit": "ns/op",
            "extra": "5483332 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "5483332 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "5483332 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 192.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "12357949 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 192.2,
            "unit": "ns/op",
            "extra": "12357949 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "12357949 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "12357949 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 9.638,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "248648676 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 9.638,
            "unit": "ns/op",
            "extra": "248648676 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "248648676 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "248648676 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.044,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "583468496 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.044,
            "unit": "ns/op",
            "extra": "583468496 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "583468496 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "583468496 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 605.1,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "3923671 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 605.1,
            "unit": "ns/op",
            "extra": "3923671 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "3923671 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "3923671 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 537.1,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "4474341 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 537.1,
            "unit": "ns/op",
            "extra": "4474341 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "4474341 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4474341 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 225.6,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "10561689 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 225.6,
            "unit": "ns/op",
            "extra": "10561689 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "10561689 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "10561689 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 286.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "8970268 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 286.6,
            "unit": "ns/op",
            "extra": "8970268 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "8970268 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "8970268 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 37623,
            "unit": "ns/op\t   45396 B/op\t     303 allocs/op",
            "extra": "63360 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 37623,
            "unit": "ns/op",
            "extra": "63360 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 45396,
            "unit": "B/op",
            "extra": "63360 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 303,
            "unit": "allocs/op",
            "extra": "63360 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 11.83,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "202080909 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 11.83,
            "unit": "ns/op",
            "extra": "202080909 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "202080909 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "202080909 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 331.5,
            "unit": "ns/op\t     340 B/op\t       5 allocs/op",
            "extra": "7084051 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 331.5,
            "unit": "ns/op",
            "extra": "7084051 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 340,
            "unit": "B/op",
            "extra": "7084051 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "7084051 times\n4 procs"
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
          "id": "41fa5c7ffd58c507f755682cfd708c75e90f8967",
          "message": "docs(readme): 新增 CEP 模式识别特色(轻量边缘引擎独有,eKuiper 无)",
          "timestamp": "2026-07-14T12:21:55+08:00",
          "tree_id": "2b300dda3c40ea740fe0a3eb8ac32621a1f9ea96",
          "url": "https://github.com/rulego/streamsql/commit/41fa5c7ffd58c507f755682cfd708c75e90f8967"
        },
        "date": 1784003064738,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 673.8,
            "unit": "ns/op\t         0 drop_rate_%\t   1484031 ops/sec\t       100.0 process_rate_%\t     399 B/op\t       4 allocs/op",
            "extra": "4699154 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 673.8,
            "unit": "ns/op",
            "extra": "4699154 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "4699154 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 1484031,
            "unit": "ops/sec",
            "extra": "4699154 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "4699154 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 399,
            "unit": "B/op",
            "extra": "4699154 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "4699154 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 565.1,
            "unit": "ns/op\t   1769580 pure_ops/sec\t     314 B/op\t       2 allocs/op",
            "extra": "5180431 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 565.1,
            "unit": "ns/op",
            "extra": "5180431 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 1769580,
            "unit": "pure_ops/sec",
            "extra": "5180431 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 314,
            "unit": "B/op",
            "extra": "5180431 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5180431 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 752.1,
            "unit": "ns/op\t        86.93 data_chan_usage_%\t   1329580 ops/sec\t        91.64 process_rate_%\t         2.400 result_chan_usage_%\t   4762897 results\t     358 B/op\t       3 allocs/op",
            "extra": "5197555 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 752.1,
            "unit": "ns/op",
            "extra": "5197555 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 86.93,
            "unit": "data_chan_usage_%",
            "extra": "5197555 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 1329580,
            "unit": "ops/sec",
            "extra": "5197555 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 91.64,
            "unit": "process_rate_%",
            "extra": "5197555 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 2.4,
            "unit": "result_chan_usage_%",
            "extra": "5197555 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 4762897,
            "unit": "results",
            "extra": "5197555 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 358,
            "unit": "B/op",
            "extra": "5197555 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "5197555 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 260,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9156062 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 260,
            "unit": "ns/op",
            "extra": "9156062 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9156062 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9156062 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 327.3,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7137086 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 327.3,
            "unit": "ns/op",
            "extra": "7137086 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7137086 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7137086 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1766,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1360612 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1766,
            "unit": "ns/op",
            "extra": "1360612 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1360612 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1360612 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 676.8,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3561373 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 676.8,
            "unit": "ns/op",
            "extra": "3561373 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3561373 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3561373 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 292,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8145747 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 292,
            "unit": "ns/op",
            "extra": "8145747 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8145747 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8145747 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 17.19,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "139791979 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 17.19,
            "unit": "ns/op",
            "extra": "139791979 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "139791979 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "139791979 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.182,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "634749882 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.182,
            "unit": "ns/op",
            "extra": "634749882 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "634749882 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "634749882 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 1002,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2414462 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 1002,
            "unit": "ns/op",
            "extra": "2414462 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2414462 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2414462 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 849.4,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2789804 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 849.4,
            "unit": "ns/op",
            "extra": "2789804 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2789804 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2789804 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 346.2,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6932248 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 346.2,
            "unit": "ns/op",
            "extra": "6932248 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6932248 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6932248 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 431.3,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5498158 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 431.3,
            "unit": "ns/op",
            "extra": "5498158 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5498158 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5498158 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 46643,
            "unit": "ns/op\t   45367 B/op\t     303 allocs/op",
            "extra": "51513 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 46643,
            "unit": "ns/op",
            "extra": "51513 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 45367,
            "unit": "B/op",
            "extra": "51513 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 303,
            "unit": "allocs/op",
            "extra": "51513 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 4.671,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "513662680 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.671,
            "unit": "ns/op",
            "extra": "513662680 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "513662680 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "513662680 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 528,
            "unit": "ns/op\t     333 B/op\t       5 allocs/op",
            "extra": "4397620 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 528,
            "unit": "ns/op",
            "extra": "4397620 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 333,
            "unit": "B/op",
            "extra": "4397620 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4397620 times\n4 procs"
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
          "id": "4aaba019739f522ea6d09c1ba342722088d5b6ce",
          "message": "docs(readme): 新增 CEP 模式识别特色(轻量边缘引擎独有)",
          "timestamp": "2026-07-14T12:30:08+08:00",
          "tree_id": "8a9eed515dfd6e3426d432f84a10c4b4dd185414",
          "url": "https://github.com/rulego/streamsql/commit/4aaba019739f522ea6d09c1ba342722088d5b6ce"
        },
        "date": 1784003557827,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 518.5,
            "unit": "ns/op\t         0 drop_rate_%\t   1928664 ops/sec\t        66.67 process_rate_%\t     267 B/op\t       2 allocs/op",
            "extra": "4686806 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 518.5,
            "unit": "ns/op",
            "extra": "4686806 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "4686806 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 1928664,
            "unit": "ops/sec",
            "extra": "4686806 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 66.67,
            "unit": "process_rate_%",
            "extra": "4686806 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 267,
            "unit": "B/op",
            "extra": "4686806 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "4686806 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 739.8,
            "unit": "ns/op\t         0 drop_rate_%\t   1351700 ops/sec\t        91.57 process_rate_%\t     350 B/op\t       3 allocs/op",
            "extra": "4289616 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 739.8,
            "unit": "ns/op",
            "extra": "4289616 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "4289616 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 1351700,
            "unit": "ops/sec",
            "extra": "4289616 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 91.57,
            "unit": "process_rate_%",
            "extra": "4289616 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 350,
            "unit": "B/op",
            "extra": "4289616 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "4289616 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 571.1,
            "unit": "ns/op\t   1750916 pure_ops/sec\t     315 B/op\t       2 allocs/op",
            "extra": "5343770 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 571.1,
            "unit": "ns/op",
            "extra": "5343770 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 1750916,
            "unit": "pure_ops/sec",
            "extra": "5343770 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 315,
            "unit": "B/op",
            "extra": "5343770 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5343770 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 584.6,
            "unit": "ns/op\t        81.82 data_chan_usage_%\t   1710574 ops/sec\t        61.47 process_rate_%\t         1.400 result_chan_usage_%\t   3225144 results\t     239 B/op\t       2 allocs/op",
            "extra": "5246805 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 584.6,
            "unit": "ns/op",
            "extra": "5246805 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 81.82,
            "unit": "data_chan_usage_%",
            "extra": "5246805 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 1710574,
            "unit": "ops/sec",
            "extra": "5246805 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 61.47,
            "unit": "process_rate_%",
            "extra": "5246805 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 1.4,
            "unit": "result_chan_usage_%",
            "extra": "5246805 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 3225144,
            "unit": "results",
            "extra": "5246805 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 239,
            "unit": "B/op",
            "extra": "5246805 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5246805 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 266.4,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9101190 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 266.4,
            "unit": "ns/op",
            "extra": "9101190 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9101190 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9101190 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 329.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7097563 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 329.2,
            "unit": "ns/op",
            "extra": "7097563 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7097563 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7097563 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1800,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1311568 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1800,
            "unit": "ns/op",
            "extra": "1311568 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1311568 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1311568 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 688.6,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3486956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 688.6,
            "unit": "ns/op",
            "extra": "3486956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3486956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3486956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 296.4,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7994019 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 296.4,
            "unit": "ns/op",
            "extra": "7994019 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7994019 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7994019 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 17.12,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "139680154 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 17.12,
            "unit": "ns/op",
            "extra": "139680154 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "139680154 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "139680154 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.098,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "635597631 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.098,
            "unit": "ns/op",
            "extra": "635597631 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "635597631 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "635597631 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 1008,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2355000 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 1008,
            "unit": "ns/op",
            "extra": "2355000 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2355000 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2355000 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 867.1,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2650839 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 867.1,
            "unit": "ns/op",
            "extra": "2650839 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2650839 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2650839 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 350.6,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6741109 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 350.6,
            "unit": "ns/op",
            "extra": "6741109 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6741109 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6741109 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 438.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5433590 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 438.8,
            "unit": "ns/op",
            "extra": "5433590 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5433590 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5433590 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 48829,
            "unit": "ns/op\t   45401 B/op\t     303 allocs/op",
            "extra": "49461 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 48829,
            "unit": "ns/op",
            "extra": "49461 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 45401,
            "unit": "B/op",
            "extra": "49461 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 303,
            "unit": "allocs/op",
            "extra": "49461 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 4.723,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "513412893 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.723,
            "unit": "ns/op",
            "extra": "513412893 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "513412893 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "513412893 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 546.4,
            "unit": "ns/op\t     331 B/op\t       5 allocs/op",
            "extra": "4474107 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 546.4,
            "unit": "ns/op",
            "extra": "4474107 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 331,
            "unit": "B/op",
            "extra": "4474107 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4474107 times\n4 procs"
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
          "id": "ce3f7e1138acd65bb3aac1d88e5aedbbf6dd1b0f",
          "message": "chore: 优化代码注释",
          "timestamp": "2026-07-14T12:42:09+08:00",
          "tree_id": "aa79ca58170c21f1cdd0bb8695bd63759a7d0d9f",
          "url": "https://github.com/rulego/streamsql/commit/ce3f7e1138acd65bb3aac1d88e5aedbbf6dd1b0f"
        },
        "date": 1784004276500,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 704.9,
            "unit": "ns/op\t         0 drop_rate_%\t   1418627 ops/sec\t       100.0 process_rate_%\t     399 B/op\t       4 allocs/op",
            "extra": "4245672 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 704.9,
            "unit": "ns/op",
            "extra": "4245672 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "4245672 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 1418627,
            "unit": "ops/sec",
            "extra": "4245672 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "4245672 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 399,
            "unit": "B/op",
            "extra": "4245672 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "4245672 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 529.9,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1887185 ops/sec\t        66.67 process_rate_%\t         0 result_chan_usage_%\t   4479958 results\t     267 B/op\t       2 allocs/op",
            "extra": "6719936 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 529.9,
            "unit": "ns/op",
            "extra": "6719936 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "6719936 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1887185,
            "unit": "ops/sec",
            "extra": "6719936 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 66.67,
            "unit": "process_rate_%",
            "extra": "6719936 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "6719936 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 4479958,
            "unit": "results",
            "extra": "6719936 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 267,
            "unit": "B/op",
            "extra": "6719936 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "6719936 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 263.6,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9162921 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 263.6,
            "unit": "ns/op",
            "extra": "9162921 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9162921 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9162921 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 327.1,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7374936 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 327.1,
            "unit": "ns/op",
            "extra": "7374936 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7374936 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7374936 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1766,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1358930 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1766,
            "unit": "ns/op",
            "extra": "1358930 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1358930 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1358930 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 671,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3562029 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 671,
            "unit": "ns/op",
            "extra": "3562029 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3562029 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3562029 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 291.6,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8097289 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 291.6,
            "unit": "ns/op",
            "extra": "8097289 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8097289 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8097289 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 17.07,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "134428419 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 17.07,
            "unit": "ns/op",
            "extra": "134428419 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "134428419 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "134428419 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.003,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "636041222 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.003,
            "unit": "ns/op",
            "extra": "636041222 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "636041222 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "636041222 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 1004,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2393260 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 1004,
            "unit": "ns/op",
            "extra": "2393260 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2393260 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2393260 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 865.6,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2783875 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 865.6,
            "unit": "ns/op",
            "extra": "2783875 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2783875 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2783875 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 342.7,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6979252 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 342.7,
            "unit": "ns/op",
            "extra": "6979252 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6979252 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6979252 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 434,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5546108 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 434,
            "unit": "ns/op",
            "extra": "5546108 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5546108 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5546108 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 48245,
            "unit": "ns/op\t   45396 B/op\t     303 allocs/op",
            "extra": "48518 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 48245,
            "unit": "ns/op",
            "extra": "48518 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 45396,
            "unit": "B/op",
            "extra": "48518 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 303,
            "unit": "allocs/op",
            "extra": "48518 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 4.679,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "511379576 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.679,
            "unit": "ns/op",
            "extra": "511379576 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "511379576 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "511379576 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 533.5,
            "unit": "ns/op\t     336 B/op\t       5 allocs/op",
            "extra": "4467962 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 533.5,
            "unit": "ns/op",
            "extra": "4467962 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 336,
            "unit": "B/op",
            "extra": "4467962 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4467962 times\n4 procs"
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
          "id": "a323fe7a732c6ad24736c66f884cf317d909cecf",
          "message": "docs(readme): 重写中英文 README，突出 CEP/分析函数与性能基准",
          "timestamp": "2026-07-14T14:12:38+08:00",
          "tree_id": "9cdb26d22e4604e7f9546e28f1a2d531c108b3b0",
          "url": "https://github.com/rulego/streamsql/commit/a323fe7a732c6ad24736c66f884cf317d909cecf"
        },
        "date": 1784010053181,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkStreamSQL/SimpleFilter (github.com/rulego/streamsql)",
            "value": 632.8,
            "unit": "ns/op\t         0 drop_rate_%\t   1580220 ops/sec\t        60.00 process_rate_%\t   2349172 results\t     257 B/op\t       2 allocs/op",
            "extra": "3915288 times\n4 procs"
          },
          {
            "name": "BenchmarkStreamSQL/SimpleFilter (github.com/rulego/streamsql) - ns/op",
            "value": 632.8,
            "unit": "ns/op",
            "extra": "3915288 times\n4 procs"
          },
          {
            "name": "BenchmarkStreamSQL/SimpleFilter (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "3915288 times\n4 procs"
          },
          {
            "name": "BenchmarkStreamSQL/SimpleFilter (github.com/rulego/streamsql) - ops/sec",
            "value": 1580220,
            "unit": "ops/sec",
            "extra": "3915288 times\n4 procs"
          },
          {
            "name": "BenchmarkStreamSQL/SimpleFilter (github.com/rulego/streamsql) - process_rate_%",
            "value": 60,
            "unit": "process_rate_%",
            "extra": "3915288 times\n4 procs"
          },
          {
            "name": "BenchmarkStreamSQL/SimpleFilter (github.com/rulego/streamsql) - results",
            "value": 2349172,
            "unit": "results",
            "extra": "3915288 times\n4 procs"
          },
          {
            "name": "BenchmarkStreamSQL/SimpleFilter (github.com/rulego/streamsql) - B/op",
            "value": 257,
            "unit": "B/op",
            "extra": "3915288 times\n4 procs"
          },
          {
            "name": "BenchmarkStreamSQL/SimpleFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "3915288 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 904.3,
            "unit": "ns/op\t         0 drop_rate_%\t   1105790 ops/sec\t       100.0 process_rate_%\t     422 B/op\t       4 allocs/op",
            "extra": "3405620 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 904.3,
            "unit": "ns/op",
            "extra": "3405620 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "3405620 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 1105790,
            "unit": "ops/sec",
            "extra": "3405620 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "3405620 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 422,
            "unit": "B/op",
            "extra": "3405620 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "3405620 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 645.4,
            "unit": "ns/op\t   1549393 pure_ops/sec\t     310 B/op\t       2 allocs/op",
            "extra": "4581601 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 645.4,
            "unit": "ns/op",
            "extra": "4581601 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 1549393,
            "unit": "pure_ops/sec",
            "extra": "4581601 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 310,
            "unit": "B/op",
            "extra": "4581601 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "4581601 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 897.8,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1113882 ops/sec\t       100.0 process_rate_%\t         0 result_chan_usage_%\t   3629192 results\t     425 B/op\t       4 allocs/op",
            "extra": "3629192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 897.8,
            "unit": "ns/op",
            "extra": "3629192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "3629192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1113882,
            "unit": "ops/sec",
            "extra": "3629192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "3629192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "3629192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 3629192,
            "unit": "results",
            "extra": "3629192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 425,
            "unit": "B/op",
            "extra": "3629192 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "3629192 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 262,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9204337 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 262,
            "unit": "ns/op",
            "extra": "9204337 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9204337 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9204337 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 324.7,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7690714 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 324.7,
            "unit": "ns/op",
            "extra": "7690714 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7690714 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7690714 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1644,
            "unit": "ns/op\t     460 B/op\t      11 allocs/op",
            "extra": "1455153 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1644,
            "unit": "ns/op",
            "extra": "1455153 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 460,
            "unit": "B/op",
            "extra": "1455153 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1455153 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 652.9,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3654876 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 652.9,
            "unit": "ns/op",
            "extra": "3654876 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3654876 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3654876 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 281.6,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8758561 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 281.6,
            "unit": "ns/op",
            "extra": "8758561 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8758561 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8758561 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 15.3,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "156266622 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 15.3,
            "unit": "ns/op",
            "extra": "156266622 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "156266622 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "156266622 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 158.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "732088408 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 158.5,
            "unit": "ns/op",
            "extra": "732088408 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "732088408 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "732088408 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 913.8,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2569293 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 913.8,
            "unit": "ns/op",
            "extra": "2569293 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2569293 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2569293 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 824.7,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2906481 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 824.7,
            "unit": "ns/op",
            "extra": "2906481 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2906481 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2906481 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 377.9,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6251120 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 377.9,
            "unit": "ns/op",
            "extra": "6251120 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6251120 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6251120 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 415.3,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5826684 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 415.3,
            "unit": "ns/op",
            "extra": "5826684 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5826684 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5826684 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 47455,
            "unit": "ns/op\t   45411 B/op\t     303 allocs/op",
            "extra": "46773 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 47455,
            "unit": "ns/op",
            "extra": "46773 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 45411,
            "unit": "B/op",
            "extra": "46773 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 303,
            "unit": "allocs/op",
            "extra": "46773 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 14.72,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "163050505 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 14.72,
            "unit": "ns/op",
            "extra": "163050505 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "163050505 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "163050505 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 488.7,
            "unit": "ns/op\t     331 B/op\t       5 allocs/op",
            "extra": "4741989 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 488.7,
            "unit": "ns/op",
            "extra": "4741989 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 331,
            "unit": "B/op",
            "extra": "4741989 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4741989 times\n4 procs"
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
          "id": "9f10a18cb8434eb60e495758e319ded6db198afb",
          "message": "fix: datetime函数支持time.Time入参/JOIN键数值归一/GROUP BY键保留类型\n\n- datetime 13个函数(year/month/date_add等)补 time.Time 入参：now() 返回\n  time.Time 后，在 WHERE 中静默丢全部行、SELECT 字段变 nil\n- JOIN encodeOne 归一数值类型：int(1)==float64(1)，修 JSON 流与类型化维度表\n  INNER JOIN 静默丢行\n- GROUP BY 键保留原始类型输出(不再恒为string)，分隔符改 \\x1f 防 | 碰撞",
          "timestamp": "2026-07-22T17:03:56+08:00",
          "tree_id": "cf071cdd314b5d60eb93b96c2f8b94b818214eab",
          "url": "https://github.com/rulego/streamsql/commit/9f10a18cb8434eb60e495758e319ded6db198afb"
        },
        "date": 1784712357149,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 748.2,
            "unit": "ns/op\t         0 drop_rate_%\t   1336508 ops/sec\t       100.0 process_rate_%\t     402 B/op\t       4 allocs/op",
            "extra": "6561355 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 748.2,
            "unit": "ns/op",
            "extra": "6561355 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "6561355 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 1336508,
            "unit": "ops/sec",
            "extra": "6561355 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "6561355 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 402,
            "unit": "B/op",
            "extra": "6561355 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "6561355 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 593.7,
            "unit": "ns/op\t         0 drop_rate_%\t   1684225 ops/sec\t        63.39 process_rate_%\t     244 B/op\t       2 allocs/op",
            "extra": "6678727 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 593.7,
            "unit": "ns/op",
            "extra": "6678727 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "6678727 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 1684225,
            "unit": "ops/sec",
            "extra": "6678727 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 63.39,
            "unit": "process_rate_%",
            "extra": "6678727 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 244,
            "unit": "B/op",
            "extra": "6678727 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "6678727 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 581.3,
            "unit": "ns/op\t   1720187 pure_ops/sec\t     314 B/op\t       2 allocs/op",
            "extra": "5265352 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 581.3,
            "unit": "ns/op",
            "extra": "5265352 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 1720187,
            "unit": "pure_ops/sec",
            "extra": "5265352 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 314,
            "unit": "B/op",
            "extra": "5265352 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5265352 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 685.8,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1458124 ops/sec\t       100.0 process_rate_%\t         0 result_chan_usage_%\t   6760182 results\t     400 B/op\t       4 allocs/op",
            "extra": "6760182 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 685.8,
            "unit": "ns/op",
            "extra": "6760182 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "6760182 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1458124,
            "unit": "ops/sec",
            "extra": "6760182 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "6760182 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "6760182 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 6760182,
            "unit": "results",
            "extra": "6760182 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 400,
            "unit": "B/op",
            "extra": "6760182 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "6760182 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 388.8,
            "unit": "ns/op\t        70.75 data_chan_usage_%\t   2572089 ops/sec\t        31.09 process_rate_%\t         0.4000 result_chan_usage_%\t   1634303 results\t     121 B/op\t       1 allocs/op",
            "extra": "5256654 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 388.8,
            "unit": "ns/op",
            "extra": "5256654 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 70.75,
            "unit": "data_chan_usage_%",
            "extra": "5256654 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 2572089,
            "unit": "ops/sec",
            "extra": "5256654 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 31.09,
            "unit": "process_rate_%",
            "extra": "5256654 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0.4,
            "unit": "result_chan_usage_%",
            "extra": "5256654 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 1634303,
            "unit": "results",
            "extra": "5256654 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 121,
            "unit": "B/op",
            "extra": "5256654 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5256654 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 258.3,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9380756 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 258.3,
            "unit": "ns/op",
            "extra": "9380756 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9380756 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9380756 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 323.4,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7381232 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 323.4,
            "unit": "ns/op",
            "extra": "7381232 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7381232 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7381232 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1752,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1363804 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1752,
            "unit": "ns/op",
            "extra": "1363804 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1363804 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1363804 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 656.2,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3643644 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 656.2,
            "unit": "ns/op",
            "extra": "3643644 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3643644 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3643644 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 286.4,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8291493 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 286.4,
            "unit": "ns/op",
            "extra": "8291493 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8291493 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8291493 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 17.52,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "141047600 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 17.52,
            "unit": "ns/op",
            "extra": "141047600 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "141047600 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "141047600 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.059,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "634299958 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.059,
            "unit": "ns/op",
            "extra": "634299958 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "634299958 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "634299958 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 991,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2436196 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 991,
            "unit": "ns/op",
            "extra": "2436196 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2436196 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2436196 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 877.5,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2728879 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 877.5,
            "unit": "ns/op",
            "extra": "2728879 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2728879 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2728879 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 344.8,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6978417 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 344.8,
            "unit": "ns/op",
            "extra": "6978417 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6978417 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6978417 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 431.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5521016 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 431.6,
            "unit": "ns/op",
            "extra": "5521016 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5521016 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5521016 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 47364,
            "unit": "ns/op\t   45416 B/op\t     303 allocs/op",
            "extra": "50719 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 47364,
            "unit": "ns/op",
            "extra": "50719 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 45416,
            "unit": "B/op",
            "extra": "50719 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 303,
            "unit": "allocs/op",
            "extra": "50719 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 4.681,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "512337242 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.681,
            "unit": "ns/op",
            "extra": "512337242 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "512337242 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "512337242 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 521.6,
            "unit": "ns/op\t     333 B/op\t       5 allocs/op",
            "extra": "4587810 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 521.6,
            "unit": "ns/op",
            "extra": "4587810 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 333,
            "unit": "B/op",
            "extra": "4587810 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4587810 times\n4 procs"
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
          "id": "9f97b72ffdfe6c108fa55c92f2469f8cb4c072e7",
          "message": "fix(window/aggregator/stream): 迟到重复计算/触发竞态/聚合cast中断/JOIN panic崩溃\n\n- W1: tumbling/sliding 迟到更新重复计算——snapshot 滚动合并后迟到行仍留在 data 被反复重读\n  (tumbling 改驱逐、sliding 改按 Data 指针去重)\n- W2: tumbling 触发回调前先登记 triggeredWindows 并推进 currentSlot，避免释放锁期间\n  并发 Add 把迟到行孤立到已触发窗口\n- A5: 数值聚合 cast 失败由 return 改 continue，只跳该字段(原中断整行 Add 致同行其它\n  字段漏算、跨字段口径不一致)\n- J2: direct/window Process 主循环加 recover，用户 TableSource.Lookup panic 不再崩整进程\n  (与 CEP 路径对齐)",
          "timestamp": "2026-07-22T18:53:19+08:00",
          "tree_id": "22b9e298a97cd9641a3798f7e19dae105c71a188",
          "url": "https://github.com/rulego/streamsql/commit/9f97b72ffdfe6c108fa55c92f2469f8cb4c072e7"
        },
        "date": 1784717782292,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql)",
            "value": 739.3,
            "unit": "ns/op\t         0 drop_rate_%\t   1352617 ops/sec\t       100.0 process_rate_%\t     401 B/op\t       4 allocs/op",
            "extra": "6321118 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ns/op",
            "value": 739.3,
            "unit": "ns/op",
            "extra": "6321118 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "6321118 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - ops/sec",
            "value": 1352617,
            "unit": "ops/sec",
            "extra": "6321118 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "6321118 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - B/op",
            "value": 401,
            "unit": "B/op",
            "extra": "6321118 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/Lightweight (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "6321118 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 599.8,
            "unit": "ns/op\t   1667301 pure_ops/sec\t     313 B/op\t       2 allocs/op",
            "extra": "5096194 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 599.8,
            "unit": "ns/op",
            "extra": "5096194 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 1667301,
            "unit": "pure_ops/sec",
            "extra": "5096194 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 313,
            "unit": "B/op",
            "extra": "5096194 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5096194 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 751,
            "unit": "ns/op\t         0 data_chan_usage_%\t   1331552 ops/sec\t       100.0 process_rate_%\t         0 result_chan_usage_%\t   4279552 results\t     402 B/op\t       4 allocs/op",
            "extra": "4279552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 751,
            "unit": "ns/op",
            "extra": "4279552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "4279552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 1331552,
            "unit": "ops/sec",
            "extra": "4279552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 100,
            "unit": "process_rate_%",
            "extra": "4279552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "4279552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 4279552,
            "unit": "results",
            "extra": "4279552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 402,
            "unit": "B/op",
            "extra": "4279552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "4279552 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql)",
            "value": 785.2,
            "unit": "ns/op\t        87.33 data_chan_usage_%\t   1273485 ops/sec\t        89.68 process_rate_%\t         2.400 result_chan_usage_%\t   3795707 results\t     350 B/op\t       3 allocs/op",
            "extra": "4232353 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ns/op",
            "value": 785.2,
            "unit": "ns/op",
            "extra": "4232353 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 87.33,
            "unit": "data_chan_usage_%",
            "extra": "4232353 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - ops/sec",
            "value": 1273485,
            "unit": "ops/sec",
            "extra": "4232353 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - process_rate_%",
            "value": 89.68,
            "unit": "process_rate_%",
            "extra": "4232353 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 2.4,
            "unit": "result_chan_usage_%",
            "extra": "4232353 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - results",
            "value": 3795707,
            "unit": "results",
            "extra": "4232353 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - B/op",
            "value": 350,
            "unit": "B/op",
            "extra": "4232353 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/HighPerf50K (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "4232353 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 259.2,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9189120 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 259.2,
            "unit": "ns/op",
            "extra": "9189120 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9189120 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9189120 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 324.5,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7335916 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 324.5,
            "unit": "ns/op",
            "extra": "7335916 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7335916 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7335916 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1771,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1350147 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1771,
            "unit": "ns/op",
            "extra": "1350147 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1350147 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1350147 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 683.3,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3515154 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 683.3,
            "unit": "ns/op",
            "extra": "3515154 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3515154 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3515154 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 283.7,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8492840 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 283.7,
            "unit": "ns/op",
            "extra": "8492840 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8492840 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8492840 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 17.11,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "141165451 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 17.11,
            "unit": "ns/op",
            "extra": "141165451 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "141165451 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "141165451 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.132,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "632286128 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.132,
            "unit": "ns/op",
            "extra": "632286128 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "632286128 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "632286128 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 988.7,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2419680 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 988.7,
            "unit": "ns/op",
            "extra": "2419680 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2419680 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2419680 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 869.7,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2753140 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 869.7,
            "unit": "ns/op",
            "extra": "2753140 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2753140 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2753140 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 342,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6988316 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 342,
            "unit": "ns/op",
            "extra": "6988316 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6988316 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6988316 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 432.5,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5550679 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 432.5,
            "unit": "ns/op",
            "extra": "5550679 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5550679 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5550679 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 46907,
            "unit": "ns/op\t   45393 B/op\t     303 allocs/op",
            "extra": "51240 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 46907,
            "unit": "ns/op",
            "extra": "51240 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 45393,
            "unit": "B/op",
            "extra": "51240 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 303,
            "unit": "allocs/op",
            "extra": "51240 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 4.679,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "513307471 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.679,
            "unit": "ns/op",
            "extra": "513307471 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "513307471 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "513307471 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 528.4,
            "unit": "ns/op\t     334 B/op\t       5 allocs/op",
            "extra": "4427794 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 528.4,
            "unit": "ns/op",
            "extra": "4427794 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 334,
            "unit": "B/op",
            "extra": "4427794 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4427794 times\n4 procs"
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
          "id": "192b2e5b06c18f94c4794025d51c789a92913f6a",
          "message": "fix(window/aggregator/stream): 迟到重复计算/触发竞态/聚合cast中断/JOIN panic崩溃\n\n- tumbling/sliding 迟到更新重复计算：snapshot 滚动合并后迟到行仍留在 data 被反复重读\n  (tumbling 改驱逐、sliding 改按 Data 指针去重)\n- tumbling 触发回调前先登记 triggeredWindows 并推进 currentSlot，避免释放锁期间\n  并发 Add 把迟到行孤立到已触发窗口\n- 数值聚合 cast 失败由 return 改 continue，只跳该字段(原中断整行 Add 致同行其它\n  字段漏算、跨字段口径不一致)\n- direct/window Process 主循环加 recover，用户 TableSource.Lookup panic 不再崩整进程\n  (与 CEP 路径对齐)",
          "timestamp": "2026-07-23T09:01:27+08:00",
          "tree_id": "22b9e298a97cd9641a3798f7e19dae105c71a188",
          "url": "https://github.com/rulego/streamsql/commit/192b2e5b06c18f94c4794025d51c789a92913f6a"
        },
        "date": 1784768693889,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql)",
            "value": 788.8,
            "unit": "ns/op\t         0 drop_rate_%\t   1267774 ops/sec\t        91.11 process_rate_%\t     349 B/op\t       3 allocs/op",
            "extra": "4179660 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ns/op",
            "value": 788.8,
            "unit": "ns/op",
            "extra": "4179660 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - drop_rate_%",
            "value": 0,
            "unit": "drop_rate_%",
            "extra": "4179660 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - ops/sec",
            "value": 1267774,
            "unit": "ops/sec",
            "extra": "4179660 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - process_rate_%",
            "value": 91.11,
            "unit": "process_rate_%",
            "extra": "4179660 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - B/op",
            "value": 349,
            "unit": "B/op",
            "extra": "4179660 times\n4 procs"
          },
          {
            "name": "BenchmarkConfigurationOptimized/HighPerformance (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "4179660 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql)",
            "value": 586.3,
            "unit": "ns/op\t   1705668 pure_ops/sec\t     313 B/op\t       2 allocs/op",
            "extra": "5133979 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - ns/op",
            "value": 586.3,
            "unit": "ns/op",
            "extra": "5133979 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - pure_ops/sec",
            "value": 1705668,
            "unit": "pure_ops/sec",
            "extra": "5133979 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - B/op",
            "value": 313,
            "unit": "B/op",
            "extra": "5133979 times\n4 procs"
          },
          {
            "name": "BenchmarkPureInputOptimized (github.com/rulego/streamsql) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5133979 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql)",
            "value": 382.9,
            "unit": "ns/op\t         0 data_chan_usage_%\t   2611540 ops/sec\t        33.33 process_rate_%\t         0 result_chan_usage_%\t   2087636 results\t     134 B/op\t       1 allocs/op",
            "extra": "6262909 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ns/op",
            "value": 382.9,
            "unit": "ns/op",
            "extra": "6262909 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - data_chan_usage_%",
            "value": 0,
            "unit": "data_chan_usage_%",
            "extra": "6262909 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - ops/sec",
            "value": 2611540,
            "unit": "ops/sec",
            "extra": "6262909 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - process_rate_%",
            "value": 33.33,
            "unit": "process_rate_%",
            "extra": "6262909 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - result_chan_usage_%",
            "value": 0,
            "unit": "result_chan_usage_%",
            "extra": "6262909 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - results",
            "value": 2087636,
            "unit": "results",
            "extra": "6262909 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - B/op",
            "value": 134,
            "unit": "B/op",
            "extra": "6262909 times\n4 procs"
          },
          {
            "name": "BenchmarkMemoryEfficiency/Lightweight5K (github.com/rulego/streamsql) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6262909 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql)",
            "value": 262.5,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "9351956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - ns/op",
            "value": 262.5,
            "unit": "ns/op",
            "extra": "9351956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "9351956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_FilterProject (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9351956 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql)",
            "value": 323.8,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "7372892 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - ns/op",
            "value": 323.8,
            "unit": "ns/op",
            "extra": "7372892 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "7372892 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_MultiFieldFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "7372892 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql)",
            "value": 1792,
            "unit": "ns/op\t     459 B/op\t      11 allocs/op",
            "extra": "1353452 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - ns/op",
            "value": 1792,
            "unit": "ns/op",
            "extra": "1353452 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - B/op",
            "value": 459,
            "unit": "B/op",
            "extra": "1353452 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_ComputedFields (github.com/rulego/streamsql) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1353452 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql)",
            "value": 685.2,
            "unit": "ns/op\t     464 B/op\t       7 allocs/op",
            "extra": "3517992 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - ns/op",
            "value": 685.2,
            "unit": "ns/op",
            "extra": "3517992 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - B/op",
            "value": 464,
            "unit": "B/op",
            "extra": "3517992 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_StringConcat (github.com/rulego/streamsql) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3517992 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql)",
            "value": 284,
            "unit": "ns/op\t     344 B/op\t       3 allocs/op",
            "extra": "8304886 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - ns/op",
            "value": 284,
            "unit": "ns/op",
            "extra": "8304886 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "8304886 times\n4 procs"
          },
          {
            "name": "BenchmarkMainPath_NoFilter (github.com/rulego/streamsql) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "8304886 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions)",
            "value": 17,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "141377755 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - ns/op",
            "value": 17,
            "unit": "ns/op",
            "extra": "141377755 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "141377755 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorIncremental (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "141377755 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions)",
            "value": 4.094,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "635328228 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.094,
            "unit": "ns/op",
            "extra": "635328228 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "635328228 times\n4 procs"
          },
          {
            "name": "BenchmarkAggregatorBatch (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "635328228 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions)",
            "value": 990.6,
            "unit": "ns/op\t      80 B/op\t       4 allocs/op",
            "extra": "2395891 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - ns/op",
            "value": 990.6,
            "unit": "ns/op",
            "extra": "2395891 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - B/op",
            "value": 80,
            "unit": "B/op",
            "extra": "2395891 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Arithmetic (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "2395891 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions)",
            "value": 869.7,
            "unit": "ns/op\t      88 B/op\t       5 allocs/op",
            "extra": "2750545 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - ns/op",
            "value": 869.7,
            "unit": "ns/op",
            "extra": "2750545 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "2750545 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_FunctionCall (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2750545 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions)",
            "value": 343.3,
            "unit": "ns/op\t     136 B/op\t       5 allocs/op",
            "extra": "6938683 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - ns/op",
            "value": 343.3,
            "unit": "ns/op",
            "extra": "6938683 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - B/op",
            "value": 136,
            "unit": "B/op",
            "extra": "6938683 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_StringConcat (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6938683 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions)",
            "value": 431.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "5558179 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - ns/op",
            "value": 431.7,
            "unit": "ns/op",
            "extra": "5558179 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "5558179 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_Field (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5558179 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions)",
            "value": 47250,
            "unit": "ns/op\t   45399 B/op\t     303 allocs/op",
            "extra": "50605 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - ns/op",
            "value": 47250,
            "unit": "ns/op",
            "extra": "50605 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - B/op",
            "value": 45399,
            "unit": "B/op",
            "extra": "50605 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_CreateEnv (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 303,
            "unit": "allocs/op",
            "extra": "50605 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions)",
            "value": 4.687,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "512679889 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - ns/op",
            "value": 4.687,
            "unit": "ns/op",
            "extra": "512679889 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "512679889 times\n4 procs"
          },
          {
            "name": "BenchmarkExprBridge_ListAll (github.com/rulego/streamsql/functions) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "512679889 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window)",
            "value": 520.7,
            "unit": "ns/op\t     337 B/op\t       5 allocs/op",
            "extra": "4383786 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - ns/op",
            "value": 520.7,
            "unit": "ns/op",
            "extra": "4383786 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - B/op",
            "value": 337,
            "unit": "B/op",
            "extra": "4383786 times\n4 procs"
          },
          {
            "name": "BenchmarkTumblingWindowThroughput (github.com/rulego/streamsql/window) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4383786 times\n4 procs"
          }
        ]
      }
    ]
  }
}