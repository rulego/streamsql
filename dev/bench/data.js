window.BENCHMARK_DATA = {
  "lastUpdate": 1783561646940,
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
      }
    ]
  }
}