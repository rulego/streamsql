window.BENCHMARK_DATA = {
  "lastUpdate": 1783507058584,
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
      }
    ]
  }
}