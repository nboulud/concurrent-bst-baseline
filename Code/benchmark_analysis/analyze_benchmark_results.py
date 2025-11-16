#!/usr/bin/env python3
"""
Comprehensive Benchmark Results Analysis
Compares MyBST vs MyBSTBaseline vs BST across all test configurations
"""

import pandas as pd
import glob
import os

print("\n" + "=" * 150)
print("COMPREHENSIVE BENCHMARK ANALYSIS - MyBST vs MyBSTBaseline vs BST")
print("=" * 150)

# Summary statistics
test_results = {
    'MyBST_wins': 0,
    'MyBSTBase_wins': 0,
    'total_tests': 0
}

# Get all statistics files
csv_files = sorted(glob.glob("results/*_statistics.csv"))

for csv_file in csv_files:
    test_name = os.path.basename(csv_file).replace("_statistics.csv", "")
    
    # Parse test type
    if "overhead" in test_name:
        if "0sizeThreads_0delay" in test_name:
            test_type = "OVERHEAD (No queries)"
        elif "1sizeThreads_0delay" in test_name:
            test_type = "OVERHEAD (1 query thread, 0µs delay)"
        elif "1sizeThreads_700delay" in test_name:
            test_type = "OVERHEAD (1 query thread, 700µs delay)"
    elif "scalability" in test_name:
        test_type = "SCALABILITY (varying query threads)"
    
    # Parse workload type
    if "30ins-20rem" in test_name:
        workload = "Update-Heavy (30% ins, 20% del, 50% contains)"
    elif "3ins-2rem" in test_name:
        workload = "Read-Heavy (3% ins, 2% del, 95% contains)"
    
    print(f"\n{'=' * 150}")
    print(f"TEST: {test_type}")
    print(f"WORKLOAD: {workload}")
    print(f"{'=' * 150}")
    
    df = pd.read_csv(csv_file)
    
    # Parse benchmark names - handle scalability differently
    def parse_benchmark(name):
        parts = name.split('-')
        ds = parts[0]
        workload_threads = int(parts[1].replace('w', ''))
        size_threads = int(parts[2].replace('s', ''))
        return ds, workload_threads, size_threads
    
    df[['dataStructure', 'workloadThreads', 'sizeThreads']] = df['benchmark'].apply(
        lambda x: pd.Series(parse_benchmark(x))
    )
    
    # For scalability tests, use sizeThreads as the variable
    if "scalability" in test_name:
        index_col = 'sizeThreads'
    else:
        index_col = 'workloadThreads'
    
    try:
        # Pivot data
        pivot = df.pivot(index=index_col, columns='dataStructure', values='meanTP')
        
        # Check which structures are available
        available = [c for c in ['BST', 'MyBST', 'MyBSTBaseline'] if c in pivot.columns]
        
        if 'BST' in available and ('MyBST' in available or 'MyBSTBaseline' in available):
            # Header
            print(f"\n{index_col.replace('Threads', ' Threads'):<12} ", end="")
            if 'BST' in available:
                print(f"{'BST (ops/s)':<18} ", end="")
            if 'MyBST' in available:
                print(f"{'MyBST (ops/s)':<18} {'vs BST':<20} ", end="")
            if 'MyBSTBaseline' in available:
                print(f"{'MyBSTBase (ops/s)':<20} {'vs BST':<20} ", end="")
            if 'MyBST' in available and 'MyBSTBaseline' in available:
                print(f"{'MyBST vs MyBSTBase':<25}", end="")
            print()
            print("-" * 150)
            
            # Data rows
            for threads in sorted(pivot.index):
                print(f"{int(threads):<12} ", end="")
                
                bst_tp = pivot.loc[threads, 'BST'] if 'BST' in available else None
                mybst_tp = pivot.loc[threads, 'MyBST'] if 'MyBST' in available and pd.notna(pivot.loc[threads, 'MyBST']) else None
                mybase_tp = pivot.loc[threads, 'MyBSTBaseline'] if 'MyBSTBaseline' in available and pd.notna(pivot.loc[threads, 'MyBSTBaseline']) else None
                
                # Print BST
                if bst_tp:
                    print(f"{bst_tp:>16,.0f}  ", end="")
                
                # Print MyBST vs BST
                if mybst_tp and bst_tp:
                    speedup = mybst_tp / bst_tp
                    overhead = (1 - speedup) * 100
                    color = "🟢" if speedup >= 0.80 else "🟡" if speedup >= 0.50 else "🔴"
                    print(f"{mybst_tp:>16,.0f}  {color} {speedup:>5.3f}x ({overhead:>+6.1f}%)  ", end="")
                elif mybst_tp:
                    print(f"{mybst_tp:>16,.0f}  {'':>25}  ", end="")
                
                # Print MyBSTBaseline vs BST
                if mybase_tp and bst_tp:
                    speedup = mybase_tp / bst_tp
                    overhead = (1 - speedup) * 100
                    color = "🟢" if speedup >= 0.80 else "🟡" if speedup >= 0.50 else "🔴"
                    print(f"{mybase_tp:>18,.0f}  {color} {speedup:>5.3f}x ({overhead:>+6.1f}%)  ", end="")
                elif mybase_tp:
                    print(f"{mybase_tp:>18,.0f}  {'':>25}  ", end="")
                
                # Print MyBST vs MyBSTBaseline comparison
                if mybst_tp and mybase_tp:
                    if mybst_tp > mybase_tp:
                        ratio = mybst_tp / mybase_tp
                        diff_pct = ((mybst_tp - mybase_tp) / mybase_tp) * 100
                        winner = "✅ MyBST"
                        print(f"{winner} {ratio:>5.3f}x faster (+{diff_pct:.1f}%)", end="")
                        test_results['MyBST_wins'] += 1
                    elif mybase_tp > mybst_tp:
                        ratio = mybase_tp / mybst_tp
                        diff_pct = ((mybase_tp - mybst_tp) / mybst_tp) * 100
                        winner = "✅ MyBSTBase"
                        print(f"{winner} {ratio:>5.3f}x faster (+{diff_pct:.1f}%)", end="")
                        test_results['MyBSTBase_wins'] += 1
                    else:
                        print(f"⚖️  Equal", end="")
                    test_results['total_tests'] += 1
                
                print()
            
            # Summary averages
            print("-" * 150)
            print("AVERAGES:    ", end="")
            
            if bst_tp is not None:
                bst_avg = pivot['BST'].mean()
                print(f"{bst_avg:>16,.0f}  ", end="")
            
            if 'MyBST' in available:
                mybst_avg = pivot['MyBST'].mean()
                if pd.notna(mybst_avg) and bst_tp is not None:
                    speedup = mybst_avg / bst_avg
                    overhead = (1 - speedup) * 100
                    color = "🟢" if speedup >= 0.80 else "🟡" if speedup >= 0.50 else "🔴"
                    print(f"{mybst_avg:>16,.0f}  {color} {speedup:>5.3f}x ({overhead:>+6.1f}%)  ", end="")
            
            if 'MyBSTBaseline' in available:
                mybase_avg = pivot['MyBSTBaseline'].mean()
                if pd.notna(mybase_avg) and bst_tp is not None:
                    speedup = mybase_avg / bst_avg
                    overhead = (1 - speedup) * 100
                    color = "🟢" if speedup >= 0.80 else "🟡" if speedup >= 0.50 else "🔴"
                    print(f"{mybase_avg:>18,.0f}  {color} {speedup:>5.3f}x ({overhead:>+6.1f}%)  ", end="")
            
            if 'MyBST' in available and 'MyBSTBaseline' in available:
                mybst_avg = pivot['MyBST'].mean()
                mybase_avg = pivot['MyBSTBaseline'].mean()
                if pd.notna(mybst_avg) and pd.notna(mybase_avg):
                    if mybst_avg > mybase_avg:
                        ratio = mybst_avg / mybase_avg
                        diff_pct = ((mybst_avg - mybase_avg) / mybase_avg) * 100
                        print(f"✅ MyBST {ratio:>5.3f}x faster (+{diff_pct:.1f}%) OVERALL", end="")
                    else:
                        ratio = mybase_avg / mybst_avg
                        diff_pct = ((mybase_avg - mybst_avg) / mybst_avg) * 100
                        print(f"✅ MyBSTBase {ratio:>5.3f}x faster (+{diff_pct:.1f}%) OVERALL", end="")
            
            print()
    except Exception as e:
        print(f"\nSkipping this test due to error: {e}")

print("\n" + "=" * 150)
print("FINAL SUMMARY")
print("=" * 150)
print(f"MyBST wins: {test_results['MyBST_wins']} out of {test_results['total_tests']} thread configurations")
print(f"MyBSTBaseline wins: {test_results['MyBSTBase_wins']} out of {test_results['total_tests']} thread configurations")
print(f"MyBST win rate: {test_results['MyBST_wins']/test_results['total_tests']*100:.1f}%")
print(f"MyBSTBaseline win rate: {test_results['MyBSTBase_wins']/test_results['total_tests']*100:.1f}%")
print("\n" + "=" * 150)
print("Legend: 🟢 <20% overhead  🟡 20-50% overhead  🔴 >50% overhead")
print("Winner: ✅ indicates which implementation is faster in direct comparison")
print("=" * 150)
