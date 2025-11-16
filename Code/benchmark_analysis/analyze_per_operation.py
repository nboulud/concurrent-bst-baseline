#!/usr/bin/env python3
"""
Per-Operation Benchmark Analysis Script

This script analyzes benchmark results and provides detailed per-operation
(insert, delete, contains) speedup comparison between MyBST, MyBSTBaseline, and BST.

Calculates per-operation throughput from raw CSV files:
- Insert throughput = (ninstrue + ninsfalse) / time
- Delete throughput = (ndeltrue + ndelfalse) / time  
- Contains throughput = (ncontainstrue + ncontainsfalse) / time

Usage: python3 analyze_per_operation.py
Output: Writes to PER_OPERATION_ANALYSIS.txt
"""

import pandas as pd
import glob
import os
import sys
import numpy as np

# Change to script directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Redirect output to file
output_file = "PER_OPERATION_ANALYSIS.txt"
sys.stdout = open(output_file, 'w')

print("=" * 160)
print("PER-OPERATION ANALYSIS - MyBST vs MyBSTBaseline vs BST")
print("Detailed breakdown by operation type (insert, delete, contains)")
print("=" * 160)

# Summary statistics
test_results = {
    'MyBST_wins': 0,
    'MyBSTBase_wins': 0,
    'total_tests': 0,
    'test_details': []
}

# Get all raw CSV files (not statistics files)
csv_files = sorted(glob.glob("results/overhead_*.csv"))
csv_files = [f for f in csv_files if "_statistics.csv" not in f]

for csv_file in csv_files:
    test_name = os.path.basename(csv_file).replace(".csv", "")
    
    # Parse test type
    if "0sizeThreads_0delay" in test_name:
        test_type = "OVERHEAD (No queries)"
    elif "1sizeThreads_0delay" in test_name:
        test_type = "OVERHEAD (1 query thread, 0µs delay)"
    elif "1sizeThreads_700delay" in test_name:
        test_type = "OVERHEAD (1 query thread, 700µs delay)"
    else:
        continue  # Skip if not overhead test
    
    # Parse workload type
    if "30ins-20rem" in test_name:
        workload = "Update-Heavy"
        workload_detail = "(30% insert, 20% delete, 50% contains)"
    elif "3ins-2rem" in test_name:
        workload = "Read-Heavy"
        workload_detail = "(3% insert, 2% delete, 95% contains)"
    else:
        continue
    
    print(f"\n{'=' * 160}")
    print(f"TEST: {test_type}")
    print(f"WORKLOAD: {workload} {workload_detail}")
    print(f"FILE: {os.path.basename(csv_file)}")
    print(f"{'=' * 160}")
    
    try:
        # Read CSV file with error handling for malformed data
        df = pd.read_csv(csv_file, on_bad_lines='skip')
        
        # Convert numeric columns to proper types
        numeric_cols = ['ninstrue', 'ninsfalse', 'ndeltrue', 'ndelfalse', 
                       'ncontainstrue', 'ncontainsfalse', 'time', 'totalThroughput', 
                       'nWorkloadThreads', 'nSizeThreads', 'nsize', 'sizeThreadsThroughput']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Calculate per-operation throughputs
        df['insert_ops'] = df['ninstrue'] + df['ninsfalse']
        df['delete_ops'] = df['ndeltrue'] + df['ndelfalse']
        df['contains_ops'] = df['ncontainstrue'] + df['ncontainsfalse']
        
        df['insert_throughput'] = df['insert_ops'] / df['time']
        df['delete_throughput'] = df['delete_ops'] / df['time']
        df['contains_throughput'] = df['contains_ops'] / df['time']
        
        # Calculate size query throughput (rank/select operations)
        # Note: The benchmark doesn't distinguish between rank and select, they're counted together as "size" operations
        df['size_throughput'] = df['nsize'] / df['time']
        
        # Use the existing nWorkloadThreads column
        df['workloadThreads'] = df['nWorkloadThreads']
        
        # Check if there are size threads
        has_size_threads = (df['nSizeThreads'] > 0).any()
        
        # Drop rows with invalid data
        df = df.dropna(subset=['workloadThreads'])
        df['workloadThreads'] = df['workloadThreads'].astype(int)
        
        unique_threads = sorted(df['workloadThreads'].unique())
        
        for threads in unique_threads:
            thread_data = df[df['workloadThreads'] == threads]
            
            # Calculate mean throughput for each data structure
            bst_data = thread_data[thread_data['name'] == 'BST']
            mybst_data = thread_data[thread_data['name'] == 'MyBST']
            mybase_data = thread_data[thread_data['name'] == 'MyBSTBaseline']
            
            if len(bst_data) == 0:
                continue
            
            print(f"\n{threads} Workload Threads:")
            print("-" * 160)
            print(f"{'Operation':<12} {'BST (Kops/s)':<15} {'MyBST (Kops/s)':<17} {'MyBST vs BST':<22} {'MyBSTBase (Kops/s)':<19} {'MyBSTBase vs BST':<22} {'MyBST vs MyBSTBase':<25}")
            print("-" * 160)
            
            # Build operations list based on whether size threads exist
            operations = [
                ('Insert', 'insert_throughput'),
                ('Delete', 'delete_throughput'),
                ('Contains', 'contains_throughput'),
            ]
            
            # Add size query operations if there are size threads
            if has_size_threads:
                operations.append(('Size/Query', 'size_throughput'))
            
            operations.append(('TOTAL', 'totalThroughput'))
            
            for op_name, op_column in operations:
                print(f"{op_name:<12} ", end="")
                
                # Get BST throughput
                bst_tp = bst_data[op_column].mean() if len(bst_data) > 0 else None
                
                if bst_tp is not None and not np.isnan(bst_tp):
                    print(f"{bst_tp/1000:>13,.1f}  ", end="")
                else:
                    print(f"{'N/A':>13}  ", end="")
                    bst_tp = None
                
                # Get MyBST throughput
                mybst_tp = mybst_data[op_column].mean() if len(mybst_data) > 0 else None
                
                if mybst_tp is not None and not np.isnan(mybst_tp):
                    print(f"{mybst_tp/1000:>15,.1f}  ", end="")
                    
                    # MyBST vs BST
                    if bst_tp is not None and bst_tp > 0:
                        speedup = mybst_tp / bst_tp
                        overhead = (1 - speedup) * 100
                        color = "🟢" if speedup >= 0.80 else "🟡" if speedup >= 0.50 else "🔴"
                        print(f"{color} {speedup:>5.3f}x ({overhead:>+6.1f}%)  ", end="")
                    else:
                        print(f"{'':>22}  ", end="")
                else:
                    print(f"{'N/A':>15}  {'':>22}  ", end="")
                    mybst_tp = None
                
                # Get MyBSTBaseline throughput
                mybase_tp = mybase_data[op_column].mean() if len(mybase_data) > 0 else None
                
                if mybase_tp is not None and not np.isnan(mybase_tp):
                    print(f"{mybase_tp/1000:>17,.1f}  ", end="")
                    
                    # MyBSTBaseline vs BST
                    if bst_tp is not None and bst_tp > 0:
                        speedup = mybase_tp / bst_tp
                        overhead = (1 - speedup) * 100
                        color = "🟢" if speedup >= 0.80 else "🟡" if speedup >= 0.50 else "🔴"
                        print(f"{color} {speedup:>5.3f}x ({overhead:>+6.1f}%)  ", end="")
                    else:
                        print(f"{'':>22}  ", end="")
                else:
                    print(f"{'N/A':>17}  {'':>22}  ", end="")
                    mybase_tp = None
                
                # MyBST vs MyBSTBaseline
                if mybst_tp is not None and mybase_tp is not None and mybst_tp > 0 and mybase_tp > 0:
                    if mybst_tp > mybase_tp:
                        ratio = mybst_tp / mybase_tp
                        diff_pct = ((mybst_tp - mybase_tp) / mybase_tp) * 100
                        winner = "✅ MyBST"
                        print(f"{winner} {ratio:>5.3f}x (+{diff_pct:.1f}%)", end="")
                        if op_name == 'TOTAL':
                            test_results['MyBST_wins'] += 1
                    elif mybase_tp > mybst_tp:
                        ratio = mybase_tp / mybst_tp
                        diff_pct = ((mybase_tp - mybst_tp) / mybst_tp) * 100
                        winner = "✅ MyBSTBase"
                        print(f"{winner} {ratio:>5.3f}x (+{diff_pct:.1f}%)", end="")
                        if op_name == 'TOTAL':
                            test_results['MyBSTBase_wins'] += 1
                    else:
                        print(f"⚖️  Equal", end="")
                    
                    if op_name == 'TOTAL':
                        test_results['total_tests'] += 1
                        test_results['test_details'].append({
                            'test_type': test_type,
                            'workload': workload,
                            'threads': threads,
                            'winner': winner.split()[1] if 'MyBST' in winner else 'Equal',
                            'mybst_tp': mybst_tp,
                            'mybase_tp': mybase_tp,
                            'bst_tp': bst_tp
                        })
                
                print()
            
            print()
    
    except Exception as e:
        print(f"\nError processing {csv_file}: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 160)
print("DETAILED BREAKDOWN BY WORKLOAD AND TEST TYPE")
print("=" * 160)

# Group by test configuration
configs = {}
for detail in test_results['test_details']:
    key = f"{detail['workload']} - {detail['test_type']}"
    if key not in configs:
        configs[key] = {
            'mybst_sum': 0,
            'mybase_sum': 0,
            'bst_sum': 0,
            'count': 0,
            'mybst_wins': 0,
            'mybase_wins': 0
        }
    configs[key]['mybst_sum'] += detail['mybst_tp']
    configs[key]['mybase_sum'] += detail['mybase_tp']
    configs[key]['bst_sum'] += detail['bst_tp']
    configs[key]['count'] += 1
    if detail['winner'] == 'MyBST':
        configs[key]['mybst_wins'] += 1
    elif detail['winner'] == 'MyBSTBase':
        configs[key]['mybase_wins'] += 1

for key, data in sorted(configs.items()):
    mybst_avg = data['mybst_sum'] / data['count']
    mybase_avg = data['mybase_sum'] / data['count']
    bst_avg = data['bst_sum'] / data['count']
    
    print(f"\n{key}:")
    print(f"  Average MyBST throughput:         {mybst_avg/1e6:>8.3f} Mop/s ({mybst_avg/bst_avg*100:>5.1f}% of BST)")
    print(f"  Average MyBSTBaseline throughput: {mybase_avg/1e6:>8.3f} Mop/s ({mybase_avg/bst_avg*100:>5.1f}% of BST)")
    print(f"  Average BST throughput:           {bst_avg/1e6:>8.3f} Mop/s")
    
    if mybst_avg > mybase_avg:
        ratio = mybst_avg / mybase_avg
        diff_pct = ((mybst_avg - mybase_avg) / mybase_avg) * 100
        print(f"  Winner: ✅ MyBST ({ratio:.3f}x faster, +{diff_pct:.1f}%)")
    else:
        ratio = mybase_avg / mybst_avg
        diff_pct = ((mybase_avg - mybst_avg) / mybst_avg) * 100
        print(f"  Winner: ✅ MyBSTBaseline ({ratio:.3f}x faster, +{diff_pct:.1f}%)")
    
    print(f"  Config wins: MyBST={data['mybst_wins']}/{data['count']}, MyBSTBase={data['mybase_wins']}/{data['count']}")

print("\n" + "=" * 160)
print("FINAL SUMMARY")
print("=" * 160)
print(f"MyBST wins: {test_results['MyBST_wins']} out of {test_results['total_tests']} configurations")
print(f"MyBSTBaseline wins: {test_results['MyBSTBase_wins']} out of {test_results['total_tests']} configurations")
if test_results['total_tests'] > 0:
    print(f"MyBST win rate: {test_results['MyBST_wins']/test_results['total_tests']*100:.1f}%")
    print(f"MyBSTBaseline win rate: {test_results['MyBSTBase_wins']/test_results['total_tests']*100:.1f}%")
print("\n" + "=" * 160)
print("Legend: 🟢 <20% overhead  🟡 20-50% overhead  🔴 >50% overhead")
print("Winner: ✅ indicates which implementation is faster in direct comparison")
print("=" * 160)

# Close output file
sys.stdout.close()
sys.stdout = sys.__stdout__

print(f"Per-operation analysis complete! Results saved to {output_file}")
