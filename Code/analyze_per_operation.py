#!/usr/bin/env python3
"""
Per-Operation Performance Analysis for Academic-Style Benchmark Results
Analyzes speedup for EACH operation (insert, delete, contains, size, rank, select) at EACH workload
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.facecolor'] = 'white'

def load_data(csv_file):
    """Load and prepare data"""
    df = pd.read_csv(csv_file)
    
    # Aggregate by (name, workload)
    agg_df = df.groupby(['name', 'workload']).agg({
        'totalThroughput': 'mean',
        'insThroughput': 'mean',
        'delThroughput': 'mean',
        'containsThroughput': 'mean',
        'sizeThroughput': 'mean',
        'rankThroughput': 'mean',
        'selectThroughput': 'mean',
        'totalThreads': 'first'
    }).reset_index()
    
    return agg_df

def compute_all_speedups(df):
    """Compute speedups for ALL operations at each workload"""
    results = []
    
    workloads = df['workload'].unique()
    
    for wl in sorted(workloads):
        baseline_row = df[(df['name'] == 'MyBSTBaseline') & (df['workload'] == wl)]
        mybst_row = df[(df['name'] == 'MyBST') & (df['workload'] == wl)]
        
        if len(baseline_row) == 0 or len(mybst_row) == 0:
            continue
        
        baseline = baseline_row.iloc[0]
        mybst = mybst_row.iloc[0]
        
        # Extract thread counts from workload name
        parts = wl.split('-')
        thread_counts = {}
        for part in parts:
            if 'ins' in part:
                thread_counts['insert'] = int(part.replace('ins', ''))
            elif 'del' in part:
                thread_counts['delete'] = int(part.replace('del', ''))
            elif 'con' in part:
                thread_counts['contains'] = int(part.replace('con', ''))
            elif 'size' in part:
                thread_counts['size'] = int(part.replace('size', ''))
            elif 'rank' in part:
                thread_counts['rank'] = int(part.replace('rank', ''))
            elif 'sel' in part:
                thread_counts['select'] = int(part.replace('sel', ''))
        
        # Compute speedups for each operation
        insert_speedup = mybst['insThroughput'] / baseline['insThroughput'] if baseline['insThroughput'] > 0 else 0
        delete_speedup = mybst['delThroughput'] / baseline['delThroughput'] if baseline['delThroughput'] > 0 else 0
        contains_speedup = mybst['containsThroughput'] / baseline['containsThroughput'] if baseline['containsThroughput'] > 0 else 0
        size_speedup = mybst['sizeThroughput'] / baseline['sizeThroughput'] if baseline['sizeThroughput'] > 0 else np.nan
        rank_speedup = mybst['rankThroughput'] / baseline['rankThroughput'] if baseline['rankThroughput'] > 0 else np.nan
        select_speedup = mybst['selectThroughput'] / baseline['selectThroughput'] if baseline['selectThroughput'] > 0 else np.nan
        overall_speedup = mybst['totalThroughput'] / baseline['totalThroughput']
        
        results.append({
            'workload': wl,
            'workload_short': wl.replace('ins', 'i').replace('del', 'd').replace('con', 'c').replace('size', 's').replace('rank', 'r').replace('sel', 'e'),
            'total_threads': int(baseline['totalThreads']),
            'insert_threads': thread_counts.get('insert', 0),
            'delete_threads': thread_counts.get('delete', 0),
            'contains_threads': thread_counts.get('contains', 0),
            'size_threads': thread_counts.get('size', 0),
            'rank_threads': thread_counts.get('rank', 0),
            'select_threads': thread_counts.get('select', 0),
            'insert_speedup': insert_speedup,
            'delete_speedup': delete_speedup,
            'contains_speedup': contains_speedup,
            'size_speedup': size_speedup,
            'rank_speedup': rank_speedup,
            'select_speedup': select_speedup,
            'overall_speedup': overall_speedup,
            'baseline_insert': baseline['insThroughput'],
            'mybst_insert': mybst['insThroughput'],
            'baseline_delete': baseline['delThroughput'],
            'mybst_delete': mybst['delThroughput'],
            'baseline_contains': baseline['containsThroughput'],
            'mybst_contains': mybst['containsThroughput'],
            'baseline_size': baseline['sizeThroughput'],
            'mybst_size': mybst['sizeThroughput'],
            'baseline_rank': baseline['rankThroughput'],
            'mybst_rank': mybst['rankThroughput'],
            'baseline_select': baseline['selectThroughput'],
            'mybst_select': mybst['selectThroughput'],
            'baseline_total': baseline['totalThroughput'],
            'mybst_total': mybst['totalThroughput'],
        })
    
    return pd.DataFrame(results)

def plot_speedup_heatmap(df, output_dir):
    """Create heatmap of speedups for all operations"""
    # Prepare data for heatmap
    operations = ['insert', 'delete', 'contains', 'size', 'rank', 'select', 'overall']
    speedup_cols = [f'{op}_speedup' for op in operations]
    
    # Create matrix
    matrix_data = []
    workload_labels = []
    
    for _, row in df.iterrows():
        speedups = []
        for col in speedup_cols:
            val = row[col]
            speedups.append(val if not pd.isna(val) else 0)
        matrix_data.append(speedups)
        workload_labels.append(row['workload_short'])
    
    matrix = np.array(matrix_data).T
    
    # Create heatmap
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Use diverging colormap centered at 1.0
    vmin = 0.5
    vmax = max(3.0, matrix[matrix > 0].max())
    
    im = ax.imshow(matrix, cmap='RdYlGn', aspect='auto', vmin=vmin, vmax=vmax)
    
    # Set ticks
    ax.set_xticks(np.arange(len(workload_labels)))
    ax.set_yticks(np.arange(len(operations)))
    ax.set_xticklabels(workload_labels, rotation=45, ha='right')
    ax.set_yticklabels(['Insert', 'Delete', 'Contains', 'Size', 'Rank', 'Select', 'Overall'])
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Speedup (MyBST / MyBSTBaseline)', rotation=270, labelpad=20)
    
    # Add text annotations
    for i in range(len(operations)):
        for j in range(len(workload_labels)):
            val = matrix[i, j]
            if val > 0:
                text_color = 'white' if (val < 0.8 or val > 2.0) else 'black'
                text = ax.text(j, i, f'{val:.2f}x', ha='center', va='center', 
                             color=text_color, fontweight='bold', fontsize=9)
    
    ax.set_title('Per-Operation Speedup Heatmap (MyBST vs MyBSTBaseline)', 
                 fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('Workload Configuration', fontsize=12)
    ax.set_ylabel('Operation Type', fontsize=12)
    
    plt.tight_layout()
    plt.savefig(output_dir / 'speedup_heatmap.png', dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: speedup_heatmap.png")
    plt.close()

def plot_operation_comparison(df, output_dir):
    """Create grouped bar chart comparing all operations"""
    fig, ax = plt.subplots(figsize=(16, 8))
    
    operations = ['insert', 'delete', 'contains', 'size', 'rank', 'select']
    op_labels = ['Insert', 'Delete', 'Contains', 'Size', 'Rank', 'Select']
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
    
    x = np.arange(len(df))
    width = 0.13
    
    for i, (op, label, color) in enumerate(zip(operations, op_labels, colors)):
        speedups = df[f'{op}_speedup'].fillna(0)
        offset = (i - 2.5) * width
        bars = ax.bar(x + offset, speedups, width, label=label, color=color, alpha=0.8)
    
    ax.axhline(y=1.0, color='red', linestyle='--', linewidth=2, label='Baseline (1.0x)', zorder=0)
    ax.set_xlabel('Workload Configuration', fontsize=12, fontweight='bold')
    ax.set_ylabel('Speedup (MyBST / MyBSTBaseline)', fontsize=12, fontweight='bold')
    ax.set_title('Per-Operation Speedup Comparison Across All Workloads', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(df['workload_short'], rotation=45, ha='right')
    ax.legend(loc='upper left', ncol=3)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'operation_comparison.png', dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: operation_comparison.png")
    plt.close()

def plot_individual_operations(df, output_dir):
    """Create individual plots for each operation type"""
    operations = [
        ('insert', 'Insert Operation', '#1f77b4'),
        ('delete', 'Delete Operation', '#ff7f0e'),
        ('contains', 'Contains Operation', '#2ca02c'),
        ('size', 'Size Query', '#d62728'),
        ('rank', 'Rank Query', '#9467bd'),
        ('select', 'Select Query', '#8c564b')
    ]
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    axes = axes.flatten()
    
    for idx, (op, title, color) in enumerate(operations):
        ax = axes[idx]
        
        # Filter out NaN values
        plot_df = df[df[f'{op}_speedup'].notna()].copy()
        
        if len(plot_df) == 0:
            ax.text(0.5, 0.5, f'No {op} operations\nin any workload', 
                   ha='center', va='center', fontsize=12)
            ax.set_title(title, fontsize=12, fontweight='bold')
            continue
        
        x = np.arange(len(plot_df))
        speedups = plot_df[f'{op}_speedup']
        
        bars = ax.bar(x, speedups, color=[color if s > 1 else 'lightcoral' for s in speedups], 
                     alpha=0.7, edgecolor='black', linewidth=1)
        
        ax.axhline(y=1.0, color='red', linestyle='--', linewidth=1.5, label='Baseline', zorder=0)
        ax.set_xlabel('Workload', fontsize=10)
        ax.set_ylabel('Speedup', fontsize=10)
        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(plot_df['workload_short'], rotation=45, ha='right', fontsize=8)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add value labels
        for i, (_, row) in enumerate(plot_df.iterrows()):
            height = row[f'{op}_speedup']
            ax.text(i, height + 0.1, f'{height:.2f}x', ha='center', va='bottom', 
                   fontsize=8, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'individual_operations.png', dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: individual_operations.png")
    plt.close()

def plot_throughput_comparison(df, output_dir):
    """Create side-by-side throughput comparison for each operation"""
    operations = ['insert', 'delete', 'contains', 'size', 'rank', 'select', 'total']
    op_labels = ['Insert', 'Delete', 'Contains', 'Size', 'Rank', 'Select', 'Total']
    
    fig, axes = plt.subplots(3, 3, figsize=(20, 14))
    axes = axes.flatten()
    
    for idx, (op, label) in enumerate(zip(operations, op_labels)):
        if idx >= len(axes):
            break
            
        ax = axes[idx]
        
        baseline_col = f'baseline_{op}'
        mybst_col = f'mybst_{op}'
        
        # Filter out zero values
        plot_df = df[(df[baseline_col] > 0) | (df[mybst_col] > 0)].copy()
        
        if len(plot_df) == 0:
            ax.text(0.5, 0.5, f'No {op} operations', ha='center', va='center', fontsize=12)
            ax.set_title(f'{label} Throughput', fontsize=12, fontweight='bold')
            continue
        
        x = np.arange(len(plot_df))
        width = 0.35
        
        baseline_vals = plot_df[baseline_col]
        mybst_vals = plot_df[mybst_col]
        
        bars1 = ax.bar(x - width/2, baseline_vals, width, label='MyBSTBaseline', 
                      color='lightblue', alpha=0.8, edgecolor='black')
        bars2 = ax.bar(x + width/2, mybst_vals, width, label='MyBST', 
                      color='lightgreen', alpha=0.8, edgecolor='black')
        
        ax.set_xlabel('Workload', fontsize=10)
        ax.set_ylabel('Throughput (ops/sec)', fontsize=10)
        ax.set_title(f'{label} Throughput', fontsize=12, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(plot_df['workload_short'], rotation=45, ha='right', fontsize=8)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add percentage difference labels
        for i, (_, row) in enumerate(plot_df.iterrows()):
            baseline_val = row[baseline_col]
            mybst_val = row[mybst_col]
            if baseline_val > 0:
                pct = ((mybst_val - baseline_val) / baseline_val) * 100
                color = 'green' if pct > 0 else 'red'
                y_pos = max(baseline_val, mybst_val)
                ax.text(i, y_pos * 1.05, f'{pct:+.1f}%', ha='center', va='bottom', 
                       fontsize=7, color=color, fontweight='bold')
    
    # Hide unused subplots
    for idx in range(len(operations), len(axes)):
        axes[idx].axis('off')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'throughput_comparison.png', dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: throughput_comparison.png")
    plt.close()

def generate_detailed_report(df, output_dir):
    """Generate comprehensive markdown report"""
    report = []
    
    report.append("# Per-Operation Performance Analysis")
    report.append("")
    report.append("## MyBST vs MyBSTBaseline - Detailed Comparison")
    report.append("")
    report.append("---")
    report.append("")
    
    # Overall statistics
    report.append("## Overall Statistics")
    report.append("")
    report.append("| Operation | Avg Speedup | Min Speedup | Max Speedup | Workloads |")
    report.append("|-----------|-------------|-------------|-------------|-----------|")
    
    operations = ['insert', 'delete', 'contains', 'size', 'rank', 'select', 'overall']
    op_names = ['Insert', 'Delete', 'Contains', 'Size', 'Rank', 'Select', 'Overall']
    
    for op, name in zip(operations, op_names):
        col = f'{op}_speedup'
        valid = df[col].dropna()
        if len(valid) > 0:
            avg = valid.mean()
            min_val = valid.min()
            max_val = valid.max()
            count = len(valid)
            
            report.append(f"| **{name}** | {avg:.2f}x | {min_val:.2f}x | {max_val:.2f}x | {count} |")
        else:
            report.append(f"| **{name}** | N/A | N/A | N/A | 0 |")
    
    report.append("")
    report.append("---")
    report.append("")
    
    # Detailed per-workload breakdown
    report.append("## Detailed Per-Workload Analysis")
    report.append("")
    
    for _, row in df.iterrows():
        report.append(f"### {row['workload']}")
        report.append("")
        report.append(f"**Configuration:** {row['total_threads']} total threads")
        report.append(f"- Insert: {row['insert_threads']} threads")
        report.append(f"- Delete: {row['delete_threads']} threads")
        report.append(f"- Contains: {row['contains_threads']} threads")
        report.append(f"- Size: {row['size_threads']} threads")
        report.append(f"- Rank: {row['rank_threads']} threads")
        report.append(f"- Select: {row['select_threads']} threads")
        report.append("")
        
        report.append("| Operation | MyBSTBaseline | MyBST | Speedup | Status |")
        report.append("|-----------|---------------|-------|---------|--------|")
        
        ops = [
            ('insert', 'Insert'),
            ('delete', 'Delete'),
            ('contains', 'Contains'),
            ('size', 'Size'),
            ('rank', 'Rank'),
            ('select', 'Select'),
            ('total', 'Total')
        ]
        
        for op, name in ops:
            baseline = row[f'baseline_{op}']
            mybst = row[f'mybst_{op}']
            speedup = row[f'{op}_speedup'] if op != 'total' else row['overall_speedup']
            
            if pd.isna(speedup) or baseline == 0:
                report.append(f"| {name} | - | - | - | N/A |")
            else:
                status = "✓" if speedup > 1.0 else "✗"
                pct = (speedup - 1) * 100
                report.append(f"| {name} | {baseline:,.0f} | {mybst:,.0f} | {speedup:.2f}x ({pct:+.1f}%) | {status} |")
        
        report.append("")
        report.append("---")
        report.append("")
    
    # Key findings
    report.append("## Key Findings")
    report.append("")
    
    # Best and worst performers
    for op, name in zip(operations, op_names):
        col = f'{op}_speedup'
        valid_df = df[df[col].notna()]
        
        if len(valid_df) > 0:
            best_row = valid_df.loc[valid_df[col].idxmax()]
            worst_row = valid_df.loc[valid_df[col].idxmin()]
            
            report.append(f"### {name}")
            report.append(f"- **Best:** {best_row['workload']} ({best_row[col]:.2f}x)")
            report.append(f"- **Worst:** {worst_row['workload']} ({worst_row[col]:.2f}x)")
            report.append(f"- **Average:** {valid_df[col].mean():.2f}x")
            report.append("")
    
    # Write report
    report_file = output_dir / 'PER_OPERATION_ANALYSIS.md'
    with open(report_file, 'w') as f:
        f.write('\n'.join(report))
    
    print(f"  ✓ Saved: PER_OPERATION_ANALYSIS.md")

def main():
    csv_file = 'academic_benchmark_specialized.csv'
    output_dir = Path('per_operation_analysis')
    output_dir.mkdir(exist_ok=True)
    
    print("╔══════════════════════════════════════════════════════════════════════════╗")
    print("║            Per-Operation Performance Analysis                           ║")
    print("╚══════════════════════════════════════════════════════════════════════════╝")
    print()
    
    # Load data
    print("📊 Loading data...")
    df = load_data(csv_file)
    print(f"  ✓ Loaded {len(df)} workload configurations")
    print()
    
    # Compute speedups
    print("📈 Computing per-operation speedups...")
    speedup_df = compute_all_speedups(df)
    
    # Save detailed results
    speedup_csv = output_dir / 'per_operation_speedups.csv'
    speedup_df.to_csv(speedup_csv, index=False)
    print(f"  ✓ Saved: per_operation_speedups.csv")
    print()
    
    # Generate visualizations
    print("📊 Generating visualizations...")
    plot_speedup_heatmap(speedup_df, output_dir)
    plot_operation_comparison(speedup_df, output_dir)
    plot_individual_operations(speedup_df, output_dir)
    plot_throughput_comparison(speedup_df, output_dir)
    print()
    
    # Generate report
    print("📝 Generating detailed report...")
    generate_detailed_report(speedup_df, output_dir)
    print()
    
    # Print summary
    print("═══════════════════════════════════════════════════════════════════════════")
    print("SUMMARY - Average Speedup by Operation")
    print("═══════════════════════════════════════════════════════════════════════════")
    print()
    
    operations = [
        ('insert', 'Insert'),
        ('delete', 'Delete'),
        ('contains', 'Contains'),
        ('size', 'Size'),
        ('rank', 'Rank'),
        ('select', 'Select'),
        ('overall', 'Overall')
    ]
    
    for op, name in operations:
        col = f'{op}_speedup'
        valid = speedup_df[col].dropna()
        if len(valid) > 0:
            avg = valid.mean()
            pct = (avg - 1) * 100
            status = "✓" if avg > 1.0 else "✗"
            print(f"{name:12} {avg:.2f}x ({pct:+6.1f}%) {status}")
        else:
            print(f"{name:12} N/A")
    
    print()
    print("═══════════════════════════════════════════════════════════════════════════")
    print()
    print(f"All results saved to: {output_dir}/")

if __name__ == '__main__':
    main()
