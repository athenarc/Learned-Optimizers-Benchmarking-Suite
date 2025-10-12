import os
import json
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

def plot_scan_decisions(ground_truth_latencies, optimizer_data, save_dir="plots/4.1/", skip_neo=False, show_title=False):
    """
    Plots ground-truth latencies, overlays LQO decisions, and adds a background
    indicator for the classic PostgreSQL optimizer's decision.
    """
    # Define colors for different scan types
    SCAN_COLORS = {
        "index_scan": "#4daf4a",  # Green
        "seq_scan": "#e41a1c",    # Red
        "unknown": "#999999"       # Gray
    }
    
    # Define semi-transparent background colors for PostgreSQL's decisions
    PG_BG_COLORS = {
        "index_scan": "#4daf4a20",  # Light Green with alpha
        "seq_scan": "#e41a1c20"     # Light Red with alpha
    }

    os.makedirs(save_dir, exist_ok=True)
    
    sorted_queries = sorted(optimizer_data.items(), 
                            key=lambda x: int(x[0].split('_')[0]))
    
    for query_template, optimizers_data in sorted_queries:
        if query_template not in ground_truth_latencies:
            print(f"Warning: No ground-truth latency data found for {query_template}, skipping plot.")
            continue
        
        # --- NEW: Extract PostgreSQL decisions first ---
        pg_decisions = {}
        if "PostgreSQL" in optimizers_data:
            for sel, data in optimizers_data["PostgreSQL"].items():
                pg_decisions[int(sel[:-1])] = data.get('scan_decision')

        # --- MODIFIED: Exclude PostgreSQL from the main list of subplots ---
        lqo_optimizers = ["NEO", "BAO", "LERO", "LOGER", "FASTgres"]
        if skip_neo:
            lqo_optimizers.remove("NEO")
        optimizers = [opt for opt in lqo_optimizers if opt in optimizers_data]
        
        if not optimizers:
            continue
            
        num_optimizers = len(optimizers)

        fig, axes = plt.subplots(1, num_optimizers, figsize=(6 * num_optimizers, 5), sharey=True)
        if num_optimizers == 1:
            axes = [axes]
        
        selectivities = sorted(ground_truth_latencies[query_template].keys(), 
                               key=lambda x: int(x[:-1]))
        x_values = [int(s[:-1]) for s in selectivities]

        for i, optimizer in enumerate(optimizers):
            ax = axes[i]

            # --- NEW: Plot PostgreSQL decision background ---
            if pg_decisions:
                # Add dummy patches for the legend
                if i == 0: # Only add legend entries once
                    ax.fill_between([],[], color=PG_BG_COLORS['index_scan'], label='PG chose index scan')
                    ax.fill_between([],[], color=PG_BG_COLORS['seq_scan'], label='PG chose seq scan')

                    # Dummy lines for LQO scan choices (ensure they always appear in legend)
                    ax.plot([], [], 'D-', color=SCAN_COLORS['index_scan'], linewidth=3, markersize=7,
                            label='LQO chose index scan')
                    ax.plot([], [], 'D-', color=SCAN_COLORS['seq_scan'], linewidth=3, markersize=7,
                            label='LQO chose seq scan')

                    # Ground-truth always exists, but we can still force them
                    ax.plot([], [], 'o-', color='#377eb8', linewidth=2, markersize=6, alpha=0.7,
                            label='Index Scan Trajectory')
                    ax.plot([], [], 's-', color='#ff7f00', linewidth=2, markersize=6, alpha=0.7,
                            label='Seq Scan Trajectory')
                
                # Iterate through selectivity ranges to draw shaded regions
                for j in range(len(x_values) - 1):
                    start_x = x_values[j]
                    end_x = x_values[j+1]
                    decision = pg_decisions.get(start_x)
                    if decision:
                        ax.axvspan(start_x, end_x, color=PG_BG_COLORS.get(decision), zorder=0)
                # Handle the last segment
                last_decision = pg_decisions.get(x_values[-1])
                if last_decision:
                    # Heuristically extend the last region
                    ax.axvspan(x_values[-1], x_values[-1] + (x_values[1]-x_values[0]), 
                               color=PG_BG_COLORS.get(last_decision), zorder=0)


            # 1. Plot ground-truth latency curves
            index_scan_times = [ground_truth_latencies[query_template][s].get("index_scan") for s in selectivities]
            seq_scan_times = [ground_truth_latencies[query_template][s].get("seq_scan") for s in selectivities]

            ax.plot(x_values, index_scan_times, 'o-', color='#377eb8', 
                    label='Index Scan Trajectory', linewidth=2, markersize=6, alpha=0.7)
            ax.plot(x_values, seq_scan_times, 's-', color='#ff7f00', 
                    label='Seq Scan Trajectory', linewidth=2, markersize=6, alpha=0.7)

            # 2. Plot LQO's actual execution time
            opt_perf_data = optimizers_data.get(optimizer, {})
            
            points = []
            for sel in selectivities:
                if sel in opt_perf_data:
                    point_data = opt_perf_data[sel]
                    if 'exec_time' in point_data and point_data['exec_time'] is not None:
                        points.append({
                            'x': int(sel[:-1]), 
                            'y': point_data['exec_time'], 
                            'decision': point_data.get('scan_decision', 'unknown')
                        })
            
            if points:
                segments = []
                current_segment = [points[0]]
                for k in range(1, len(points)):
                    if points[k]['decision'] == current_segment[-1]['decision']:
                        current_segment.append(points[k])
                    else:
                        segments.append(current_segment)
                        current_segment = [points[k-1], points[k]]
                
                segments.append(current_segment)
                
                for seg_points in segments:
                    decision = seg_points[-1]['decision']
                    color = SCAN_COLORS.get(decision, SCAN_COLORS['unknown'])
                    label = f'LQO chose {decision.replace("_", " ")}'
                    
                    seg_x = [p['x'] for p in seg_points]
                    seg_y = [p['y'] for p in seg_points]
                    
                    ax.plot(seg_x, seg_y, 'D-', 
                           color=color,
                           linewidth=3, markersize=7,
                           label=label)

            # Customize subplot
            ax.set_title(f"{optimizer}", fontsize=18)
            # ax.set_xlabel("Selectivity (%)", fontsize=16)
            if i == 0:
                handles, labels = ax.get_legend_handles_labels()
                by_label = dict(zip(labels, handles))
                legend_handles = list(by_label.values())
                legend_labels = list(by_label.keys())
                # ax.set_ylabel("Execution Time (ms)", fontsize=16)
            
            ax.set_xticks(x_values)
            ax.set_xticklabels(selectivities, rotation=0, ha="center")
            ax.grid(True, linestyle='--', alpha=0.6)
            ax.set_yscale('log')

            handles, labels = ax.get_legend_handles_labels()
            by_label = dict(zip(labels, handles))
            # ax.legend(by_label.values(), by_label.keys(), fontsize=12)

        fig.legend(
            legend_handles, legend_labels,
            loc='upper center',
            bbox_to_anchor=(0.5, 1.12),   # slightly above the row of plots
            ncol=4,                       # row layout; tweak depending on how many labels
            fontsize=14,                  # larger font for readability
            handlelength=3.0,             # make handles longer
            handleheight=1.5,
            frameon=True,
            facecolor='white',
            edgecolor='black',
            fancybox=True
        )

        plt.tight_layout(rect=[0, 0, 1, 0.96])
        if show_title:
            fig.suptitle(f"Scan Decisions for: {query_template}", fontsize=20, y=1.04)
        plt.savefig(os.path.join(save_dir, f"{query_template}_scan_decisions.png"), dpi=300, bbox_inches='tight')
        plt.close(fig)

import pandas as pd
import seaborn as sns

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

def plot_aggregated_scan_performance(ground_truth_latencies, optimizer_data, save_dir="plots/4.1/"):
    """
    Generates B&W-friendly aggregated plots summarizing scan decision performance 
    across all queries, binned by query complexity, using specified colors and 
    hatching patterns. It uses PostgreSQL as a clear baseline and adds a plot for
    scan operator distribution.

    Args:
        ground_truth_latencies (dict): Data from utils.extract_scan_latencies.
        optimizer_data (dict): Data from utils.extract_optimizer_data.
        save_dir (str): Directory to save the output plots.
    """
    
    # Dictionaries for styling
    optimizer_colors = {
        'PostgreSQL': '#1f77b4', 'BAO': '#2ca02c', 'LOGER': '#d62728',
        'FASTgres': '#9467bd', 'LERO': '#8c564b'
    }
    patterns = {
        'PostgreSQL': '/', 'BAO': 'xx', 'LOGER': '++',
        'FASTgres': '**', 'LERO': 'oo'
    }
    scan_choice_colors = {
        'index_scan': '#99ff99',  # Light Green
        'seq_scan': '#ff9999'     # Light Red
    }
    
    os.makedirs(save_dir, exist_ok=True)

    all_results = []
    
    # 1. Process all data points into a flat structure
    for query_template, optimizers_data in optimizer_data.items():
        if query_template not in ground_truth_latencies: continue
        try:
            join_count = int(query_template.split('_')[0])
        except (ValueError, IndexError): continue

        for optimizer, opt_perf_data in optimizers_data.items():
            if optimizer == "NEO": continue
            for sel, point_data in opt_perf_data.items():
                gt_sel_data = ground_truth_latencies[query_template].get(sel, {})
                gt_index_t, gt_seq_t = gt_sel_data.get("index_scan"), gt_sel_data.get("seq_scan")
                lqo_t, lqo_decision = point_data.get('exec_time'), point_data.get('scan_decision')

                if any(v is None for v in [gt_index_t, gt_seq_t, lqo_t, lqo_decision]): continue
                
                optimal_decision = "index_scan" if gt_index_t <= gt_seq_t else "seq_scan"
                optimal_latency = min(gt_index_t, gt_seq_t)
                is_correct = 1 if lqo_decision == optimal_decision else 0
                regret = (lqo_t / optimal_latency) if optimal_latency > 0 else float('inf')

                all_results.append({
                    "optimizer": optimizer, "join_count": join_count, 
                    "is_correct": is_correct, "regret": regret,
                    "scan_decision": lqo_decision 
                })

    if not all_results:
        print("No data to plot for aggregation."); return

    df = pd.DataFrame(all_results)
    
    # 2. Define complexity bins
    bins = [0, 4, 8, 12, 16]
    labels = ["1-4 Joins", "5-8 Joins", "9-12 Joins", "13-16 Joins"]
    df['complexity_bin'] = pd.cut(df['join_count'], bins=bins, labels=labels, right=True)
    
    # 3. Define the desired order for plotting
    optimizer_order = ["PostgreSQL", "BAO", "LERO", "LOGER", "FASTgres"]
    final_optimizer_order = [opt for opt in optimizer_order if opt in df['optimizer'].unique()]
    
    # (Blocks 4 and 5 for Accuracy and Regret plots are unchanged)

    # 4. Plot 1: Decision Accuracy (Bar Plot with Hatching)
    plt.figure(figsize=(14, 7))
    sns.set_style("whitegrid")
    ax1 = sns.barplot(data=df, x="complexity_bin", y="is_correct", hue="optimizer",
                      hue_order=final_optimizer_order, palette=optimizer_colors, errorbar=None,
                      edgecolor='black')
    for i, bar in enumerate(ax1.patches):
        optimizer_name = final_optimizer_order[i % len(final_optimizer_order)]
        if optimizer_name in patterns: bar.set_hatch(patterns[optimizer_name])
    # ax1.set_title("Scan Decision Accuracy by Query Complexity", fontsize=20, pad=20)
    ax1.set_xlabel("Query Complexity", fontsize=16)
    ax1.set_ylabel("Fraction of Correct Decisions", fontsize=16)
    ax1.set_ylim(0, 1.05)
    plt.legend(title='Optimizer', fontsize=12)
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, "aggregated_accuracy.png"), dpi=300)
    plt.close()

    # 5. Plot 2: Performance Regret (Box Plot with Hatching)
    plt.figure(figsize=(14, 7))
    sns.set_style("whitegrid")
    df['regret_capped'] = df['regret'].clip(upper=1000)
    ax2 = sns.boxplot(data=df, x="complexity_bin", y="regret_capped", hue="optimizer",
                      hue_order=final_optimizer_order, palette=optimizer_colors, fliersize=3)
    for i, artist in enumerate(ax2.artists):
        optimizer_name = final_optimizer_order[i % len(final_optimizer_order)]
        if optimizer_name in patterns:
            artist.set_hatch(patterns[optimizer_name])
            artist.set_edgecolor('black')
    ax2.set_yscale('log')
    ax2.axhline(y=1.0, color='r', linestyle='--', label='Optimal (No Regret)')
    # ax2.set_title("Performance Regret of Scan Decisions by Query Complexity", fontsize=20, pad=20)
    ax2.set_xlabel("Query Complexity", fontsize=16)
    ax2.set_ylabel("Performance Regret (Slowdown Factor, Log Scale)", fontsize=16)
    plt.legend(title='Optimizer', fontsize=12, loc='upper left')
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, "aggregated_regret.png"), dpi=300)
    plt.close()

    # 6. Plot 3: Scan Operator Distribution (Clean Pie Charts)
    dist_data = df.groupby(['complexity_bin', 'optimizer', 'scan_decision']).size().unstack(fill_value=0)
    
    # Iterate through each complexity bin to create a separate plot
    for bin_label in labels:
        bin_df = dist_data.loc[bin_label]
        
        # Filter to only include optimizers present in this bin
        ordered_optimizers = [opt for opt in final_optimizer_order if opt in bin_df.index]
        num_optimizers = len(ordered_optimizers)
        
        if num_optimizers == 0:
            continue

        fig, axes = plt.subplots(1, num_optimizers, figsize=(5 * num_optimizers, 5))
        if num_optimizers == 1:
            axes = [axes]
        
        for ax, optimizer in zip(axes, ordered_optimizers):
            scan_counts = bin_df.loc[optimizer]
            categories = ["index_scan", "seq_scan"]
            # Ensure columns exist, fill with 0 if not
            sizes = [scan_counts.get(cat, 0) for cat in categories]
            
            # Filter out zero-sized slices for a cleaner pie chart
            non_zero_indices = [i for i, size in enumerate(sizes) if size > 0]
            
            if not non_zero_indices:
                ax.text(0.5, 0.5, "No Data", ha='center', va='center', fontsize=12)
                ax.set_title(optimizer, fontsize=16, pad=20)
                ax.set_aspect('equal')
                continue

            pie_labels = [categories[i].replace("_", " ").title() for i in non_zero_indices]
            pie_sizes = [sizes[i] for i in non_zero_indices]
            pie_colors = [scan_choice_colors.get(categories[i]) for i in non_zero_indices]
            
            wedges, texts, autotexts = ax.pie(pie_sizes, colors=pie_colors, autopct='%1.1f%%', 
                                              startangle=90, textprops={'fontsize': 14, 'weight': 'bold'})
            
            ax.set_aspect('equal')
            ax.set_title(optimizer, fontsize=16, pad=20)
        
        # Create a single legend for the figure
        fig.legend(handles=wedges, labels=pie_labels, loc='upper right', fontsize=12, title='Scan Type')
        
        plt.tight_layout(rect=[0, 0, 0.9, 0.95]) # Adjust rect to make space for legend
        # fig.suptitle(f"Scan Operator Distribution for {bin_label}", fontsize=18)
        
        plot_path = os.path.join(save_dir, f"scan_dist_{bin_label.replace(' ', '_')}.png")
        print(f"Saving plot: {plot_path}")
        plt.savefig(plot_path, bbox_inches='tight', dpi=300)
        plt.close(fig)

def plot_scan_latencies(latencies, save_dir="plots/4.1/"):
    """
    Plots all query templates up to the specified number of joins
    """
    # Get all query templates and sort them by join level
    query_templates = sorted([q for q in latencies.keys() if int(q.split('_')[0])],
                           key=lambda x: (int(x.split('_')[0]), x))
    
    if not query_templates:
        print("No query templates found!")
        return
    
    # Create a figure with subplots
    num_plots = len(query_templates)
    cols = 3  # Number of columns in subplot grid
    rows = (num_plots + cols - 1) // cols
    
    fig, axes = plt.subplots(rows, cols, figsize=(18, 5*rows))
    fig.tight_layout(pad=5.0)
    
    # If only one plot, axes won't be an array
    if num_plots == 1:
        axes = np.array([axes])
    
    axes = axes.flatten()  # Flatten for easy iteration
    
    for i, query_template in enumerate(query_templates):
        if i >= len(axes):  # In case we have more plots than axes
            break
            
        data = latencies[query_template]
        selectivities = sorted(data.keys(), key=lambda x: int(x[:-1]))
        
        # Prepare data
        x_values = [int(s[:-1]) for s in selectivities]
        index_scan_times = [data[s].get("index_scan", None) for s in selectivities]
        seq_scan_times = [data[s].get("seq_scan", None) for s in selectivities]
        
        # Plot
        ax = axes[i]
        if any(t is not None for t in index_scan_times):
            ax.plot(x_values, index_scan_times, 'o-', label='Index Scan', linewidth=2, markersize=5)
        if any(t is not None for t in seq_scan_times):
            ax.plot(x_values, seq_scan_times, 's-', label='Seq Scan', linewidth=2, markersize=5)
        
        plot_title= query_template.replace('_', ' ')
        # Keep only the first 2 words
        plot_title = ' '.join(plot_title.split()[:2])
        
        # Customize subplot
        ax.set_title(plot_title)
        ax.set_xlabel("Selectivity (%)", fontsize=16)
        ax.set_ylabel("Execution Time (ms)", fontsize=16)
        ax.set_xticks(x_values)
        ax.set_xticklabels(selectivities)
        ax.grid(True, linestyle='--', alpha=0.5)
        ax.legend(fontsize=16)
    
    # Hide any unused subplots
    for j in range(i+1, len(axes)):
        axes[j].axis('off')
    
    # Create the directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(os.path.join(save_dir, "all_queries_scan_latencies.png"), dpi=300, bbox_inches='tight')
    plt.show()

def plot_join_operator_composition(operator_counts, execution_times, save_dir="plots/4.2/"):
    """
    Generates pie charts per optimizer at each join level, showing the distribution
    of join operators used. Also calculates and displays speedup vs. PostgreSQL based
    on the ACCUMULATED (total) execution time for the workload.
    
    Args:
        operator_counts (dict): Data from utils.analyze_join_operators.
        execution_times (dict): Data from utils.analyze_join_operators.
        save_dir (str): Directory to save the output plots.
    """
    optimizer_order = ["PostgreSQL", "NEO", "BAO", "LOGER", "FASTgres", "LERO"]
    color_map = {
        "Nested Loop": "#ff9999",
        "Hash Join": "#66b3ff",
        "Merge Join": "#99ff99"
    }
    os.makedirs(save_dir, exist_ok=True)
            
    total_times = defaultdict(lambda: defaultdict(float))
    
    # This loop sums the execution times for each optimizer at each join level.
    for (join_level, query_name), optimizers_data in execution_times.items():
        for optimizer, time in optimizers_data.items():
            if time > 0: # Only count valid execution times
                total_times[join_level][optimizer] += time

    # Generate a plot for each join level
    for join_level_str in sorted(operator_counts.keys()):
        join_level = int(join_level_str)
        optimizers_data = operator_counts[join_level_str]
        
        ordered_optimizers = [opt for opt in optimizer_order if opt in optimizers_data]
        num_optimizers = len(ordered_optimizers)
        
        if num_optimizers == 0:
            continue

        fig, axes = plt.subplots(1, num_optimizers, figsize=(5 * num_optimizers, 6))
        if num_optimizers == 1:
            axes = [axes]
        
        for ax, optimizer in zip(axes, ordered_optimizers):
            join_counts = optimizers_data.get(optimizer, {})
            categories = ["Nested Loop", "Hash Join", "Merge Join"]
            sizes = [join_counts.get(cat, 0) for cat in categories]
            
            # Filter out zero-sized slices for a cleaner pie chart
            non_zero_indices = [i for i, size in enumerate(sizes) if size > 0]
            
            if not non_zero_indices:
                ax.text(0.5, 0.5, "No Joins\nFound", ha='center', va='center', fontsize=12)
                ax.set_title(optimizer, fontsize=16, pad=20)
                ax.set_aspect('equal')
                continue

            pie_labels = [categories[i] for i in non_zero_indices]
            pie_sizes = [sizes[i] for i in non_zero_indices]
            pie_colors = [color_map.get(label) for label in pie_labels]
            
            ax.pie(pie_sizes, labels=pie_labels, colors=pie_colors, autopct='%1.1f%%', 
                   startangle=90, textprops={'fontsize': 14})
            ax.set_aspect('equal')
            
            # Add speedup information below the title
            title_str = f"{optimizer}"
            if optimizer != "PostgreSQL":
                # Use the 'total_times' dictionary for the speedup calculation
                pg_time = total_times.get(join_level, {}).get("PostgreSQL", 0)
                lqo_time = total_times.get(join_level, {}).get(optimizer, 0)
                if pg_time > 0 and lqo_time > 0:
                    speedup = pg_time / lqo_time
                    title_str += f"\nSpeedup: {speedup:.2f}x"
                else:
                    title_str += f"\nSpeedup: N/A"
            
            ax.set_title(title_str, fontsize=16, pad=20)
        
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        fig.suptitle(f"Join Operator Distribution for {join_level}-Join Queries", fontsize=18)
        
        plot_path = os.path.join(save_dir, f"join_level_{join_level}.png")
        print(f"Saving plot: {plot_path}")
        plt.savefig(plot_path, bbox_inches='tight', dpi=300)
        plt.close(fig) # Close the figure to prevent display and free memory

def plot_prediction_vs_actual(df, save_dir="plots/4.3/"):
    """
    Creates a grid of plots for each join level, comparing predicted and actual latencies.
    Each plot contains subplots for hashjoin, mergejoin, and nestloop.
    
    Args:
        df (pd.DataFrame): The DataFrame generated by collect_prediction_vs_actual_metrics.
        save_dir (str): Directory to save the output plots.
    """
    if df.empty:
        print("DataFrame is empty. No data to plot.")
        return

    os.makedirs(save_dir, exist_ok=True)

    # Define consistent color mapping for clarity
    color_map = {
        'Postgres': '#1f77b4', # Blue
        'BAO': '#d62728',      # Red
        'LOGER': '#2ca02c',    # Green
        'NEO': '#ff7f0e'       # Orange
    }

    join_levels = sorted(df['join_level'].unique())
    
    for join_level in join_levels:
        level_data = df[df['join_level'] == join_level].copy()

        fig, axes = plt.subplots(1, 3, figsize=(20, 6), sharey=True)
        fig.tight_layout(pad=1.5)

        for j, join_type in enumerate(['hashjoin', 'mergejoin', 'nestloop']):
            ax = axes[j]
            type_data = level_data[level_data['join_type'] == join_type]
            # Sort by Postgres latency to create a smooth baseline curve
            type_data = type_data.sort_values('latency').reset_index(drop=True)
            
            if type_data.empty:
                ax.text(0.5, 0.5, "No Data", ha='center', va='center')
                ax.set_title(f'{join_type.capitalize()}', fontsize=16)
                continue

            # Plot actual runtimes (lines)
            ax.plot(type_data.index, type_data['latency'], marker='o', markersize=4, linestyle='-', 
                    color=color_map['Postgres'], label='Postgres Runtime')
            ax.plot(type_data.index, type_data['bao_latency'], marker='s', markersize=4, linestyle='--', 
                    color=color_map['BAO'], label='BAO Runtime')
            ax.plot(type_data.index, type_data['loger_latency'], marker='^', markersize=4, linestyle='--', 
                    color=color_map['LOGER'], label='LOGER Runtime')

            # Plot predicted latencies (scatter 'x' markers)
            ax.scatter(type_data.index, type_data['bao_prediction'], 
                       color=color_map['BAO'], marker='x', s=100, label='BAO Prediction')
            ax.scatter(type_data.index, type_data['loger_prediction'], 
                       color=color_map['LOGER'], marker='x', s=100, label='LOGER Prediction')
            ax.scatter(type_data.index, type_data['neo_prediction'], 
                       color=color_map['NEO'], marker='x', s=100, label='NEO Prediction')

            ax.set_yscale('log')
            ax.grid(True, which="both", ls="--", alpha=0.6)
            ax.set_xticklabels([]) # X-axis is just an index, no meaningful labels
            ax.set_xlabel('')
            ax.set_title(f'{join_type.capitalize()}', fontsize=16)

            if j == 0:
                ax.set_ylabel('')
        
        # Create a single, clean legend for the entire figure
        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 1.10), ncol=4, fontsize=12)
        
        plot_path = os.path.join(save_dir, f"join_level_{join_level}_predictions.png")
        print(f"Saving plot: {plot_path}")
        plt.savefig(plot_path, dpi=600, bbox_inches='tight')
        plt.show()

def plot_q_errors_line(q_errors, save_dir="plots/4.4/"):
    """Plots average Q-errors by join level as a line graph."""
    os.makedirs(save_dir, exist_ok=True)
    
    join_levels = sorted(q_errors.keys())
    optimizers = ['BAO', 'LOGER', 'NEO']
    
    # Define highly distinguishable line styles and markers
    styles = {
        'BAO': {'marker': 'o', 'linestyle': '-', 'linewidth': 2.5, 'markersize': 8},
        'LOGER': {'marker': 's', 'linestyle': '--', 'linewidth': 2.0, 'markersize': 7},
        'NEO': {'marker': '^', 'linestyle': ':', 'linewidth': 2.0, 'markersize': 7},
    }
    
    avg_q_errors = {
        opt: [np.mean(q_errors[level].get(opt, [0])) for level in join_levels]
        for opt in optimizers
    }
    
    plt.figure(figsize=(12, 7))
    for opt in optimizers:
        plt.plot(join_levels, avg_q_errors[opt], label=f'{opt}', **styles[opt])
        
    plt.xlabel('Number of Joins', fontsize=14)
    plt.ylabel('Average Q-Error (log scale)', fontsize=14)
    plt.xticks(join_levels, fontsize=12)
    plt.yticks(fontsize=12)
    plt.yscale('log')
    plt.grid(True, which="both", ls="--", alpha=0.6)
    plt.legend(fontsize=12, framealpha=1)
    plt.title('Latency Prediction Q-Error vs. Number of Joins', fontsize=16)
    
    plot_path = os.path.join(save_dir, 'q_errors_avg_line_graph.png')
    print(f"Saving line plot: {plot_path}")
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_q_errors_bar(q_errors, save_dir="plots/4.4/"):
    """Plots average Q-errors by join level as a grouped bar chart."""
    os.makedirs(save_dir, exist_ok=True)

    join_levels = sorted(q_errors.keys())
    optimizers = ['BAO', 'LOGER', 'NEO']
    
    avg_q_errors = {
        opt: [np.mean(q_errors[level].get(opt, [0])) for level in join_levels]
        for opt in optimizers
    }
    
    plt.figure(figsize=(16, 8))
    bar_width = 0.25
    x_pos = np.arange(len(join_levels))
    
    styles = {
        'BAO': {'color': '#1f77b4', 'hatch': '////', 'edgecolor': 'black'},
        'LOGER': {'color': '#ff7f0e', 'hatch': '....', 'edgecolor': 'black'},
        'NEO': {'color': '#2ca02c', 'hatch': 'xxxx', 'edgecolor': 'black'}
    }
    
    plt.bar(x_pos - bar_width, avg_q_errors['BAO'], width=bar_width, label='BAO', **styles['BAO'])
    plt.bar(x_pos, avg_q_errors['LOGER'], width=bar_width, label='LOGER', **styles['LOGER'])
    plt.bar(x_pos + bar_width, avg_q_errors['NEO'], width=bar_width, label='NEO', **styles['NEO'])
    
    plt.xlabel('Number of Joins', fontsize=14)
    plt.ylabel('Average Q-Error (log scale)', fontsize=14)
    plt.xticks(x_pos, join_levels, fontsize=12)
    plt.yscale('log')
    plt.grid(True, which="both", ls="--", axis='y', alpha=0.5)
    plt.legend(fontsize=12, framealpha=0.9)
    plt.title('Latency Prediction Q-Error vs. Number of Joins', fontsize=16)

    # Perfect prediction line for reference
    plt.axhline(y=1, color='red', linestyle='--', alpha=0.7, linewidth=1.5)
    plt.text(len(join_levels)-0.5, 1.2, 'Perfect Prediction (Q-Error=1)', 
             color='red', ha='right', va='center', fontsize=10)
    
    plot_path = os.path.join(save_dir, 'q_errors_bar_graph.png')
    print(f"Saving bar plot: {plot_path}")
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_divergence_jointplot(df, output_dir):
    """
    Creates an advanced jointplot showing the divergence scatter plot
    along with histograms for each axis.
    """
    if df.empty:
        print("Cannot plot, DataFrame is empty.")
        return

    # Prepare data for plotting
    df_plot = df.copy()
    df_plot['speedup_log2'] = np.log2(df_plot['speedup_factor'])
    df_plot['Result'] = np.where(df_plot['speedup_log2'] >= 0, 'Speedup', 'Slowdown')

    # Create the joint plot
    g = sns.jointplot(
        data=df_plot,
        x='embedding_distance',
        y='speedup_log2',
        hue='Result',
        palette={'Speedup': '#2ca02c', 'Slowdown': '#d62728'},
        height=10,
        alpha=0.7,
        edgecolor='w',
        s=50
    )

    # --- Customize the main scatter plot ---
    ax = g.ax_joint
    ax.axhline(0, color='black', linestyle='--', linewidth=1.5, alpha=0.8)
    median_dist = df_plot['embedding_distance'].median()
    ax.axvline(median_dist, color='blue', linestyle=':', linewidth=2, alpha=0.8)

    # Re-create legend for main plot
    handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10) for color in ['#2ca02c', '#d62728']]
    labels = ['Speedup (≥ 1x)', 'Slowdown (< 1x)']
    ax.legend(handles, labels, title='Result', fontsize=12)

    # Customize axis labels and title
    g.fig.suptitle('BAO Embedding Divergence vs. Performance Speedup', fontsize=22, y=1.02)
    ax.set_xlabel('Cosine Distance (BAO vs. PG Plan Embedding)\n(Higher value -> More Different)', fontsize=16)
    ax.set_ylabel('Speedup Factor (log₂ scale)', fontsize=16)

    # Format y-axis ticks
    y_ticks_log = ax.get_yticks()
    ax.set_yticklabels([f'{2**val:.2g}x' for val in y_ticks_log])
    ax.tick_params(axis='both', labelsize=12)
    
    # Save the plot
    os.makedirs(output_dir, exist_ok=True)
    plot_path = os.path.join(output_dir, "embedding_divergence_jointplot.png")
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"Analysis plot saved to: {plot_path}")
    
    plt.show()

def plot_divergence_jointplots(df, output_dir):
    """
    Creates and saves a jointplot for each optimizer in the dataframe.
    """
    if df.empty:
        print("Cannot plot, DataFrame is empty.")
        return

    os.makedirs(output_dir, exist_ok=True)
    
    optimizers = df['optimizer'].unique()
    n_opts = len(optimizers)
    fig, axes = plt.subplots(1, n_opts, figsize=(6 * n_opts, 6), sharey=True)

    # If only one optimizer, axes won't be iterable
    if n_opts == 1:
        axes = [axes]

    for ax, opt in zip(axes, optimizers):
        df_opt = df[df['optimizer'] == opt].copy()
        
        if df_opt.empty:
            print(f"No data for optimizer '{opt}', skipping...")
            continue
        
        df_opt['speedup_log2'] = np.log2(df_opt['speedup_factor'])
        df_opt['Result'] = np.where(df_opt['speedup_log2'] >= 0, 'Speedup', 'Slowdown')

        # Scatter plot (replicates jointplot style)
        sns.scatterplot(
            data=df_opt,
            x='embedding_distance',
            y='speedup_log2',
            hue='Result',
            palette={'Speedup': '#2ca02c', 'Slowdown': '#d62728'},
            alpha=0.7,
            edgecolor='w',
            s=50,
            ax=ax,
            legend=False
        )

        ax.axhline(0, color='black', linestyle='--', linewidth=1.5, alpha=0.8)
        median_dist = df_opt['embedding_distance'].median()
        ax.axvline(median_dist, color='blue', linestyle=':', linewidth=2, alpha=0.8)

        # Re-create legend
        handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10) 
                   for color in ['#2ca02c', '#d62728']]
        labels = ['Speedup (≥ 1x)', 'Slowdown (< 1x)']
        ax.legend(handles, labels, title='Result', fontsize=12)

        # g.fig.suptitle(f'{opt} Embedding Divergence vs. Performance Speedup', fontsize=22, y=1.02)
        ax.set_xlabel('', fontsize=16)
        ax.set_ylabel('', fontsize=16)

        # Format y-axis ticks
        y_ticks_log = ax.get_yticks()
        ax.set_yticklabels([f'{2**val:.2g}x' for val in y_ticks_log])
        ax.tick_params(axis='both', labelsize=12)

        plt.tight_layout()
        handles = [plt.Line2D([0], [0], marker='o', color='w',
                          markerfacecolor=color, markersize=10)
               for color in ['#2ca02c', '#d62728']]
        labels = ['Speedup (≥ 1x)', 'Slowdown (< 1x)']    
        ax.legend(handles, labels, title='Result', fontsize=12, loc='upper right')
            
        # Save the plot
        plt.tight_layout()
        plot_path = os.path.join(output_dir, f"embedding_divergence_jointplot_{opt}.png")
        plt.savefig(plot_path, dpi=600, bbox_inches='tight')
        print(f"Analysis plot for '{opt}' saved to: {plot_path}")

        # Show the plot
        # plt.show()
        # plt.close(g.fig)  # Close the figure to avoid overlapping plots

import os
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter


def plot_divergence_row(df, output_dir):
    """
    Creates and saves a single figure with all optimizers shown side-by-side
    in one row. Uses FuncFormatter for stable y-axis labels.
    """
    if df.empty:
        print("Cannot plot, DataFrame is empty.")
        return

    os.makedirs(output_dir, exist_ok=True)

    # Prepare columns
    df['speedup_log2'] = np.log2(df['speedup_factor'])
    df['Result'] = np.where(df['speedup_log2'] >= 0, 'Speedup', 'Slowdown')

    optimizers = df['optimizer'].unique()
    n_opts = len(optimizers)

    fig, axes = plt.subplots(1, n_opts, figsize=(6 * n_opts, 6), sharey=True)

    if n_opts == 1:  # single case
        axes = [axes]

    # Define formatter: map log2(speedup) -> "factor x"
    def log2_to_factor(x, pos):
        return f"{2**x:.2g}x"

    for ax, opt in zip(axes, optimizers):
        df_opt = df[df['optimizer'] == opt].copy()
        if df_opt.empty:
            print(f"No data for optimizer '{opt}', skipping...")
            continue

        # Scatter plot
        sns.scatterplot(
            data=df_opt,
            x='embedding_distance',
            y='speedup_log2',
            hue='Result',
            palette={'Speedup': '#2ca02c', 'Slowdown': '#d62728'},
            alpha=0.7,
            edgecolor='w',
            s=50,
            ax=ax,
            legend=False
        )

        # Reference lines
        ax.axhline(0, color='black', linestyle='--', linewidth=1.5, alpha=0.8)
        median_dist = df_opt['embedding_distance'].median()
        ax.axvline(median_dist, color='blue', linestyle=':', linewidth=2, alpha=0.8)

        # Labels
        ax.set_title(opt, fontsize=18)
        ax.set_xlabel('', fontsize=16)
        ax.set_ylabel('', fontsize=16)

        # Apply custom y-axis formatter
        ax.yaxis.set_major_formatter(FuncFormatter(log2_to_factor))
        ax.tick_params(axis='both', labelsize=12)

    # Shared legend
    handles = [plt.Line2D([0], [0], marker='o', color='w',
                          markerfacecolor=color, markersize=10)
               for color in ['#2ca02c', '#d62728']]
    labels = ['Speedup (≥ 1x)', 'Slowdown (< 1x)']
    fig.legend(handles, labels, title='Result', fontsize=12, loc='upper right')

    plt.tight_layout()

    # Save
    plot_path = os.path.join(output_dir, "embedding_divergence_row.png")
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"Row analysis plot saved to: {plot_path}")

    plt.show()

import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter

def plot_divergence_joint_row(df, output_dir):
    """
    Creates and saves a single figure with joint plots for each optimizer,
    showing the scatter plot and marginal distributions.
    """
    if df.empty:
        print("Cannot plot, DataFrame is empty.")
        return

    os.makedirs(output_dir, exist_ok=True)

    # Prepare columns
    df['speedup_log2'] = np.log2(df['speedup_factor'])
    df['Result'] = np.where(df['speedup_log2'] >= 0, 'Speedup', 'Slowdown')
    
    palette = {'Speedup': '#2ca02c', 'Slowdown': '#d62728'}

    optimizers = df['optimizer'].unique()
    n_opts = len(optimizers)

    # Create the figure and a main gridspec
    fig = plt.figure(figsize=(8 * n_opts, 8))
    outer_gs = gridspec.GridSpec(1, n_opts, figure=fig)

    # To share the Y axis across all main plots
    ax_scatter_0 = None 

    # Define formatter: map log2(speedup) -> "factor x"
    def log2_to_factor(x, pos):
        return f"{2**x:.2g}x"
    formatter = FuncFormatter(log2_to_factor)

    for i, opt in enumerate(optimizers):
        # Create a nested GridSpec for each optimizer's joint plot
        inner_gs = gridspec.GridSpecFromSubplotSpec(
            2, 2, subplot_spec=outer_gs[i], 
            width_ratios=[4, 1], height_ratios=[1, 4],
            wspace=0.05, hspace=0.05
        )

        df_opt = df[df['optimizer'] == opt].copy()
        if df_opt.empty:
            print(f"No data for optimizer '{opt}', skipping...")
            continue

        # --- Create axes for the joint plot ---
        # Main scatter plot
        if i == 0:
            ax_scatter = fig.add_subplot(inner_gs[1, 0])
            ax_scatter_0 = ax_scatter # Save the first plot's axis
        else:
            # Share Y-axis with the first plot
            ax_scatter = fig.add_subplot(inner_gs[1, 0], sharey=ax_scatter_0)
            plt.setp(ax_scatter.get_yticklabels(), visible=False) # Hide y-labels for subsequent plots
            ax_scatter.set_ylabel('')

        # Top histogram for X distribution
        ax_histx = fig.add_subplot(inner_gs[0, 0], sharex=ax_scatter)
        # Right histogram for Y distribution
        ax_histy = fig.add_subplot(inner_gs[1, 1], sharey=ax_scatter)

        # --- Plotting ---
        # Scatter plot (main plot)
        sns.scatterplot(
            data=df_opt, x='embedding_distance', y='speedup_log2',
            hue='Result', palette=palette, alpha=0.7, edgecolor='w',
            s=60, ax=ax_scatter, legend=False
        )

        # Top histogram
        sns.histplot(
            data=df_opt, x='embedding_distance', hue='Result', 
            palette=palette, ax=ax_histx, legend=False,
            multiple="stack", bins=20
        )

        # Right histogram
        sns.histplot(
            data=df_opt, y='speedup_log2', hue='Result',
            palette=palette, ax=ax_histy, legend=False,
            multiple="stack", bins=20
        )
        
        # --- Formatting ---
        # Cleanup marginal plot axes
        plt.setp(ax_histx.get_xticklabels(), visible=False)
        plt.setp(ax_histy.get_yticklabels(), visible=False)
        ax_histx.set_ylabel('')
        ax_histy.set_xlabel('')

        # Set title on the top-most axis
        ax_histx.set_title(opt, fontsize=18, pad=10)

        # Labels for the main scatter plot
        ax_scatter.set_xlabel('Embedding Distance', fontsize=16)
        if i == 0: # Only set Y label on the first plot
            ax_scatter.set_ylabel('Speedup Factor', fontsize=16)

        # Reference lines on the scatter plot
        ax_scatter.axhline(0, color='black', linestyle='--', linewidth=1.5, alpha=0.8)
        median_dist = df_opt['embedding_distance'].median()
        ax_scatter.axvline(median_dist, color='blue', linestyle=':', linewidth=2, alpha=0.8)
        
        # Add gridlines to the scatter plot
        ax_scatter.grid(True, linestyle='--', alpha=0.6)

        # Apply custom y-axis formatter and ticks
        ax_scatter.yaxis.set_major_formatter(formatter)
        ax_scatter.tick_params(axis='both', labelsize=12)
        ax_histx.tick_params(axis='y', labelsize=10)
        ax_histy.tick_params(axis='x', labelsize=10)

    # Shared legend for the entire figure
    handles = [plt.Line2D([0], [0], marker='o', color='w',
                          markerfacecolor=color, markersize=10)
               for color in ['#2ca02c', '#d62728']]
    labels = ['Speedup (≥ 1x)', 'Slowdown (< 1x)']
    fig.legend(handles, labels, title='Result', fontsize=12, loc='upper right', bbox_to_anchor=(0.99, 0.99))

    fig.suptitle('Speedup vs. Embedding Distance by Optimizer', fontsize=22, y=1.02)
    plt.tight_layout(rect=[0, 0, 1, 0.98]) # Adjust layout to make space for suptitle

    # Save
    plot_path = os.path.join(output_dir, "embedding_divergence_joint_row.png")
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"Joint analysis plot saved to: {plot_path}")

    plt.show()

from matplotlib.ticker import FuncFormatter, MaxNLocator # Import MaxNLocator
def plot_divergence_final(df, output_dir):
    """
    Creates final joint plots with KDEs, auto-ticks, and intelligent zooming
    based on data quantiles to focus on the dense data regions.
    """
    if df.empty:
        print("Cannot plot, DataFrame is empty.")
        return

    os.makedirs(output_dir, exist_ok=True)

    df['speedup_log2'] = np.log2(df['speedup_factor'])
    df['Result'] = np.where(df['speedup_log2'] >= 0, 'Speedup', 'Slowdown')
    
    palette = {'Speedup': '#2ca02c', 'Slowdown': '#d62728'}
    optimizers = df['optimizer'].unique()
    n_opts = len(optimizers)

    fig = plt.figure(figsize=(8 * n_opts, 8))
    outer_gs = gridspec.GridSpec(1, n_opts, figure=fig)
    ax_scatter_0 = None 

    def log2_to_factor(x, pos):
        # Handle potential edge cases where x might be very small
        val = 2**x
        if val < 0.001:
            return f"{val:.1e}x"
        return f"{val:.2g}x"
    formatter = FuncFormatter(log2_to_factor)

    for i, opt in enumerate(optimizers):
        inner_gs = gridspec.GridSpecFromSubplotSpec(
            2, 2, subplot_spec=outer_gs[i], 
            width_ratios=[4, 1], height_ratios=[1, 4],
            wspace=0.05, hspace=0.05
        )
        df_opt = df[df['optimizer'] == opt].copy()
        if df_opt.empty:
            continue

        if i == 0:
            ax_scatter = fig.add_subplot(inner_gs[1, 0])
            ax_scatter_0 = ax_scatter 
        else:
            ax_scatter = fig.add_subplot(inner_gs[1, 0], sharey=ax_scatter_0)
            plt.setp(ax_scatter.get_yticklabels(), visible=False)
            ax_scatter.set_ylabel('')

        ax_histx = fig.add_subplot(inner_gs[0, 0], sharex=ax_scatter)
        ax_histy = fig.add_subplot(inner_gs[1, 1], sharey=ax_scatter)

        sns.scatterplot(
            data=df_opt, x='embedding_distance', y='speedup_log2',
            hue='Result', palette=palette, alpha=0.7, edgecolor='w',
            s=60, ax=ax_scatter, legend=False
        )
        sns.kdeplot(
            data=df_opt, x='embedding_distance', hue='Result',
            palette=palette, ax=ax_histx, legend=False,
            multiple="stack", fill=True, alpha=0.7, clip_on=False
        )
        sns.kdeplot(
            data=df_opt, y='speedup_log2', hue='Result',
            palette=palette, ax=ax_histy, legend=False,
            multiple="stack", fill=True, alpha=0.7, clip_on=False
        )
        
        ax_histx.set_xlabel(''); ax_histx.set_ylabel('')
        ax_histy.set_xlabel(''); ax_histy.set_ylabel('')
        plt.setp(ax_histx.get_xticklabels(), visible=False)
        plt.setp(ax_histy.get_yticklabels(), visible=False)
        ax_histx.tick_params(left=False, labelleft=False)
        ax_histy.tick_params(bottom=False, labelbottom=False)
        
        ax_histx.set_title(opt, fontsize=18, pad=10)
        ax_scatter.set_xlabel('', fontsize=16)
        if i == 0:
            ax_scatter.set_ylabel('', fontsize=16)
            
        ax_scatter.axhline(0, color='black', linestyle='--', linewidth=1.5, alpha=0.8, zorder=0)
        median_dist = df_opt['embedding_distance'].median()
        ax_scatter.axvline(median_dist, color='blue', linestyle=':', linewidth=2, alpha=0.8, zorder=0)
        ax_scatter.grid(True, linestyle='--', alpha=0.6)
        
        # --- NEW: INTELLIGENT ZOOMING BASED ON DATA QUANTILES ---
        # Define the quantile range to focus on (e.g., from 2% to 98%)
        q_low = 0.02
        q_high = 0.98
        
        # Calculate the quantile-based limits for x and y axes
        xlims = df_opt['embedding_distance'].quantile([q_low, q_high])
        ylims = df_opt['speedup_log2'].quantile([q_low, q_high])

        # Calculate padding as a percentage of the quantile range
        x_padding = (xlims.iloc[1] - xlims.iloc[0]) * 0.1
        y_padding = (ylims.iloc[1] - ylims.iloc[0]) * 0.1

        # Set the final axis limits, ensuring they don't crash if range is zero
        ax_scatter.set_xlim(xlims.iloc[0] - x_padding, xlims.iloc[1] + x_padding)
        ax_scatter.set_ylim(ylims.iloc[0] - y_padding, ylims.iloc[1] + y_padding)
        
        ax_scatter.xaxis.set_major_locator(MaxNLocator(nbins=5, integer=False))
        ax_scatter.yaxis.set_major_locator(MaxNLocator(nbins=6, prune='both'))
        
        ax_scatter.yaxis.set_major_formatter(formatter)
        ax_scatter.tick_params(axis='both', labelsize=12)

    handles = [plt.Line2D([0], [0], marker='o', color='w',
                          markerfacecolor=color, markersize=10)
               for color in ['#2ca02c', '#d62728']]
    labels = ['Speedup (≥ 1x)', 'Slowdown (< 1x)']
    fig.legend(handles, labels, title='Result', fontsize=12, loc='upper right', bbox_to_anchor=(0.99, 0.99))

    fig.suptitle('', fontsize=22, y=1.02)
    plt.tight_layout(rect=[0, 0, 1, 0.98])

    plot_path = os.path.join(output_dir, "embedding_divergence_final.png")
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"Final analysis plot saved to: {plot_path}")
    plt.show()

def plot_divergence_scatter_row_zoomed(df, output_dir):
    """
    Creates and saves a single figure with intelligently zoomed scatter plots for
    each optimizer, arranged side-by-side in one row.
    """
    if df.empty:
        print("Cannot plot, DataFrame is empty.")
        return

    os.makedirs(output_dir, exist_ok=True)

    # --- Data Preparation ---
    df['speedup_log2'] = np.log2(df['speedup_factor'])
    df['Result'] = np.where(df['speedup_log2'] >= 0, 'Speedup', 'Slowdown')
    
    palette = {'Speedup': '#2ca02c', 'Slowdown': '#d62728'}
    optimizers = df['optimizer'].unique()
    n_opts = len(optimizers)

    # --- Plot Setup ---
    fig, axes = plt.subplots(1, n_opts, figsize=(6 * n_opts, 6), sharey=True)
    if n_opts == 1:
        axes = [axes] # Ensure axes is always a list to iterate over

    # --- Intelligent Zooming & Formatting ---
    # Define the quantile range to focus on (e.g., from 2% to 98%)
    q_low, q_high = 0.02, 0.98
    
    # Calculate GLOBAL y-limits for a consistent, shared y-axis zoom
    global_ylims = df['speedup_log2'].quantile([q_low, q_high])
    y_padding = (global_ylims.iloc[1] - global_ylims.iloc[0]) * 0.1
    final_ylim = (global_ylims.iloc[0] - y_padding, global_ylims.iloc[1] + y_padding)
    
    # Define y-axis formatter
    def log2_to_factor(x, pos):
        val = 2**x
        if val < 0.001: return f"{val:.1e}x"
        return f"{val:.2g}x"
    formatter = FuncFormatter(log2_to_factor)

    for ax, opt in zip(axes, optimizers):
        df_opt = df[df['optimizer'] == opt].copy()
        if df_opt.empty:
            continue

        # Scatter plot
        sns.scatterplot(
            data=df_opt, x='embedding_distance', y='speedup_log2',
            hue='Result', palette=palette, alpha=0.7, edgecolor='w',
            s=60, ax=ax, legend=False
        )

        # Calculate LOCAL x-limits for individual plot zooming
        xlims = df_opt['embedding_distance'].quantile([q_low, q_high])
        x_padding = (xlims.iloc[1] - xlims.iloc[0]) * 0.1
        ax.set_xlim(xlims.iloc[0] - x_padding, xlims.iloc[1] + x_padding)
        # Apply the global y-limit
        ax.set_ylim(final_ylim)

        # Reference lines and grid
        ax.axhline(0, color='black', linestyle='--', linewidth=1.5, alpha=0.8)
        median_dist = df_opt['embedding_distance'].median()
        ax.axvline(median_dist, color='blue', linestyle=':', linewidth=2, alpha=0.8)
        ax.grid(True, linestyle='--', alpha=0.6)

        # Labels, Title, and Ticks
        ax.set_title(opt, fontsize=18)
        ax.set_xlabel('', fontsize=16)
        ax.xaxis.set_major_locator(MaxNLocator(nbins=5, integer=False))
        ax.tick_params(axis='both', labelsize=12)

    # Set shared y-axis label and formatting only on the first plot
    axes[0].set_ylabel('', fontsize=16)
    axes[0].yaxis.set_major_locator(MaxNLocator(nbins=6, prune='both'))
    axes[0].yaxis.set_major_formatter(formatter)

    # --- Final Touches ---
    # Shared legend
    handles = [plt.Line2D([0], [0], marker='o', color='w',
                          markerfacecolor=color, markersize=10)
               for color in ['#2ca02c', '#d62728']]
    labels = ['Speedup (≥ 1x)', 'Slowdown (< 1x)']
    fig.legend(handles, labels, title='Result', fontsize=12, loc='upper right')

    plt.tight_layout()

    # Save
    plot_path = os.path.join(output_dir, "embedding_divergence_scatter_zoomed.png")
    plt.savefig(plot_path, dpi=600, bbox_inches='tight')
    print(f"Zoomed scatter plot saved to: {plot_path}")
    plt.show()


def plot_divergence_individual(df, output_dir):
    """
    Creates and saves separate figures for each optimizer instead of a row of subplots.
    """
    if df.empty:
        print("Cannot plot, DataFrame is empty.")
        return

    os.makedirs(output_dir, exist_ok=True)

    df['speedup_log2'] = np.log2(df['speedup_factor'])
    df['Result'] = np.where(df['speedup_log2'] >= 0, 'Speedup', 'Slowdown')

    optimizers = df['optimizer'].unique()

    for opt in optimizers:
        df_opt = df[df['optimizer'] == opt].copy()
        if df_opt.empty:
            print(f"No data for optimizer '{opt}', skipping...")
            continue

        # Create a fresh figure for this optimizer
        fig, ax = plt.subplots(figsize=(6, 6))

        # Scatter plot (replicates jointplot style)
        sns.scatterplot(
            data=df_opt,
            x='embedding_distance',
            y='speedup_log2',
            hue='Result',
            palette={'Speedup': '#2ca02c', 'Slowdown': '#d62728'},
            alpha=0.7,
            edgecolor='w',
            s=50,
            ax=ax,
            legend=False
        )

        # Reference lines
        ax.axhline(0, color='black', linestyle='--', linewidth=1.5, alpha=0.8)
        median_dist = df_opt['embedding_distance'].median()
        ax.axvline(median_dist, color='blue', linestyle=':', linewidth=2, alpha=0.8)

        # Labels
        ax.set_title(opt, fontsize=18)
        ax.set_xlabel('Cosine Distance', fontsize=16)
        ax.set_ylabel('Speedup Factor (log₂ scale)', fontsize=16)

        # Format y-axis ticks
        y_ticks_log = ax.get_yticks()
        ax.set_yticklabels([f'{2**val:.2g}x' for val in y_ticks_log])
        ax.tick_params(axis='both', labelsize=12)

        # Add legend per plot
        handles = [plt.Line2D([0], [0], marker='o', color='w',
                              markerfacecolor=color, markersize=10)
                   for color in ['#2ca02c', '#d62728']]
        labels = ['Speedup (≥ 1x)', 'Slowdown (< 1x)']
        ax.legend(handles, labels, title='Result', fontsize=12, loc='upper right')

        # Save individual plot
        plot_path = os.path.join(output_dir, f"embedding_divergence_{opt}.png")
        plt.savefig(plot_path, dpi=600, bbox_inches='tight')
        plt.close(fig)  # Close to free memory

        print(f"Plot saved for optimizer '{opt}' at: {plot_path}")

def plot_all_divergence_analyses(df, output_dir):
    """
    Creates a grid of divergence plots, one for each optimizer.
    """
    if df.empty:
        print("Cannot plot, DataFrame is empty.")
        return

    # Prepare data for plotting
    df_plot = df.copy()
    df_plot = df_plot[df_plot['speedup_factor'] >= 0.062].reset_index(drop=True)
    df_plot['speedup_log2'] = np.log2(df_plot['speedup_factor'])
    df_plot['Result'] = np.where(df_plot['speedup_log2'] >= 0, 'Speedup', 'Slowdown')

    # Get the list of optimizers that have data
    optimizers_with_data = df_plot['optimizer'].unique()
    
    # Create a grid of subplots
    num_optimizers = len(optimizers_with_data)
    cols = 2  # Adjust as needed
    rows = (num_optimizers + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(12 * cols, 9 * rows), squeeze=False)
    axes = axes.flatten()

    for i, optimizer_name in enumerate(optimizers_with_data):
        ax = axes[i]
        subset_df = df_plot[df_plot['optimizer'] == optimizer_name]
        
        # Handle FASTgres special case where distance is always 0
        if subset_df['embedding_distance'].nunique() <= 1:
            sns.stripplot(ax=ax, data=subset_df, x='embedding_distance', y='speedup_log2',
                          hue='Result', palette={'Speedup': '#2ca02c', 'Slowdown': '#d62728'},
                          jitter=0.02, alpha=0.7, s=6)
        else:
            sns.scatterplot(ax=ax, data=subset_df, x='embedding_distance', y='speedup_log2',
                            hue='Result', palette={'Speedup': '#2ca02c', 'Slowdown': '#d62728'},
                            alpha=0.6, edgecolor='k', linewidth=0.5, s=60)
        
        # Formatting and Labels
        ax.set_title(f"{optimizer_name} Divergence vs. Speedup", fontsize=18, pad=15)
        ax.set_xlabel('Cosine Distance', fontsize=14)
        ax.set_ylabel('Speedup Factor (log₂ scale)', fontsize=14)
        
        # --- Marginal histograms ---
        # Top histogram (embedding_distance)
        ax_histx = ax.inset_axes([0, 1.05, 1, 0.2])  
        sns.histplot(subset_df['embedding_distance'], ax=ax_histx, bins=20, color="gray", alpha=0.6)
        ax_histx.set_xticks([])
        ax_histx.set_yticks([])

        # Right histogram (speedup_log2)
        ax_histy = ax.inset_axes([1.05, 0, 0.2, 1])  
        sns.histplot(subset_df['speedup_log2'], ax=ax_histy, bins=20, color="gray", alpha=0.6, orientation="horizontal")
        ax_histy.set_xticks([])
        ax_histy.set_yticks([])
        
        # Reference lines
        ax.axhline(0, color='black', linestyle='--', linewidth=1)
        median_dist = subset_df['embedding_distance'].median()
        ax.axvline(median_dist, color='blue', linestyle=':', linewidth=1.5, label=f'Median Dist ({median_dist:.2f})')

        # Y-axis ticks
        y_ticks_log = ax.get_yticks()
        ax.set_yticklabels([f'{2**val:.2g}x' for val in y_ticks_log])
        
        ax.legend(title='Result')

    # Hide any unused subplots
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    fig.tight_layout(pad=3.0)
    
    # Save the combined plot
    os.makedirs(output_dir, exist_ok=True)
    plot_path = os.path.join(output_dir, "all_optimizers_divergence_vs_speedup.png")
    plt.savefig(plot_path, dpi=300)
    print(f"\nCombined analysis plot saved to: {plot_path}")
    plt.show()

import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import os
from matplotlib.ticker import FuncFormatter

def plot_divergence_per_optimizer(df, output_dir):
    """
    Creates one clean divergence plot per optimizer with marginal histograms.
    """
    if df.empty:
        print("Cannot plot, DataFrame is empty.")
        return

    # Preprocess
    df_plot = df.copy()
    df_plot = df_plot[df_plot['speedup_factor'] >= 0.062].reset_index(drop=True)
    df_plot['speedup_log2'] = np.log2(df_plot['speedup_factor'])
    df_plot['Result'] = np.where(df_plot['speedup_log2'] >= 0, 'Speedup', 'Slowdown')

    os.makedirs(output_dir, exist_ok=True)

    # Custom formatter for speedup axis
    fmt_speedup = FuncFormatter(lambda v, pos: f'{2**v:.2g}x')

    for optimizer_name, subset_df in df_plot.groupby("optimizer"):
        g = sns.JointGrid(
            data=subset_df,
            x="embedding_distance", y="speedup_log2",
            height=8
        )

        # Main scatter
        sns.scatterplot(
            data=subset_df, x="embedding_distance", y="speedup_log2",
            hue="Result", palette={"Speedup": "#2ca02c", "Slowdown": "#d62728"},
            alpha=0.7, edgecolor="k", linewidth=0.4, s=60,
            ax=g.ax_joint
        )

        # Histograms
        sns.histplot(subset_df["embedding_distance"], bins=25, ax=g.ax_marg_x, color="gray", alpha=0.6)
        sns.histplot(subset_df["speedup_log2"], bins=25, ax=g.ax_marg_y, color="gray", alpha=0.6, orientation="horizontal")

        # Formatting
        g.ax_joint.set_title(f"{optimizer_name} Divergence vs. Speedup", fontsize=18, pad=15)
        g.ax_joint.set_xlabel("Cosine Distance", fontsize=14)
        g.ax_joint.set_ylabel("Speedup Factor", fontsize=14)
        g.ax_joint.yaxis.set_major_formatter(fmt_speedup)

        # Reference lines
        g.ax_joint.axhline(0, color="black", linestyle="--", linewidth=1)
        median_dist = subset_df["embedding_distance"].median()
        g.ax_joint.axvline(median_dist, color="blue", linestyle=":", linewidth=1.5,
                           label=f"Median Dist ({median_dist:.2f})")

        g.ax_joint.legend(title="Result")

        # Save per optimizer
        plot_path = os.path.join(output_dir, f"{optimizer_name}_divergence_vs_speedup.png")
        plt.savefig(plot_path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Saved: {plot_path}")

def plot_tsne_analysis(df, output_dir):
    """
    Creates a grid of t-SNE plots, one for each optimizer, to visualize the embedding space.
    """
    if df.empty:
        print("Cannot plot t-SNE: DataFrame is empty.")
        return
        
    optimizers_with_data = df['optimizer'].unique()
    num_optimizers = len(optimizers_with_data)
    cols = 2
    rows = (num_optimizers + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(11 * cols, 9 * rows), squeeze=False)
    axes = axes.flatten()

    for i, optimizer_name in enumerate(optimizers_with_data):
        ax = axes[i]
        subset_df = df[df['optimizer'] == optimizer_name]
        
        # Define color and style for the plot
        palette = {optimizer_name: '#007ACC', 'PostgreSQL': '#FF4500'} # Blue for optimizer, Orange for PG
        markers = {optimizer_name: 'o', 'PostgreSQL': 'X'}
        
        sns.scatterplot(
            ax=ax,
            data=subset_df,
            x='tsne_x',
            y='tsne_y',
            hue='plan_type',
            style='plan_type',
            palette=palette,
            markers=markers,
            s=80, # Marker size
            alpha=0.8,
            edgecolor='k',
            linewidth=0.5
        )
        
        ax.set_title(f't-SNE Visualization of {optimizer_name} Embedding Space', fontsize=18, pad=15)
        ax.set_xlabel('t-SNE Component 1', fontsize=14)
        ax.set_ylabel('t-SNE Component 2', fontsize=14)
        ax.legend(title='Plan Origin', fontsize=12)

    # Hide any unused subplots
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    fig.tight_layout(pad=3.0)
    
    os.makedirs(output_dir, exist_ok=True)
    plot_path = os.path.join(output_dir, "all_optimizers_tsne_visualization.png")
    plt.savefig(plot_path, dpi=300)
    print(f"\nCombined t-SNE plot saved to: {plot_path}")
    plt.show()

def plot_tsne_analysis_per_optimizer(df, output_dir):
    """
    Creates and saves a separate t-SNE scatter plot for each optimizer.
    """
    if df.empty:
        print("Cannot plot t-SNE: DataFrame is empty.")
        return

    os.makedirs(output_dir, exist_ok=True)
    
    optimizers = df['optimizer'].unique()
    
    for opt in optimizers:
        df_opt = df[df['optimizer'] == opt].copy()
        
        if df_opt.empty:
            print(f"No data for optimizer '{opt}', skipping...")
            continue
        
        # Define color/style (PG vs this optimizer)
        palette = {opt: '#007ACC', 'PostgreSQL': '#FF4500'}
        markers = {opt: 'o', 'PostgreSQL': 'X'}

        plt.figure(figsize=(9, 8))
        sns.scatterplot(
            data=df_opt,
            x='tsne_x',
            y='tsne_y',
            hue='plan_type',
            style='plan_type',
            palette=palette,
            markers=markers,
            s=80,
            alpha=0.8,
            edgecolor='k',
            linewidth=0.5
        )
        
        plt.xlabel('t-SNE Component 1', fontsize=14)
        plt.ylabel('t-SNE Component 2', fontsize=14)
        plt.legend(title='Plan Origin', fontsize=12)
        
        plot_path = os.path.join(output_dir, f"tsne_visualization_{opt}.png")
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        print(f"t-SNE plot for '{opt}' saved to: {plot_path}")
        plt.close()