import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from typing import List, Any, Dict
from core.config import VIZ_SETTINGS

class DataQualityVisualizer:
    """
    Utility class to generate quality visualization charts.
    """
    
    def __init__(self, output_dir: str = "audit_reports"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        sns.set_theme(style="whitegrid", palette=VIZ_SETTINGS['palette'])

    def generate_null_heatmap(self, df: pd.DataFrame, file_id: str):
        """Generates a heatmap showing null density across the file."""
        plt.figure(figsize=VIZ_SETTINGS['figure_size'])
        sns.heatmap(df.isnull(), yticklabels=False, cbar=False, cmap='viridis')
        plt.title(f"Null Density Heatmap: {file_id}")
        
        path = os.path.join(self.output_dir, f"{file_id}_null_heatmap.png")
        plt.savefig(path, dpi=VIZ_SETTINGS['dpi'])
        plt.close()
        return path

    def generate_type_distribution(self, profile_list: List[Any], file_id: str):
        """Generates a pie chart of detected column types."""
        types = [p.detected_type for p in profile_list]
        type_counts = pd.Series(types).value_counts()
        
        plt.figure(figsize=(8, 8))
        type_counts.plot.pie(autopct='%1.1f%%', startangle=140, cmap='Pastel1')
        plt.title(f"Column Type Distribution: {file_id}")
        plt.ylabel('')
        
        path = os.path.join(self.output_dir, f"{file_id}_type_dist.png")
        plt.savefig(path, dpi=VIZ_SETTINGS['dpi'])
        plt.close()
        return path

    def generate_quality_summary_chart(self, summary_metrics: Dict[str, float], file_id: str):
        """Generates a bar chart of high-level quality scores."""
        metrics = pd.Series(summary_metrics)
        
        plt.figure(figsize=VIZ_SETTINGS['figure_size'])
        ax = metrics.plot(kind='bar', color=['#4CAF50', '#2196F3', '#FFC107'])
        plt.ylim(0, 105)
        plt.title(f"Quality Score Summary: {file_id}")
        plt.ylabel("Score (%)")
        
        # Add labels on top
        for p in ax.patches:
            ax.annotate(f"{p.get_height()}%", (p.get_x() + p.get_width() / 2., p.get_height()), 
                        ha='center', va='center', xytext=(0, 10), textcoords='offset points')
        
        path = os.path.join(self.output_dir, f"{file_id}_quality_scores.png")
        plt.savefig(path, dpi=VIZ_SETTINGS['dpi'])
        plt.close()
        return path
