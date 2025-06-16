import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import os
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

import matplotlib.pyplot as plt
import seaborn as sns
from tabulate import tabulate

import base64
from io import BytesIO

import json
import webbrowser

class StockPerformanceAnalyzer:
    def __init__(self, db_path: str = r"C:\Projects\apps\institutional_flow_quant\data\quant_strategy.db"):
        """
        Initialize the Stock Performance Analyzer
        
        Args:
            db_path: Path to the SQLite database containing stock prices
        """
        self.db_path = db_path
        self.base_html_path = r"C:\Projects\apps\institutional_flow_quant\output\progressive_analysis"
        self.output_html_path = r"C:\Projects\apps\institutional_flow_quant\output\progressive_analysis"

    def format_price(self, price) -> str:
        """Format price values for display"""
        if price is None or pd.isna(price):
            return "N/A"
        return f"₹{price:.2f}"


    def format_percentage(self, percentage) -> str:
        """Format percentage values for display"""
        if percentage is None or pd.isna(percentage):
            return "N/A"
        return f"{percentage:.2f}%"


    def get_return_class(self, return_value) -> str:
        """Get CSS class based on return value"""
        if return_value is None or pd.isna(return_value):
            return "neutral"
        return "positive" if return_value >= 0 else "negative"


    def get_performance_badge(self, return_value) -> str:
        """Get performance badge based on return value"""
        if return_value is None or pd.isna(return_value):
            return '<span class="badge neutral">N/A</span>'

        if return_value >= 15:
            return '<span class="badge excellent">🚀 Excellent</span>'
        elif return_value >= 8:
            return '<span class="badge very-good">⭐ Very Good</span>'
        elif return_value >= 2:
            return '<span class="badge good">✅ Good</span>'
        elif return_value > 0:
            return '<span class="badge slight-positive">📈 Slight Gain</span>'
        elif return_value == 0:
            return '<span class="badge neutral">➖ Flat</span>'
        elif return_value >= -2:
            return '<span class="badge slight-negative">📉 Slight Loss</span>'
        elif return_value >= -5:
            return '<span class="badge poor">⚠️ Poor</span>'
        else:
            return '<span class="badge very-poor">❌ Very Poor</span>'
        
    def create_chart_base64(self, fig):
        """Convert matplotlib figure to base64 string for HTML embedding"""
        buffer = BytesIO()
        fig.savefig(buffer, format='png', dpi=300, bbox_inches='tight',
                    facecolor='white', edgecolor='none')
        buffer.seek(0)
        image_base64 = base64.b64encode(buffer.getvalue()).decode()
        buffer.close()
        plt.close(fig)
        return f"data:image/png;base64,{image_base64}"

    def generate_html_report(self, analysis_results: Dict[str, Dict], analysis_date: str):
        """
        Generate comprehensive HTML report

        Args:
            analysis_results: Dictionary containing analysis results for all sections
            analysis_date: Analysis date string
        """
        # Create output filename
        date_str = analysis_date.replace('-', '')
        output_filename = f"performance_analysis_{date_str}.html"
        output_path = os.path.join(self.output_html_path, output_filename)

        # Generate charts
        charts = self.create_performance_charts(analysis_results)

        # Create HTML content
        html_content = self.build_html_content(analysis_results, analysis_date, charts)

        # Save HTML file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"✓ HTML report generated: {output_path}")
        return output_path

    def create_performance_charts(self, analysis_results: Dict[str, Dict]) -> Dict[str, str]:
        """Create all performance charts and return as base64 strings"""
        charts = {}

        # Collect data for plotting
        section_data = {}
        for section_name, result in analysis_results.items():
            if 'data' in result and not result['data'].empty:
                section_data[section_name] = result['data']

        if not section_data:
            return charts

        try:
            # Collect data for plotting
            section_data = {}
            for section_name, result in analysis_results.items():
                if 'data' in result and not result['data'].empty:
                    section_data[section_name] = result['data']

            if not section_data:
                print("Warning: No data available for chart generation")
                return charts

            # Chart 1: Average Returns Comparison
            fig, ax = plt.subplots(figsize=(12, 6))
            sections = []
            returns_7d = []
            returns_15d = []

            for section, data in section_data.items():
                sections.append(section.title())
                avg_7d = data['return_7d'].mean() if 'return_7d' in data.columns and data['return_7d'].notna().any() else 0
                avg_15d = data['return_15d'].mean() if 'return_15d' in data.columns and data[
                    'return_15d'].notna().any() else 0
                returns_7d.append(avg_7d)
                returns_15d.append(avg_15d)

            x = range(len(sections))
            width = 0.35
            bars1 = ax.bar([i - width / 2 for i in x], returns_7d, width, label='7-Day Returns',
                           color='#2E86AB', alpha=0.8)
            bars2 = ax.bar([i + width / 2 for i in x], returns_15d, width, label='15-Day Returns',
                           color='#A23B72', alpha=0.8)

            ax.set_xlabel('Sections', fontsize=12)
            ax.set_ylabel('Average Return (%)', fontsize=12)
            ax.set_title('Average Returns by Section', fontsize=14, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels(sections)
            ax.legend()
            ax.grid(True, alpha=0.3)
            ax.axhline(y=0, color='red', linestyle='--', alpha=0.5)

            # Add value labels on bars
            for bar in bars1:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width() / 2., height + 0.1,
                        f'{height:.1f}%', ha='center', va='bottom', fontsize=10)

            for bar in bars2:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width() / 2., height + 0.1,
                        f'{height:.1f}%', ha='center', va='bottom', fontsize=10)

            plt.tight_layout()
            charts['avg_returns'] = self.create_chart_base64(fig)

            # Chart 2: Success Rate Comparison
            fig, ax = plt.subplots(figsize=(12, 6))
            success_rates_7d = []
            success_rates_15d = []

            for section, data in section_data.items():
                if 'return_7d' in data.columns and data['return_7d'].notna().any():
                    success_7d = (data['return_7d'] > 0).sum() / data['return_7d'].notna().sum() * 100
                else:
                    success_7d = 0

                if 'return_15d' in data.columns and data['return_15d'].notna().any():
                    success_15d = (data['return_15d'] > 0).sum() / data['return_15d'].notna().sum() * 100
                else:
                    success_15d = 0

                success_rates_7d.append(success_7d)
                success_rates_15d.append(success_15d)

            bars1 = ax.bar([i - width / 2 for i in x], success_rates_7d, width, label='7-Day Success Rate',
                           color='#F18F01', alpha=0.8)
            bars2 = ax.bar([i + width / 2 for i in x], success_rates_15d, width, label='15-Day Success Rate',
                           color='#C73E1D', alpha=0.8)

            ax.set_xlabel('Sections', fontsize=12)
            ax.set_ylabel('Success Rate (%)', fontsize=12)
            ax.set_title('Success Rate by Section (% Positive Returns)', fontsize=14, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels(sections)
            ax.legend()
            ax.grid(True, alpha=0.3)
            ax.set_ylim(0, 100)

            # Add value labels
            for bar in bars1:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width() / 2., height + 1,
                        f'{height:.0f}%', ha='center', va='bottom', fontsize=10)

            for bar in bars2:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width() / 2., height + 1,
                        f'{height:.0f}%', ha='center', va='bottom', fontsize=10)

            plt.tight_layout()
            charts['success_rates'] = self.create_chart_base64(fig)
            
        except Exception as e:
            print(f"Warning: Error generating charts: {str(e)}")
            return charts

        return charts

    def build_html_content(self, analysis_results: Dict[str, Dict], analysis_date: str, charts: Dict[str, str]) -> str:
        """Build the complete HTML content"""

        # Calculate summary statistics
        summary_stats = self.calculate_summary_statistics(analysis_results)

        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Stock Performance Analysis - {analysis_date}</title>
            <style>
                {self.get_css_styles()}
            </style>
        </head>
        <body>
            <div class="container">
                <header class="header">
                    <h1>📊 Stock Performance Analysis Report</h1>
                    <div class="analysis-date">Analysis Date: {analysis_date}</div>
                    <div class="generated-time">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
                </header>

                <div class="summary-cards">
                    {self.generate_summary_cards(summary_stats)}
                </div>

                <div class="charts-section">
                    <h2>📈 Performance Charts</h2>
                    {self.generate_charts_html(charts)}
                </div>

                <div class="sections-analysis">
                    <h2>🔍 Detailed Section Analysis</h2>
                    {self.generate_sections_html(analysis_results)}
                </div>

                <div class="insights-section">
                    <h2>💡 Key Insights</h2>
                    {self.generate_insights_html(analysis_results)}
                </div>

                <footer class="footer">
                    <p>Report generated by Stock Performance Analyzer | {datetime.now().year}</p>
                </footer>
            </div>

            <script>
                {self.get_javascript()}
            </script>
        </body>
        </html>
        """

        return html

    def calculate_summary_statistics(self, analysis_results: Dict[str, Dict]) -> Dict:
        """Calculate overall summary statistics"""
        stats = {
            'total_sections': len(analysis_results),
            'total_stocks': 0,
            'stocks_with_data': 0,
            'best_section_7d': {'name': 'N/A', 'return': 0},
            'best_section_15d': {'name': 'N/A', 'return': 0},
            'overall_success_rate_7d': 0,
            'overall_success_rate_15d': 0
        }

        total_positive_7d = 0
        total_stocks_7d = 0
        total_positive_15d = 0
        total_stocks_15d = 0

        for section_name, result in analysis_results.items():
            if 'summary' in result:
                summary = result['summary']
                stats['total_stocks'] += summary.get('total_stocks', 0)
                stats['stocks_with_data'] += summary.get('stocks_with_data', 0)

                # Track best performing sections
                if '7d_stats' in summary:
                    avg_return = summary['7d_stats'].get('avg_return', 0)
                    if avg_return > stats['best_section_7d']['return']:
                        stats['best_section_7d'] = {'name': section_name.title(), 'return': avg_return}

                    total_positive_7d += summary['7d_stats'].get('positive_returns', 0)
                    total_stocks_7d += summary['7d_stats'].get('stocks_with_7d_data', 0)

                if '15d_stats' in summary:
                    avg_return = summary['15d_stats'].get('avg_return', 0)
                    if avg_return > stats['best_section_15d']['return']:
                        stats['best_section_15d'] = {'name': section_name.title(), 'return': avg_return}

                    total_positive_15d += summary['15d_stats'].get('positive_returns', 0)
                    total_stocks_15d += summary['15d_stats'].get('stocks_with_15d_data', 0)

        # Calculate overall success rates
        if total_stocks_7d > 0:
            stats['overall_success_rate_7d'] = (total_positive_7d / total_stocks_7d) * 100
        if total_stocks_15d > 0:
            stats['overall_success_rate_15d'] = (total_positive_15d / total_stocks_15d) * 100

        return stats

    def generate_summary_cards(self, summary_stats: Dict) -> str:
        """Generate summary cards HTML"""
        return f"""
        <div class="summary-card">
            <h3>📊 Overall Statistics</h3>
            <div class="metric">
                <span>Total Sections:</span>
                <span class="metric-value">{summary_stats['total_sections']}</span>
            </div>
            <div class="metric">
                <span>Total Stocks:</span>
                <span class="metric-value">{summary_stats['total_stocks']}</span>
            </div>
            <div class="metric">
                <span>Stocks with Data:</span>
                <span class="metric-value">{summary_stats['stocks_with_data']}</span>
            </div>
        </div>

        <div class="summary-card">
            <h3>🏆 Best Performing Sections</h3>
            <div class="metric">
                <span>7-Day Winner:</span>
                <span class="metric-value">{summary_stats['best_section_7d']['name']}</span>
            </div>
            <div class="metric">
                <span>7-Day Return:</span>
                <span class="metric-value {'positive' if summary_stats['best_section_7d']['return'] >= 0 else 'negative'}">{summary_stats['best_section_7d']['return']:.2f}%</span>
            </div>
            <div class="metric">
                <span>15-Day Winner:</span>
                <span class="metric-value">{summary_stats['best_section_15d']['name']}</span>
            </div>
            <div class="metric">
                <span>15-Day Return:</span>
                <span class="metric-value {'positive' if summary_stats['best_section_15d']['return'] >= 0 else 'negative'}">{summary_stats['best_section_15d']['return']:.2f}%</span>
            </div>
        </div>

        <div class="summary-card">
            <h3>📈 Success Rates</h3>
            <div class="metric">
                <span>7-Day Success Rate:</span>
                <span class="metric-value">{summary_stats['overall_success_rate_7d']:.1f}%</span>
            </div>
            <div class="metric">
                <span>15-Day Success Rate:</span>
                <span class="metric-value">{summary_stats['overall_success_rate_15d']:.1f}%</span>
            </div>
        </div>
        """

    def generate_charts_html(self, charts: Dict[str, str]) -> str:
        """Generate charts section HTML"""
        html = ""
        if 'avg_returns' in charts:
            html += f"""
            <div class="chart-container">
                <h3>Average Returns Comparison</h3>
                <img src="{charts['avg_returns']}" alt="Average Returns Chart">
            </div>
            """

        if 'success_rates' in charts:
            html += f"""
            <div class="chart-container">
                <h3>Success Rate Comparison</h3>
                <img src="{charts['success_rates']}" alt="Success Rates Chart">
            </div>
            """

        return html

    def generate_sections_html(self, analysis_results: Dict[str, Dict]) -> str:
        """Generate detailed sections analysis HTML"""
        html = ""

        for section_name, result in analysis_results.items():
            if 'summary' not in result:
                continue

            summary = result['summary']
            data = result.get('data', pd.DataFrame())

            html += f"""
            <div class="section-analysis">
                <div class="section-header">
                    {section_name.title()} Section Analysis
                </div>
                <div class="section-content">
                    <div class="performance-stats">
                        <div class="stat-box">
                            <div class="stat-label">Total Stocks</div>
                            <div class="stat-value">{summary.get('total_stocks', 0)}</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-label">Stocks with Data</div>
                            <div class="stat-value">{summary.get('stocks_with_data', 0)}</div>
                        </div>
            """

            # Add 7-day stats if available
            if '7d_stats' in summary:
                stats_7d = summary['7d_stats']
                html += f"""
                        <div class="stat-box">
                            <div class="stat-label">7-Day Avg Return</div>
                            <div class="stat-value {'positive' if stats_7d['avg_return'] >= 0 else 'negative'}">{stats_7d['avg_return']:.2f}%</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-label">7-Day Success Rate</div>
                            <div class="stat-value">{stats_7d['positive_returns'] / stats_7d['stocks_with_7d_data'] * 100:.1f}%</div>
                        </div>
                """

            # Add 15-day stats if available
            if '15d_stats' in summary:
                stats_15d = summary['15d_stats']
                html += f"""
                        <div class="stat-box">
                            <div class="stat-label">15-Day Avg Return</div>
                            <div class="stat-value {'positive' if stats_15d['avg_return'] >= 0 else 'negative'}">{stats_15d['avg_return']:.2f}%</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-label">15-Day Success Rate</div>
                            <div class="stat-value">{stats_15d['positive_returns'] / stats_15d['stocks_with_15d_data'] * 100:.1f}%</div>
                        </div>
                """

            html += """
                    </div>
            """

            # Add top performers table
            if not data.empty:
                html += self.generate_performers_table(data, section_name)

            html += """
                </div>
            </div>
            """

        return html

    def generate_performers_table(self, data: pd.DataFrame, section_name: str) -> str:
        """Generate comprehensive stock performance table for a section"""
        try:
            if data.empty:
                return "<p>No performance data available for this section.</p>"

            # Validate required columns exist
            required_columns = ['symbol', 'initial_price']
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                return f"<p>Missing required columns: {', '.join(missing_columns)}</p>"

            # Create unique IDs for each section
            tab_7d_id = f"{section_name}-7d-tab"
            tab_15d_id = f"{section_name}-15d-tab"
            tab_all_id = f"{section_name}-all-tab"

            html = f"""
            <div class="tab-container">
                <div class="tab-buttons">
                    <button class="tab-button active" onclick="showTab(event, '{tab_all_id}')">All Stocks</button>
                    <button class="tab-button" onclick="showTab(event, '{tab_7d_id}')">7-Day Performance</button>
                    <button class="tab-button" onclick="showTab(event, '{tab_15d_id}')">15-Day Performance</button>
                </div>
            """

            # All stocks table with complete data
            html += f"""
            <div id="{tab_all_id}" class="tab-content active">
                <h4>📋 Complete Stock Performance Data</h4>
                <div class="table-responsive">
                    <table class="performers-table">
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Initial Price</th>
                                <th>7D Price</th>
                                <th>7D Return (%)</th>
                                <th>7D Max Gain (%)</th>
                                <th>7D Max Loss (%)</th>
                                <th>15D Price</th>
                                <th>15D Return (%)</th>
                                <th>15D Max Gain (%)</th>
                                <th>15D Max Loss (%)</th>
                            </tr>
                        </thead>
                        <tbody>
            """

            # Sort by symbol for consistent ordering
            sorted_data = data.sort_values('symbol')

            for _, row in sorted_data.iterrows():
                html += f"""
                            <tr>
                                <td class="stock-symbol">{row['symbol']}</td>
                                <td>{self.format_price(row.get('initial_price'))}</td>
                                <td>{self.format_price(row.get('price_7d'))}</td>
                                <td class="{self.get_return_class(row.get('return_7d'))}">{self.format_percentage(row.get('return_7d'))}</td>
                                <td class="positive">{self.format_percentage(row.get('max_gain_7d'))}</td>
                                <td class="negative">{self.format_percentage(row.get('max_loss_7d'))}</td>
                                <td>{self.format_price(row.get('price_15d'))}</td>
                                <td class="{self.get_return_class(row.get('return_15d'))}">{self.format_percentage(row.get('return_15d'))}</td>
                                <td class="positive">{self.format_percentage(row.get('max_gain_15d'))}</td>
                                <td class="negative">{self.format_percentage(row.get('max_loss_15d'))}</td>
                            </tr>
                """

            html += """
                        </tbody>
                    </table>
                </div>
            </div>
            """

            # 7-day performance table (sorted by performance)
            if 'return_7d' in data.columns:
                valid_7d = data[data['return_7d'].notna()].copy()
                if not valid_7d.empty:
                    sorted_7d = valid_7d.sort_values('return_7d', ascending=False)

                    html += f"""
                    <div id="{tab_7d_id}" class="tab-content">
                        <h4>📈 7-Day Performance Rankings</h4>
                        <div class="performance-summary">
                            <div class="summary-stat">
                                <span class="stat-label">Total Stocks:</span>
                                <span class="stat-value">{len(sorted_7d)}</span>
                            </div>
                            <div class="summary-stat">
                                <span class="stat-label">Positive Returns:</span>
                                <span class="stat-value positive">{len(sorted_7d[sorted_7d['return_7d'] > 0])}</span>
                            </div>
                            <div class="summary-stat">
                                <span class="stat-label">Success Rate:</span>
                                <span class="stat-value">{len(sorted_7d[sorted_7d['return_7d'] > 0]) / len(sorted_7d) * 100:.1f}%</span>
                            </div>
                        </div>
                        <div class="table-responsive">
                            <table class="performers-table">
                                <thead>
                                    <tr>
                                        <th>Rank</th>
                                        <th>Symbol</th>
                                        <th>Initial Price</th>
                                        <th>7D Price</th>
                                        <th>7D Return (%)</th>
                                        <th>Performance</th>
                                    </tr>
                                </thead>
                                <tbody>
                    """

                    for rank, (_, row) in enumerate(sorted_7d.iterrows(), 1):
                        performance_badge = self.get_performance_badge(row.get('return_7d'))
                        html += f"""
                                    <tr>
                                        <td class="rank">{rank}</td>
                                        <td class="stock-symbol">{row['symbol']}</td>
                                        <td>{self.format_price(row.get('initial_price'))}</td>
                                        <td>{self.format_price(row.get('price_7d'))}</td>
                                        <td class="{self.get_return_class(row.get('return_7d'))}">{self.format_percentage(row.get('return_7d'))}</td>
                                        <td>{performance_badge}</td>
                                    </tr>
                        """

                    html += """
                                </tbody>
                            </table>
                        </div>
                    </div>
                    """

            # 15-day performance table
            if 'return_15d' in data.columns:
                valid_15d = data[data['return_15d'].notna()].copy()
                if not valid_15d.empty:
                    sorted_15d = valid_15d.sort_values('return_15d', ascending=False)

                    html += f"""
                    <div id="{tab_15d_id}" class="tab-content">
                        <h4>📈 15-Day Performance Rankings</h4>
                        <div class="performance-summary">
                            <div class="summary-stat">
                                <span class="stat-label">Total Stocks:</span>
                                <span class="stat-value">{len(sorted_15d)}</span>
                            </div>
                            <div class="summary-stat">
                                <span class="stat-label">Positive Returns:</span>
                                <span class="stat-value positive">{len(sorted_15d[sorted_15d['return_15d'] > 0])}</span>
                            </div>
                            <div class="summary-stat">
                                <span class="stat-label">Success Rate:</span>
                                <span class="stat-value">{len(sorted_15d[sorted_15d['return_15d'] > 0]) / len(sorted_15d) * 100:.1f}%</span>
                            </div>
                        </div>
                        <div class="table-responsive">
                            <table class="performers-table">
                                <thead>
                                    <tr>
                                        <th>Rank</th>
                                        <th>Symbol</th>
                                        <th>Initial Price</th>
                                        <th>15D Price</th>
                                        <th>15D Return (%)</th>
                                        <th>Performance</th>
                                    </tr>
                                </thead>
                                <tbody>
                    """

                    for rank, (_, row) in enumerate(sorted_15d.iterrows(), 1):
                        performance_badge = self.get_performance_badge(row.get('return_15d'))
                        html += f"""
                                    <tr>
                                        <td class="rank">{rank}</td>
                                        <td class="stock-symbol">{row['symbol']}</td>
                                        <td>{self.format_price(row.get('initial_price'))}</td>
                                        <td>{self.format_price(row.get('price_15d'))}</td>
                                        <td class="{self.get_return_class(row.get('return_15d'))}">{self.format_percentage(row.get('return_15d'))}</td>
                                        <td>{performance_badge}</td>
                                    </tr>
                        """

                    html += """
                                </tbody>
                            </table>
                        </div>
                    </div>
                    """

            html += "</div>"
            return html

        except Exception as e:
            return f"<p>Error generating performance table: {str(e)}</p>"

    def generate_insights_html(self, analysis_results: Dict[str, Dict]) -> str:
        """Generate insights section HTML"""
        insights = self._generate_insights_list(analysis_results)

        html = "<ul class='insights-list'>"
        for insight in insights:
            html += f"<li>{insight}</li>"
        html += "</ul>"

        return html

    def _generate_insights_list(self, analysis_results: Dict[str, Dict]) -> List[str]:
        """Generate insights from analysis results"""
        insights = []

        # Find best performing section
        best_7d_section = None
        best_7d_return = float('-inf')
        best_15d_section = None
        best_15d_return = float('-inf')

        for section_name, result in analysis_results.items():
            if 'summary' in result:
                summary = result['summary']

                if '7d_stats' in summary:
                    avg_return = summary['7d_stats']['avg_return']
                    if avg_return > best_7d_return:
                        best_7d_return = avg_return
                        best_7d_section = section_name

                if '15d_stats' in summary:
                    avg_return = summary['15d_stats']['avg_return']
                    if avg_return > best_15d_return:
                        best_15d_return = avg_return
                        best_15d_section = section_name

        if best_7d_section:
            insights.append(
                f"🏆 <strong>Best 7-day performance:</strong> {best_7d_section.title()} section with {best_7d_return:.2f}% average return")

        if best_15d_section:
            insights.append(
                f"🏆 <strong>Best 15-day performance:</strong> {best_15d_section.title()} section with {best_15d_return:.2f}% average return")

        # Check prediction accuracy
        for section_name, result in analysis_results.items():
            if 'summary' in result and section_name in ['bullish', 'bearish']:
                summary = result['summary']

                if '7d_stats' in summary:
                    stats = summary['7d_stats']
                    success_rate = stats['positive_returns'] / stats['stocks_with_7d_data'] * 100

                    if section_name == 'bullish' and success_rate > 60:
                        insights.append(
                            f"✅ <strong>Bullish predictions were accurate:</strong> {success_rate:.1f}% of bullish stocks had positive 7-day returns")
                    elif section_name == 'bullish' and success_rate < 40:
                        insights.append(
                            f"❌ <strong>Bullish predictions were poor:</strong> Only {success_rate:.1f}% of bullish stocks had positive 7-day returns")

                    if section_name == 'bearish' and success_rate < 40:
                        insights.append(
                            f"✅ <strong>Bearish predictions were accurate:</strong> Only {success_rate:.1f}% of bearish stocks had positive 7-day returns")
                    elif section_name == 'bearish' and success_rate > 60:
                        insights.append(
                            f"❌ <strong>Bearish predictions were poor:</strong> {success_rate:.1f}% of bearish stocks had positive 7-day returns")

        if not insights:
            insights.append("• No significant insights could be generated from the available data.")

        return insights

    def get_javascript(self) -> str:
        """Return JavaScript for interactive features"""
        return """
            function showTab(evt, tabName) {
                        // Find the parent tab container
                        var tabContainer = evt.currentTarget.closest('.tab-container');
                        
                        // Hide all tab contents in this container
                        var tabContents = tabContainer.querySelectorAll('.tab-content');
                        for (var i = 0; i < tabContents.length; i++) {
                            tabContents[i].classList.remove('active');
                        }
                        
                        // Remove active class from all buttons in this container
                        var tabButtons = tabContainer.querySelectorAll('.tab-button');
                        for (var i = 0; i < tabButtons.length; i++) {
                            tabButtons[i].classList.remove('active');
                        }
                        
                        // Show the selected tab and mark button as active
                        document.getElementById(tabName).classList.add('active');
                        evt.currentTarget.classList.add('active');
                    }

            // Smooth scrolling for better UX
            document.addEventListener('DOMContentLoaded', function() {
                const links = document.querySelectorAll('a[href^="#"]');
                for (const link of links) {
                    link.addEventListener('click', function(e) {
                        e.preventDefault();
                        const targetId = this.getAttribute('href');
                        const targetElement = document.querySelector(targetId);
                        if (targetElement) {
                            targetElement.scrollIntoView({
                                behavior: 'smooth'
                            });
                        }
                    });
                }
            });
        """

    def get_html_file_path(self, analysis_date: str) -> str:
        """
        Get the HTML file path for a given date
        
        Args:
            analysis_date: Date in YYYY-MM-DD format
        
        Returns:
            Full path to the HTML file
        """
        # Convert date format from YYYY-MM-DD to YYYYMMDD
        date_obj = datetime.strptime(analysis_date, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%Y%m%d')
        filename = f"market_dashboard_{formatted_date}.html"
        return os.path.join(self.base_html_path, filename)
    
    def validate_inputs(self, analysis_date: str) -> bool:
        """
        Validate if the analysis date and files exist
        
        Args:
            analysis_date: Date in YYYY-MM-DD format
        
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Validate date format
            datetime.strptime(analysis_date, '%Y-%m-%d')
            
            # Check if HTML file exists
            html_path = self.get_html_file_path(analysis_date)
            if not os.path.exists(html_path):
                print(f"Error: HTML file not found at {html_path}")
                return False
            
            # Check if database exists
            if not os.path.exists(self.db_path):
                print(f"Error: Database not found at {self.db_path}")
                return False
            
            return True
        except ValueError:
            print("Error: Invalid date format. Please use YYYY-MM-DD format.")
            return False
    
    def parse_html_file(self, analysis_date: str) -> Dict[str, List[str]]:
        """
        Parse the HTML file and extract stock symbols from each section
        
        Args:
            analysis_date: Date in YYYY-MM-DD format
        
        Returns:
            Dictionary containing stock lists for each section
        """
        html_path = self.get_html_file_path(analysis_date)
        
        try:
            with open(html_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            
            # Dictionary to store stocks from each section
            stock_sections = {
                'bullish': [],
                'bearish': [],
                'reversal': [],
                'smart_money': []
            }
            
            # Find the stock lists tab content
            stock_lists_tab = soup.find('div', {'id': 'stock-lists-tab'})
            
            if stock_lists_tab:
                # Extract Bullish Stocks
                bullish_section = self._extract_stocks_from_section(
                    stock_lists_tab, "Bullish Stocks"
                )
                stock_sections['bullish'] = bullish_section
                
                # Extract Bearish Stocks
                bearish_section = self._extract_stocks_from_section(
                    stock_lists_tab, "Bearish Stocks"
                )
                stock_sections['bearish'] = bearish_section
                
                # Extract Reversal Stocks
                reversal_section = self._extract_stocks_from_section(
                    stock_lists_tab, "Reversal Stocks"
                )
                stock_sections['reversal'] = reversal_section
                
                # Extract Smart Money Stocks
                smart_money_section = self._extract_stocks_from_section(
                    stock_lists_tab, "Smart Money Stocks"
                )
                stock_sections['smart_money'] = smart_money_section
            
            return stock_sections
            
        except Exception as e:
            print(f"Error parsing HTML file: {str(e)}")
            return {}
    
    def _extract_stocks_from_section(self, parent_element, section_title: str) -> List[str]:
        """
        Extract stock symbols from a specific section
        
        Args:
            parent_element: BeautifulSoup element containing the section
            section_title: Title of the section to find
        
        Returns:
            List of stock symbols
        """
        stocks = []
        
        try:
            # Find the section by title
            section_header = parent_element.find('h3', string=lambda text: text and section_title in text)
            
            if section_header:
                # Find the table in the same chart-container
                chart_container = section_header.find_parent('div', class_='chart-container')
                if chart_container:
                    table = chart_container.find('table', class_='stock-table')
                    if table:
                        # Extract stock symbols from the first column (excluding header)
                        rows = table.find_all('tr')[1:]  # Skip header row
                        for row in rows:
                            cells = row.find_all('td')
                            if cells:
                                stock_symbol = cells[0].get_text(strip=True)
                                if stock_symbol:
                                    stocks.append(stock_symbol)
        
        except Exception as e:
            print(f"Error extracting stocks from {section_title}: {str(e)}")
        
        return stocks
    
    def print_extracted_stocks(self, stock_sections: Dict[str, List[str]]):
        """
        Print the extracted stocks for verification
        
        Args:
            stock_sections: Dictionary containing stock lists
        """
        print("=" * 80)
        print("EXTRACTED STOCK SECTIONS")
        print("=" * 80)
        
        for section_name, stocks in stock_sections.items():
            print(f"\n{section_name.upper()} STOCKS ({len(stocks)} stocks):")
            print("-" * 50)
            if stocks:
                # Print stocks in rows of 10
                for i in range(0, len(stocks), 10):
                    row_stocks = stocks[i:i+10]
                    print(" | ".join(f"{stock:<12}" for stock in row_stocks))
            else:
                print("No stocks found in this section")
        
        print("\n" + "=" * 80)

    def get_trading_dates(self, start_date: str, end_date: str) -> List[str]:
            """
            Get all available trading dates between start and end date from database

            Args:
                start_date: Start date in YYYY-MM-DD format
                end_date: End date in YYYY-MM-DD format

            Returns:
                List of trading dates in YYYY-MM-DD format
            """
            try:
                conn = sqlite3.connect(self.db_path)
                query = """
                    SELECT DISTINCT date 
                    FROM stock_prices 
                    WHERE date BETWEEN ? AND ? 
                    ORDER BY date
                """
                df = pd.read_sql_query(query, conn, params=[start_date, end_date])
                conn.close()

                return df['date'].tolist()

            except Exception as e:
                print(f"Error fetching trading dates: {str(e)}")
                return []
    
    def calculate_target_dates(self, analysis_date: str) -> Dict[str, str]:
        """
        Calculate target dates for analysis (7 days and 15 days after analysis date)

        Args:
            analysis_date: Analysis date in YYYY-MM-DD format

        Returns:
            Dictionary with target dates
        """
        try:
            analysis_dt = datetime.strptime(analysis_date, '%Y-%m-%d')

            # Calculate approximate target dates
            date_7_approx = (analysis_dt + timedelta(days=10)).strftime('%Y-%m-%d')  # Buffer for weekends
            date_15_approx = (analysis_dt + timedelta(days=20)).strftime('%Y-%m-%d')  # Buffer for weekends

            # Get actual trading dates
            trading_dates = self.get_trading_dates(analysis_date, date_15_approx)

            if not trading_dates:
                print("No trading dates found in the specified range")
                return {}

            # Find actual 7th and 15th trading day
            target_dates = {
                'analysis_date': analysis_date,
                'date_7': None,
                'date_15': None
            }

            # Skip the analysis date itself and find 7th and 15th trading day
            future_dates = [date for date in trading_dates if date > analysis_date]

            if len(future_dates) >= 7:
                target_dates['date_7'] = future_dates[6]  # 7th trading day (0-indexed)

            if len(future_dates) >= 15:
                target_dates['date_15'] = future_dates[14]  # 15th trading day (0-indexed)

            return target_dates

        except Exception as e:
            print(f"Error calculating target dates: {str(e)}")
            return {}
    
    def fetch_stock_prices(self, symbols: List[str], dates: List[str]) -> pd.DataFrame:
        """
        Fetch stock prices for given symbols and dates
        
        Args:
            symbols: List of stock symbols
            dates: List of dates in YYYY-MM-DD format
        
        Returns:
            DataFrame with stock prices
        """
        if not symbols or not dates:
            return pd.DataFrame()
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Create placeholders for SQL query
            symbol_placeholders = ','.join(['?' for _ in symbols])
            date_placeholders = ','.join(['?' for _ in dates])
            
            query = f"""
                SELECT date, symbol, close_price, high_price, low_price, open_price
                FROM stock_prices 
                WHERE symbol IN ({symbol_placeholders}) 
                AND date IN ({date_placeholders})
                ORDER BY symbol, date
            """
            
            params = symbols + dates
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            return df
        
        except Exception as e:
            print(f"Error fetching stock prices: {str(e)}")
            return pd.DataFrame()
    
    def get_price_data_for_analysis(self, symbols: List[str], target_dates: Dict[str, str]) -> pd.DataFrame:
        """
        Get comprehensive price data for analysis
        
        Args:
            symbols: List of stock symbols
            target_dates: Dictionary with analysis and target dates
        
        Returns:
            DataFrame with price analysis data
        """
        # Prepare dates for query
        dates_to_fetch = [date for date in target_dates.values() if date is not None]
        
        if not dates_to_fetch:
            print("No valid dates for analysis")
            return pd.DataFrame()
        
        # Fetch price data
        price_df = self.fetch_stock_prices(symbols, dates_to_fetch)
        
        if price_df.empty:
            print("No price data found for the given symbols and dates")
            return pd.DataFrame()
        
        # Pivot data for easier analysis
        analysis_results = []
        
        for symbol in symbols:
            symbol_data = price_df[price_df['symbol'] == symbol].copy()
            
            if symbol_data.empty:
                # Add missing symbol with NaN values
                analysis_results.append({
                    'symbol': symbol,
                    'analysis_date': target_dates['analysis_date'],
                    'initial_price': None,
                    'price_7d': None,
                    'price_15d': None,
                    'high_7d': None,
                    'low_7d': None,
                    'high_15d': None,
                    'low_15d': None,
                    'return_7d': None,
                    'return_15d': None,
                    'max_gain_7d': None,
                    'max_loss_7d': None,
                    'max_gain_15d': None,
                    'max_loss_15d': None
                })
                continue
            
            # Get prices for different dates
            initial_price = symbol_data[symbol_data['date'] == target_dates['analysis_date']]['close_price'].iloc[0] if not symbol_data[symbol_data['date'] == target_dates['analysis_date']].empty else None
            
            price_7d = symbol_data[symbol_data['date'] == target_dates.get('date_7')]['close_price'].iloc[0] if target_dates.get('date_7') and not symbol_data[symbol_data['date'] == target_dates.get('date_7')].empty else None
            
            price_15d = symbol_data[symbol_data['date'] == target_dates.get('date_15')]['close_price'].iloc[0] if target_dates.get('date_15') and not symbol_data[symbol_data['date'] == target_dates.get('date_15')].empty else None
            
            # Calculate high/low for periods
            if target_dates.get('date_7'):
                period_7d_data = symbol_data[symbol_data['date'].between(target_dates['analysis_date'], target_dates['date_7'])]
                high_7d = period_7d_data['high_price'].max() if not period_7d_data.empty else None
                low_7d = period_7d_data['low_price'].min() if not period_7d_data.empty else None
            else:
                high_7d, low_7d = None, None
            
            if target_dates.get('date_15'):
                period_15d_data = symbol_data[symbol_data['date'].between(target_dates['analysis_date'], target_dates['date_15'])]
                high_15d = period_15d_data['high_price'].max() if not period_15d_data.empty else None
                low_15d = period_15d_data['low_price'].min() if not period_15d_data.empty else None
            else:
                high_15d, low_15d = None, None
            
            # Calculate returns and max gains/losses
            return_7d = ((price_7d - initial_price) / initial_price * 100) if initial_price and price_7d else None
            return_15d = ((price_15d - initial_price) / initial_price * 100) if initial_price and price_15d else None
            
            max_gain_7d = ((high_7d - initial_price) / initial_price * 100) if initial_price and high_7d else None
            max_loss_7d = ((low_7d - initial_price) / initial_price * 100) if initial_price and low_7d else None
            
            max_gain_15d = ((high_15d - initial_price) / initial_price * 100) if initial_price and high_15d else None
            max_loss_15d = ((low_15d - initial_price) / initial_price * 100) if initial_price and low_15d else None
            
            analysis_results.append({
                'symbol': symbol,
                'analysis_date': target_dates['analysis_date'],
                'initial_price': initial_price,
                'price_7d': price_7d,
                'price_15d': price_15d,
                'high_7d': high_7d,
                'low_7d': low_7d,
                'high_15d': high_15d,
                'low_15d': low_15d,
                'return_7d': return_7d,
                'return_15d': return_15d,
                'max_gain_7d': max_gain_7d,
                'max_loss_7d': max_loss_7d,
                'max_gain_15d': max_gain_15d,
                'max_loss_15d': max_loss_15d
            })
        
        return pd.DataFrame(analysis_results)




    def analyze_section_performance(self, section_name: str, symbols: List[str], target_dates: Dict[str, str]) -> Dict:
            """
            Analyze performance for a specific section

            Args:
                section_name: Name of the section (bullish, bearish, etc.)
                symbols: List of stock symbols in the section
                target_dates: Dictionary with analysis and target dates

            Returns:
                Dictionary with analysis results
            """
            print(f"\nAnalyzing {section_name.upper()} section...")
            print(f"Total stocks: {len(symbols)}")

            if not symbols:
                return {'section': section_name, 'total_stocks': 0, 'data': pd.DataFrame()}

            # Get price data
            price_data = self.get_price_data_for_analysis(symbols, target_dates)

            if price_data.empty:
                print(f"No price data available for {section_name} section")
                return {'section': section_name, 'total_stocks': len(symbols), 'data': pd.DataFrame()}

            # Filter out stocks with no initial price data
            valid_data = price_data[price_data['initial_price'].notna()].copy()

            # Calculate summary statistics
            summary_stats = {
                'section': section_name,
                'total_stocks': len(symbols),
                'stocks_with_data': len(valid_data),
                'missing_data': len(symbols) - len(valid_data),
                'target_dates': target_dates
            }

            if not valid_data.empty:
                # 7-day statistics
                if target_dates.get('date_7'):
                    valid_7d = valid_data[valid_data['return_7d'].notna()]
                    if not valid_7d.empty:
                        summary_stats['7d_stats'] = {
                            'stocks_with_7d_data': len(valid_7d),
                            'avg_return': valid_7d['return_7d'].mean(),
                            'median_return': valid_7d['return_7d'].median(),
                            'positive_returns': len(valid_7d[valid_7d['return_7d'] > 0]),
                            'negative_returns': len(valid_7d[valid_7d['return_7d'] < 0]),
                            'best_performer': valid_7d.loc[valid_7d['return_7d'].idxmax()]['symbol'] if not valid_7d.empty else None,
                            'best_return': valid_7d['return_7d'].max(),
                            'worst_performer': valid_7d.loc[valid_7d['return_7d'].idxmin()]['symbol'] if not valid_7d.empty else None,
                            'worst_return': valid_7d['return_7d'].min(),
                            'std_dev': valid_7d['return_7d'].std()
                        }

                # 15-day statistics
                if target_dates.get('date_15'):
                    valid_15d = valid_data[valid_data['return_15d'].notna()]
                    if not valid_15d.empty:
                        summary_stats['15d_stats'] = {
                            'stocks_with_15d_data': len(valid_15d),
                            'avg_return': valid_15d['return_15d'].mean(),
                            'median_return': valid_15d['return_15d'].median(),
                            'positive_returns': len(valid_15d[valid_15d['return_15d'] > 0]),
                            'negative_returns': len(valid_15d[valid_15d['return_15d'] < 0]),
                            'best_performer': valid_15d.loc[valid_15d['return_15d'].idxmax()]['symbol'] if not valid_15d.empty else None,
                            'best_return': valid_15d['return_15d'].max(),
                            'worst_performer': valid_15d.loc[valid_15d['return_15d'].idxmin()]['symbol'] if not valid_15d.empty else None,
                            'worst_return': valid_15d['return_15d'].min(),
                            'std_dev': valid_15d['return_15d'].std()
                        }

            return {
                'summary': summary_stats,
                'data': valid_data
            }

  
    def generate_section_report(self, analysis_result: Dict, show_top_n: int = 10):
        """
        Generate a detailed report for a section
        
        Args:
            analysis_result: Result from analyze_section_performance
            show_top_n: Number of top/bottom performers to show
        """
        summary = analysis_result['summary']
        data = analysis_result['data']
        
        print(f"\n{'='*80}")
        print(f"{summary['section'].upper()} SECTION PERFORMANCE REPORT")
        print(f"{'='*80}")
        
        print(f"Analysis Date: {summary['target_dates']['analysis_date']}")
        print(f"Total Stocks in Section: {summary['total_stocks']}")
        print(f"Stocks with Price Data: {summary['stocks_with_data']}")
        print(f"Missing Data: {summary['missing_data']}")
        
        if data.empty:
            print("No performance data available for this section.")
            return
        
        # 7-day performance report
        if '7d_stats' in summary:
            stats_7d = summary['7d_stats']
            print(f"\n📊 7-DAY PERFORMANCE (Target Date: {summary['target_dates'].get('date_7', 'N/A')})")
            print("-" * 60)
            print(f"Stocks with Data: {stats_7d['stocks_with_7d_data']}")
            print(f"Average Return: {stats_7d['avg_return']:.2f}%")
            print(f"Median Return: {stats_7d['median_return']:.2f}%")
            print(f"Positive Returns: {stats_7d['positive_returns']} ({stats_7d['positive_returns']/stats_7d['stocks_with_7d_data']*100:.1f}%)")
            print(f"Negative Returns: {stats_7d['negative_returns']} ({stats_7d['negative_returns']/stats_7d['stocks_with_7d_data']*100:.1f}%)")
            print(f"Best Performer: {stats_7d['best_performer']} ({stats_7d['best_return']:.2f}%)")
            print(f"Worst Performer: {stats_7d['worst_performer']} ({stats_7d['worst_return']:.2f}%)")
            print(f"Standard Deviation: {stats_7d['std_dev']:.2f}%")
        
        # 15-day performance report
        if '15d_stats' in summary:
            stats_15d = summary['15d_stats']
            print(f"\n📊 15-DAY PERFORMANCE (Target Date: {summary['target_dates'].get('date_15', 'N/A')})")
            print("-" * 60)
            print(f"Stocks with Data: {stats_15d['stocks_with_15d_data']}")
            print(f"Average Return: {stats_15d['avg_return']:.2f}%")
            print(f"Median Return: {stats_15d['median_return']:.2f}%")
            print(f"Positive Returns: {stats_15d['positive_returns']} ({stats_15d['positive_returns']/stats_15d['stocks_with_15d_data']*100:.1f}%)")
            print(f"Negative Returns: {stats_15d['negative_returns']} ({stats_15d['negative_returns']/stats_15d['stocks_with_15d_data']*100:.1f}%)")
            print(f"Best Performer: {stats_15d['best_performer']} ({stats_15d['best_return']:.2f}%)")
            print(f"Worst Performer: {stats_15d['worst_performer']} ({stats_15d['worst_return']:.2f}%)")
            print(f"Standard Deviation: {stats_15d['std_dev']:.2f}%")
        
        # Top performers tables
        self._print_top_performers(data, show_top_n)
    
    def _print_top_performers(self, data: pd.DataFrame, show_top_n: int):
        """Print top and bottom performers in tabulated format"""
        
        if data.empty:
            return
        
        # Top performers 7-day
        if 'return_7d' in data.columns:
            valid_7d = data[data['return_7d'].notna()].copy()
            if not valid_7d.empty:
                print(f"\n🏆 TOP {show_top_n} PERFORMERS (7-Day)")
                top_7d = valid_7d.nlargest(show_top_n, 'return_7d')[['symbol', 'initial_price', 'price_7d', 'return_7d', 'max_gain_7d', 'max_loss_7d']]
                print(tabulate(top_7d, headers=['Symbol', 'Initial Price', '7D Price', '7D Return%', 'Max Gain%', 'Max Loss%'], 
                              tablefmt='grid', floatfmt='.2f'))
                
                print(f"\n📉 BOTTOM {show_top_n} PERFORMERS (7-Day)")
                bottom_7d = valid_7d.nsmallest(show_top_n, 'return_7d')[['symbol', 'initial_price', 'price_7d', 'return_7d', 'max_gain_7d', 'max_loss_7d']]
                print(tabulate(bottom_7d, headers=['Symbol', 'Initial Price', '7D Price', '7D Return%', 'Max Gain%', 'Max Loss%'], 
                              tablefmt='grid', floatfmt='.2f'))
        
        # Top performers 15-day
        if 'return_15d' in data.columns:
            valid_15d = data[data['return_15d'].notna()].copy()
            if not valid_15d.empty:
                print(f"\n🏆 TOP {show_top_n} PERFORMERS (15-Day)")
                top_15d = valid_15d.nlargest(show_top_n, 'return_15d')[['symbol', 'initial_price', 'price_15d', 'return_15d', 'max_gain_15d', 'max_loss_15d']]
                print(tabulate(top_15d, headers=['Symbol', 'Initial Price', '15D Price', '15D Return%', 'Max Gain%', 'Max Loss%'], 
                              tablefmt='grid', floatfmt='.2f'))
                
                print(f"\n📉 BOTTOM {show_top_n} PERFORMERS (15-Day)")
                bottom_15d = valid_15d.nsmallest(show_top_n, 'return_15d')[['symbol', 'initial_price', 'price_15d', 'return_15d', 'max_gain_15d', 'max_loss_15d']]
                print(tabulate(bottom_15d, headers=['Symbol', 'Initial Price', '15D Price', '15D Return%', 'Max Gain%', 'Max Loss%'], 
                              tablefmt='grid', floatfmt='.2f'))
    
    def create_performance_visualizations(self, analysis_results: Dict[str, Dict], save_path: str = None):
        """
        Create visualizations for performance analysis
        
        Args:
            analysis_results: Dictionary containing analysis results for all sections
            save_path: Optional path to save the plots
        """
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Stock Performance Analysis Dashboard', fontsize=16, fontweight='bold')
        
        # Collect data for plotting
        section_data = {}
        for section_name, result in analysis_results.items():
            if 'data' in result and not result['data'].empty:
                section_data[section_name] = result['data']
        
        if not section_data:
            print("No data available for visualization")
            return
        
        # Plot 1: Average Returns by Section (7-day and 15-day)
        ax1 = axes[0, 0]
        sections = []
        returns_7d = []
        returns_15d = []
        
        for section, data in section_data.items():
            sections.append(section.title())
            avg_7d = data['return_7d'].mean() if 'return_7d' in data.columns and data['return_7d'].notna().any() else 0
            avg_15d = data['return_15d'].mean() if 'return_15d' in data.columns and data['return_15d'].notna().any() else 0
            returns_7d.append(avg_7d)
            returns_15d.append(avg_15d)
        
        x = range(len(sections))
        width = 0.35
        ax1.bar([i - width/2 for i in x], returns_7d, width, label='7-Day', alpha=0.8)
        ax1.bar([i + width/2 for i in x], returns_15d, width, label='15-Day', alpha=0.8)
        ax1.set_xlabel('Sections')
        ax1.set_ylabel('Average Return (%)')
        ax1.set_title('Average Returns by Section')
        ax1.set_xticks(x)
        ax1.set_xticklabels(sections, rotation=45)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.axhline(y=0, color='red', linestyle='--', alpha=0.5)
        
        # Plot 2: Success Rate by Section
        ax2 = axes[0, 1]
        success_rates_7d = []
        success_rates_15d = []
        
        for section, data in section_data.items():
            if 'return_7d' in data.columns and data['return_7d'].notna().any():
                success_7d = (data['return_7d'] > 0).sum() / data['return_7d'].notna().sum() * 100
            else:
                success_7d = 0
            
            if 'return_15d' in data.columns and data['return_15d'].notna().any():
                success_15d = (data['return_15d'] > 0).sum() / data['return_15d'].notna().sum() * 100
            else:
                success_15d = 0
            
            success_rates_7d.append(success_7d)
            success_rates_15d.append(success_15d)
        
        ax2.bar([i - width/2 for i in x], success_rates_7d, width, label='7-Day', alpha=0.8)
        ax2.bar([i + width/2 for i in x], success_rates_15d, width, label='15-Day', alpha=0.8)
        ax2.set_xlabel('Sections')
        ax2.set_ylabel('Success Rate (%)')
        ax2.set_title('Success Rate by Section (% Positive Returns)')
        ax2.set_xticks(x)
        ax2.set_xticklabels(sections, rotation=45)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.set_ylim(0, 100)
        
        # Plot 3: Return Distribution (7-day)
        ax3 = axes[1, 0]
        all_returns_7d = []
        section_labels = []
        
        for section, data in section_data.items():
            if 'return_7d' in data.columns:
                valid_returns = data['return_7d'].dropna()
                if not valid_returns.empty:
                    all_returns_7d.extend(valid_returns.tolist())
                    section_labels.extend([section.title()] * len(valid_returns))
        
        if all_returns_7d:
            df_plot = pd.DataFrame({'Returns': all_returns_7d, 'Section': section_labels})
            sns.boxplot(data=df_plot, x='Section', y='Returns', ax=ax3)
            ax3.set_title('7-Day Return Distribution by Section')
            ax3.set_ylabel('Return (%)')
            ax3.tick_params(axis='x', rotation=45)
            ax3.grid(True, alpha=0.3)
            ax3.axhline(y=0, color='red', linestyle='--', alpha=0.5)
        
        # Plot 4: Return Distribution (15-day)
        ax4 = axes[1, 1]
        all_returns_15d = []
        section_labels_15d = []
        
        for section, data in section_data.items():
            if 'return_15d' in data.columns:
                valid_returns = data['return_15d'].dropna()
                if not valid_returns.empty:
                    all_returns_15d.extend(valid_returns.tolist())
                    section_labels_15d.extend([section.title()] * len(valid_returns))
        
        if all_returns_15d:
            df_plot_15d = pd.DataFrame({'Returns': all_returns_15d, 'Section': section_labels_15d})
            sns.boxplot(data=df_plot_15d, x='Section', y='Returns', ax=ax4)
            ax4.set_title('15-Day Return Distribution by Section')
            ax4.set_ylabel('Return (%)')
            ax4.tick_params(axis='x', rotation=45)
            ax4.grid(True, alpha=0.3)
            ax4.axhline(y=0, color='red', linestyle='--', alpha=0.5)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Visualization saved to: {save_path}")
        
        plt.show()

    def get_css_styles(self) -> str:
        """Return enhanced CSS styles for the HTML report"""
        return """
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }

            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                line-height: 1.6;
                color: #333;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }

            .container {
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
            }

            .header {
                background: white;
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.1);
                text-align: center;
                margin-bottom: 30px;
            }

            .header h1 {
                color: #2c3e50;
                font-size: 2.5em;
                margin-bottom: 10px;
            }

            .analysis-date {
                font-size: 1.3em;
                color: #3498db;
                font-weight: 600;
            }

            .generated-time {
                color: #7f8c8d;
                margin-top: 5px;
            }

            .summary-cards {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }

            .summary-card {
                background: white;
                padding: 25px;
                border-radius: 12px;
                box-shadow: 0 5px 15px rgba(0,0,0,0.08);
                border-left: 5px solid #3498db;
                transition: transform 0.2s ease;
            }

            .summary-card:hover {
                transform: translateY(-3px);
            }

            .summary-card h3 {
                color: #2c3e50;
                margin-bottom: 15px;
                font-size: 1.2em;
            }

            .summary-card .metric {
                display: flex;
                justify-content: space-between;
                margin-bottom: 8px;
            }

            .metric-value {
                font-weight: 600;
                color: #27ae60;
            }

            .metric-value.negative {
                color: #e74c3c;
            }

            .charts-section, .sections-analysis, .insights-section {
                background: white;
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.1);
                margin-bottom: 30px;
            }

            .charts-section h2, .sections-analysis h2, .insights-section h2 {
                color: #2c3e50;
                margin-bottom: 25px;
                font-size: 1.8em;
                border-bottom: 3px solid #3498db;
                padding-bottom: 10px;
            }

            .chart-container {
                margin-bottom: 30px;
                text-align: center;
            }

            .chart-container img {
                max-width: 100%;
                height: auto;
                border-radius: 8px;
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            }

            .section-analysis {
               margin-bottom: 40px;
               border: 1px solid #ecf0f1;
               border-radius: 10px;
               overflow: hidden;
           }

           .section-header {
               background: linear-gradient(135deg, #3498db, #2980b9);
               color: white;
               padding: 20px;
               font-size: 1.3em;
               font-weight: 600;
           }

           .section-content {
               padding: 25px;
           }

           .performance-stats {
               display: grid;
               grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
               gap: 20px;
               margin-bottom: 25px;
           }

           .stat-box {
               background: #f8f9fa;
               padding: 15px;
               border-radius: 8px;
               text-align: center;
               border-left: 4px solid #3498db;
           }

           .stat-label {
               color: #7f8c8d;
               font-size: 0.9em;
               margin-bottom: 5px;
           }

           .stat-value {
               font-size: 1.4em;
               font-weight: 600;
               color: #2c3e50;
           }

           .insights-list {
               list-style: none;
           }

           .insights-list li {
               background: #f8f9fa;
               margin-bottom: 15px;
               padding: 15px;
               border-radius: 8px;
               border-left: 4px solid #f39c12;
           }

           .footer {
               text-align: center;
               padding: 20px;
               color: white;
               background: rgba(255,255,255,0.1);
               border-radius: 10px;
           }

           .tab-container {
               margin-bottom: 20px;
           }

           .tab-buttons {
               display: flex;
               background: #ecf0f1;
               border-radius: 8px;
               padding: 5px;
               margin-bottom: 20px;
           }

           .tab-button {
               flex: 1;
               padding: 10px 20px;
               background: transparent;
               border: none;
               border-radius: 5px;
               cursor: pointer;
               font-weight: 500;
               transition: all 0.2s ease;
           }

           .tab-button.active {
               background: #3498db;
               color: white;
           }

           .tab-content {
               display: none;
           }

           .tab-content.active {
               display: block;
           }

           /* Enhanced table styles */
           .table-responsive {
               overflow-x: auto;
               margin: 20px 0;
               border-radius: 8px;
               box-shadow: 0 2px 8px rgba(0,0,0,0.1);
           }

           .performers-table {
               width: 100%;
               border-collapse: collapse;
               background: white;
               font-size: 0.9em;
           }

           .performers-table th {
               background: linear-gradient(135deg, #34495e, #2c3e50);
               color: white;
               padding: 12px 8px;
               text-align: center;
               font-weight: 600;
               font-size: 0.85em;
               border-right: 1px solid rgba(255,255,255,0.1);
           }

           .performers-table td {
               padding: 10px 8px;
               text-align: center;
               border-bottom: 1px solid #ecf0f1;
               border-right: 1px solid #ecf0f1;
           }

           .performers-table tbody tr:nth-child(even) {
               background: #f8f9fa;
           }

           .performers-table tbody tr:hover {
               background: #e3f2fd;
               transition: background-color 0.2s ease;
           }

           .stock-symbol {
               font-weight: 600;
               color: #2c3e50;
               font-family: 'Courier New', monospace;
           }

           .rank {
               font-weight: 600;
               color: #7f8c8d;
               font-size: 0.9em;
           }

           .positive {
               color: #27ae60;
               font-weight: 600;
           }

           .negative {
               color: #e74c3c;
               font-weight: 600;
           }

           .neutral {
               color: #7f8c8d;
               font-style: italic;
           }

           /* Performance badges */
           .badge {
               padding: 4px 8px;
               border-radius: 12px;
               font-size: 0.75em;
               font-weight: 600;
               text-align: center;
               display: inline-block;
               min-width: 80px;
           }

           .badge.excellent {
               background: linear-gradient(135deg, #27ae60, #2ecc71);
               color: white;
           }

           .badge.very-good {
               background: linear-gradient(135deg, #16a085, #1abc9c);
               color: white;
           }

           .badge.good {
               background: linear-gradient(135deg, #f39c12, #e67e22);
               color: white;
           }

           .badge.slight-positive {
               background: linear-gradient(135deg, #3498db, #2980b9);
               color: white;
           }

           .badge.neutral {
               background: #95a5a6;
               color: white;
           }

           .badge.slight-negative {
               background: linear-gradient(135deg, #e67e22, #d35400);
               color: white;
           }

           .badge.poor {
               background: linear-gradient(135deg, #e74c3c, #c0392b);
               color: white;
           }

           .badge.very-poor {
               background: linear-gradient(135deg, #8e44ad, #9b59b6);
               color: white;
           }

           /* Performance summary */
           .performance-summary {
               display: grid;
               grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
               gap: 15px;
               margin: 20px 0;
               padding: 20px;
               background: #f8f9fa;
               border-radius: 8px;
               border-left: 4px solid #3498db;
           }

           .summary-stat {
               text-align: center;
           }

           .summary-stat .stat-label {
               display: block;
               color: #7f8c8d;
               font-size: 0.85em;
               margin-bottom: 5px;
           }

           .summary-stat .stat-value {
               display: block;
               font-size: 1.2em;
               font-weight: 600;
               color: #2c3e50;
           }

           @media (max-width: 768px) {
               .container {
                   padding: 10px;
               }

               .header h1 {
                   font-size: 2em;
               }

               .performance-stats {
                   grid-template-columns: 1fr;
               }

               .performers-table {
                   font-size: 0.8em;
               }

               .performers-table th,
               .performers-table td {
                   padding: 8px 4px;
               }

               .performance-summary {
                   grid-template-columns: repeat(2, 1fr);
               }

               .badge {
                   font-size: 0.7em;
                   padding: 3px 6px;
                   min-width: 60px;
               }
           }

           /* Print styles */
           @media print {
               .tab-buttons {
                   display: none;
               }

               .tab-content {
                   display: block !important;
                   page-break-inside: avoid;
               }

               .performers-table {
                   font-size: 0.8em;
               }
           }
       """

    
    def generate_summary_report(self, analysis_results: Dict[str, Dict]):
        """
        Generate an overall summary report comparing all sections
        
        Args:
            analysis_results: Dictionary containing analysis results for all sections
        """
        print(f"\n{'='*100}")
        print("OVERALL PERFORMANCE SUMMARY")
        print(f"{'='*100}")
        
        summary_data = []
        
        for section_name, result in analysis_results.items():
            if 'summary' not in result:
                continue
                
            summary = result['summary']
            row = {
                'Section': section_name.title(),
                'Total Stocks': summary['total_stocks'],
                'With Data': summary['stocks_with_data']
            }
            
            # Add 7-day metrics
            if '7d_stats' in summary:
                stats = summary['7d_stats']
                row.update({
                    '7D Avg Return (%)': f"{stats['avg_return']:.2f}",
                    '7D Success Rate (%)': f"{stats['positive_returns']/stats['stocks_with_7d_data']*100:.1f}",
                    '7D Best (%)': f"{stats['best_return']:.2f}",
                    '7D Worst (%)': f"{stats['worst_return']:.2f}"
                })
            else:
                row.update({
                    '7D Avg Return (%)': 'N/A',
                    '7D Success Rate (%)': 'N/A',
                    '7D Best (%)': 'N/A',
                    '7D Worst (%)': 'N/A'
                })
            
            # Add 15-day metrics
            if '15d_stats' in summary:
                stats = summary['15d_stats']
                row.update({
                    '15D Avg Return (%)': f"{stats['avg_return']:.2f}",
                    '15D Success Rate (%)': f"{stats['positive_returns']/stats['stocks_with_15d_data']*100:.1f}",
                    '15D Best (%)': f"{stats['best_return']:.2f}",
                    '15D Worst (%)': f"{stats['worst_return']:.2f}"
                })
            else:
                row.update({
                    '15D Avg Return (%)': 'N/A',
                    '15D Success Rate (%)': 'N/A',
                    '15D Best (%)': 'N/A',
                    '15D Worst (%)': 'N/A'
                })
            
            summary_data.append(row)
        
        if summary_data:
            df_summary = pd.DataFrame(summary_data)
            print(tabulate(df_summary, headers='keys', tablefmt='grid'))
        
        # Key insights
        print(f"\n{'='*100}")
        print("KEY INSIGHTS")
        print(f"{'='*100}")
        
        self._generate_insights(analysis_results)
    
    def _generate_insights(self, analysis_results: Dict[str, Dict]):
        """Generate key insights from the analysis"""
        
        insights = []
        
        # Find best performing section
        best_7d_section = None
        best_7d_return = float('-inf')
        best_15d_section = None
        best_15d_return = float('-inf')
        
        for section_name, result in analysis_results.items():
            if 'summary' in result:
                summary = result['summary']
                
                if '7d_stats' in summary:
                    avg_return = summary['7d_stats']['avg_return']
                    if avg_return > best_7d_return:
                        best_7d_return = avg_return
                        best_7d_section = section_name
                
                if '15d_stats' in summary:
                    avg_return = summary['15d_stats']['avg_return']
                    if avg_return > best_15d_return:
                        best_15d_return = avg_return
                        best_15d_section = section_name
        
        if best_7d_section:
            insights.append(f"🏆 Best 7-day performance: {best_7d_section.title()} section with {best_7d_return:.2f}% average return")
        
        if best_15d_section:
            insights.append(f"🏆 Best 15-day performance: {best_15d_section.title()} section with {best_15d_return:.2f}% average return")
        
        # Check if bullish/bearish predictions were accurate
        for section_name, result in analysis_results.items():
            if 'summary' in result and section_name in ['bullish', 'bearish']:
                summary = result['summary']
                
                if '7d_stats' in summary:
                    stats = summary['7d_stats']
                    success_rate = stats['positive_returns'] / stats['stocks_with_7d_data'] * 100
                    
                    if section_name == 'bullish' and success_rate > 60:
                        insights.append(f"✅ Bullish predictions were accurate: {success_rate:.1f}% of bullish stocks had positive 7-day returns")
                    elif section_name == 'bullish' and success_rate < 40:
                        insights.append(f"❌ Bullish predictions were poor: Only {success_rate:.1f}% of bullish stocks had positive 7-day returns")
                    
                    if section_name == 'bearish' and success_rate < 40:
                        insights.append(f"✅ Bearish predictions were accurate: Only {success_rate:.1f}% of bearish stocks had positive 7-day returns")
                    elif section_name == 'bearish' and success_rate > 60:
                        insights.append(f"❌ Bearish predictions were poor: {success_rate:.1f}% of bearish stocks had positive 7-day returns")
        
        for insight in insights:
            print(f"• {insight}")
        
        if not insights:
            print("• No significant insights could be generated from the available data.")






    def get_enhanced_css_styles(self) -> str:
        """Return enhanced CSS styles including new table styles"""
        base_css = self.get_css_styles()  # Get existing CSS

        enhanced_css = base_css + """
    
            /* Enhanced table styles */
            .table-responsive {
                overflow-x: auto;
                margin: 20px 0;
                border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            }
    
            .performers-table {
                width: 100%;
                border-collapse: collapse;
                background: white;
                font-size: 0.9em;
            }
    
            .performers-table th {
                background: linear-gradient(135deg, #34495e, #2c3e50);
                color: white;
                padding: 12px 8px;
                text-align: center;
                font-weight: 600;
                font-size: 0.85em;
                border-right: 1px solid rgba(255,255,255,0.1);
            }
    
            .performers-table td {
                padding: 10px 8px;
                text-align: center;
                border-bottom: 1px solid #ecf0f1;
                border-right: 1px solid #ecf0f1;
            }
    
            .performers-table tbody tr:nth-child(even) {
                background: #f8f9fa;
            }
    
            .performers-table tbody tr:hover {
                background: #e3f2fd;
                transition: background-color 0.2s ease;
            }
    
            .stock-symbol {
                font-weight: 600;
                color: #2c3e50;
                font-family: 'Courier New', monospace;
            }
    
            .rank {
                font-weight: 600;
                color: #7f8c8d;
                font-size: 0.9em;
            }
    
            .positive {
                color: #27ae60;
                font-weight: 600;
            }
    
            .negative {
                color: #e74c3c;
                font-weight: 600;
            }
    
            .neutral {
                color: #7f8c8d;
                font-style: italic;
            }
    
            /* Performance badges */
            .badge {
                padding: 4px 8px;
                border-radius: 12px;
                font-size: 0.75em;
                font-weight: 600;
                text-align: center;
                display: inline-block;
                min-width: 80px;
            }
    
            .badge.excellent {
                background: linear-gradient(135deg, #27ae60, #2ecc71);
                color: white;
            }
    
            .badge.very-good {
                background: linear-gradient(135deg, #16a085, #1abc9c);
                color: white;
            }
    
            .badge.good {
                background: linear-gradient(135deg, #f39c12, #e67e22);
                color: white;
            }
    
            .badge.slight-positive {
                background: linear-gradient(135deg, #3498db, #2980b9);
                color: white;
            }
    
            .badge.neutral {
                background: #95a5a6;
                color: white;
            }
    
            .badge.slight-negative {
                background: linear-gradient(135deg, #e67e22, #d35400);
                color: white;
            }
    
            .badge.poor {
                background: linear-gradient(135deg, #e74c3c, #c0392b);
                color: white;
            }
    
            .badge.very-poor {
                background: linear-gradient(135deg, #8e44ad, #9b59b6);
                color: white;
            }
    
            /* Performance summary */
            .performance-summary {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 15px;
                margin: 20px 0;
                padding: 20px;
                background: #f8f9fa;
                border-radius: 8px;
                border-left: 4px solid #3498db;
            }
    
            .summary-stat {
                text-align: center;
            }
    
            .summary-stat .stat-label {
                display: block;
                color: #7f8c8d;
                font-size: 0.85em;
                margin-bottom: 5px;
            }
    
            .summary-stat .stat-value {
                display: block;
                font-size: 1.2em;
                font-weight: 600;
                color: #2c3e50;
            }
    
            /* Mobile responsiveness */
            @media (max-width: 768px) {
                .performers-table {
                    font-size: 0.8em;
                }
    
                .performers-table th,
                .performers-table td {
                    padding: 8px 4px;
                }
    
                .performance-summary {
                    grid-template-columns: repeat(2, 1fr);
                }
    
                .badge {
                    font-size: 0.7em;
                    padding: 3px 6px;
                    min-width: 60px;
                }
            }
    
            /* Print styles */
            @media print {
                .tab-buttons {
                    display: none;
                }
    
                .tab-content {
                    display: block !important;
                    page-break-inside: avoid;
                }
    
                .performers-table {
                    font-size: 0.8em;
                }
            }
        """

        return enhanced_css


    # Update the main get_css_styles method to use enhanced styles
def get_css_styles(self) -> str:
    """Return enhanced CSS styles for the HTML report"""
    return self.get_enhanced_css_styles()



# Testing function for Part 3
def test_part3():
    """Test the analysis and reporting functionality"""
    analyzer = StockPerformanceAnalyzer()
    test_date = "2025-06-13"

    # Mock some analysis results for testing
    sample_data = pd.DataFrame({
        'symbol': ['RELIANCE', 'TCS', 'HDFCBANK'],
        'initial_price': [2500, 3200, 1800],
        'price_7d': [2550, 3150, 1850],
        'price_15d': [2600, 3100, 1900],
        'return_7d': [2.0, -1.56, 2.78],
        'return_15d': [4.0, -3.13, 5.56],
        'max_gain_7d': [3.0, 0.5, 4.0],
        'max_loss_7d': [-1.0, -2.0, -0.5],
        'max_gain_15d': [5.0, 1.0, 6.0],
        'max_loss_15d': [-2.0, -4.0, -1.0]
    })

    mock_result = {
        'summary': {
            'section': 'bullish',
            'total_stocks': 3,
            'stocks_with_data': 3,
            'target_dates': {'analysis_date': test_date, 'date_7': '2025-06-23', 'date_15': '2025-07-01'},
            '7d_stats': {
                'stocks_with_7d_data': 3,
                'avg_return': 1.07,
                'median_return': 2.0,
                'positive_returns': 2,
                'negative_returns': 1,
                'best_performer': 'HDFCBANK',
                'best_return': 2.78,
                'worst_performer': 'TCS',
                'worst_return': -1.56,
                'std_dev': 2.17
            }
        },
        'data': sample_data
    }

    print("Testing Part 3 - Analysis and Reporting")
    analyzer.generate_section_report(mock_result, show_top_n=3)


def main():
    """
    Main function to run the complete stock performance analysis with HTML output
    """
    print("=" * 100)
    print("STOCK PERFORMANCE ANALYSIS TOOL - HTML REPORT GENERATOR")
    print("=" * 100)

    # Initialize analyzer
    analyzer = StockPerformanceAnalyzer()

    # Get analysis date from user
    analysis_date = get_analysis_date_input()

    if not analysis_date:
        print("Invalid date input. Exiting...")
        return

    print(f"\nStarting analysis for date: {analysis_date}")
    print("-" * 60)

    # Validate inputs
    if not analyzer.validate_inputs(analysis_date):
        print("Validation failed. Please check your inputs and try again.")
        return

    print("✓ Validation passed")

    # Parse HTML file to extract stock sections
    print("📄 Parsing HTML file...")
    stock_sections = analyzer.parse_html_file(analysis_date)

    if not stock_sections:
        print("❌ Failed to extract stock sections from HTML file.")
        return

    print("✓ HTML parsing completed")

    # Calculate target dates
    print("\n📅 Calculating target dates...")
    target_dates = analyzer.calculate_target_dates(analysis_date)

    if not target_dates:
        print("❌ Failed to calculate target dates.")
        return

    print("✓ Target dates calculated")

    # Analyze each section
    print("\n🔍 Starting performance analysis...")
    analysis_results = {}

    for section_name, symbols in stock_sections.items():
        if symbols:  # Only analyze sections with stocks
            result = analyzer.analyze_section_performance(section_name, symbols, target_dates)
            analysis_results[section_name] = result
            print(f"✓ Analyzed {section_name} section")

    # Generate HTML report
    if analysis_results:
        print("\n📊 Generating HTML report...")
        report_path = analyzer.generate_html_report(analysis_results, analysis_date)
        print(f"✓ HTML report saved to: {report_path}")

        # Open the report in browser
        import webbrowser
        webbrowser.open(f'file://{os.path.abspath(report_path)}')

    print(f"\n{'=' * 100}")
    print("ANALYSIS COMPLETED - HTML REPORT GENERATED")
    print(f"{'=' * 100}")


def get_analysis_date_input() -> str:
    """
    Get analysis date input from user with validation

    Returns:
        Date string in YYYY-MM-DD format or None if invalid
    """
    while True:
        print("\nPlease enter the analysis date:")
        print("Format: YYYY-MM-DD (e.g., 2025-06-13)")

        # Show some example dates based on existing files
        try:
            base_path = r"C:\Projects\apps\institutional_flow_quant\output\progressive_analysis"
            if os.path.exists(base_path):
                html_files = [f for f in os.listdir(base_path) if
                              f.startswith('market_dashboard_') and f.endswith('.html')]
                if html_files:
                    print("\nAvailable analysis dates based on existing files:")
                    for file in sorted(html_files)[-5:]:  # Show last 5 files
                        date_part = file.replace('market_dashboard_', '').replace('.html', '')
                        if len(date_part) == 8:
                            formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                            print(f"  - {formatted_date}")
        except:
            pass

        date_input = input("\nEnter date (or 'quit' to exit): ").strip()

        if date_input.lower() in ['quit', 'exit', 'q']:
            return None

        # Validate date format
        try:
            datetime.strptime(date_input, '%Y-%m-%d')
            return date_input
        except ValueError:
            print("❌ Invalid date format. Please use YYYY-MM-DD format.")
            continue


def export_results_to_csv(analysis_results: Dict, analysis_date: str):
    """
    Export analysis results to CSV files

    Args:
        analysis_results: Dictionary containing analysis results
        analysis_date: Analysis date string
    """
    try:
        # Create output directory
        output_dir = os.path.join(r"C:\Projects\apps\institutional_flow_quant\data", "analysis_output")
        os.makedirs(output_dir, exist_ok=True)

        date_str = analysis_date.replace('-', '')

        # Export detailed data for each section
        for section_name, result in analysis_results.items():
            if 'data' in result and not result['data'].empty:
                filename = f"{section_name}_performance_{date_str}.csv"
                filepath = os.path.join(output_dir, filename)
                result['data'].to_csv(filepath, index=False)
                print(f"✓ Exported {section_name} data to: {filepath}")

        # Create summary CSV
        summary_data = []
        for section_name, result in analysis_results.items():
            if 'summary' in result:
                summary = result['summary']
                row = {'section': section_name, 'analysis_date': analysis_date}

                # Add basic info
                row.update({
                    'total_stocks': summary.get('total_stocks', 0),
                    'stocks_with_data': summary.get('stocks_with_data', 0),
                    'missing_data': summary.get('missing_data', 0)
                })

                # Add 7-day stats
                if '7d_stats' in summary:
                    stats = summary['7d_stats']
                    row.update({
                        '7d_avg_return': stats.get('avg_return'),
                        '7d_median_return': stats.get('median_return'),
                        '7d_positive_returns': stats.get('positive_returns'),
                        '7d_success_rate': stats.get('positive_returns', 0) / max(stats.get('stocks_with_7d_data', 1),
                                                                                  1) * 100,
                        '7d_best_return': stats.get('best_return'),
                        '7d_worst_return': stats.get('worst_return'),
                        '7d_std_dev': stats.get('std_dev')
                    })

                # Add 15-day stats
                if '15d_stats' in summary:
                    stats = summary['15d_stats']
                    row.update({
                        '15d_avg_return': stats.get('avg_return'),
                        '15d_median_return': stats.get('median_return'),
                        '15d_positive_returns': stats.get('positive_returns'),
                        '15d_success_rate': stats.get('positive_returns', 0) / max(stats.get('stocks_with_15d_data', 1),
                                                                                   1) * 100,
                        '15d_best_return': stats.get('best_return'),
                        '15d_worst_return': stats.get('worst_return'),
                        '15d_std_dev': stats.get('std_dev')
                    })

                summary_data.append(row)

        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_filename = f"performance_summary_{date_str}.csv"
            summary_filepath = os.path.join(output_dir, summary_filename)
            summary_df.to_csv(summary_filepath, index=False)
            print(f"✓ Exported summary to: {summary_filepath}")

        print(f"\n📁 All files exported to: {output_dir}")

    except Exception as e:
        print(f"❌ Error exporting to CSV: {str(e)}")


def run_batch_analysis():
    """
    Run analysis for multiple dates (batch processing)
    """
    print("=" * 100)
    print("BATCH ANALYSIS MODE")
    print("=" * 100)

    analyzer = StockPerformanceAnalyzer()

    # Get list of available HTML files
    base_path = r"C:\Projects\apps\institutional_flow_quant\output\progressive_analysis"

    if not os.path.exists(base_path):
        print(f"❌ Directory not found: {base_path}")
        return

    html_files = [f for f in os.listdir(base_path) if f.startswith('market_dashboard_') and f.endswith('.html')]

    if not html_files:
        print("❌ No market dashboard HTML files found.")
        return

    print(f"Found {len(html_files)} HTML files for analysis:")

    # Convert filenames to dates and sort
    date_files = []
    for file in html_files:
        date_part = file.replace('market_dashboard_', '').replace('.html', '')
        if len(date_part) == 8:
            try:
                formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                datetime.strptime(formatted_date, '%Y-%m-%d')  # Validate date
                date_files.append((formatted_date, file))
            except ValueError:
                continue

    date_files.sort()

    # Show available dates
    for i, (date, file) in enumerate(date_files):
        print(f"  {i + 1:2d}. {date}")

    # Get user selection
    print("\nOptions:")
    print("  A. Analyze all files")
    print("  L. Analyze last N files")
    print("  S. Select specific dates")
    print("  Q. Quit")

    choice = input("\nEnter your choice: ").upper().strip()

    dates_to_analyze = []

    if choice == 'A':
        dates_to_analyze = [date for date, file in date_files]
    elif choice == 'L':
        try:
            n = int(input("Enter number of recent files to analyze: "))
            dates_to_analyze = [date for date, file in date_files[-n:]]
        except ValueError:
            print("❌ Invalid number.")
            return
    elif choice == 'S':
        print("Enter date numbers separated by commas (e.g., 1,3,5):")
        try:
            indices = [int(x.strip()) - 1 for x in input().split(',')]
            dates_to_analyze = [date_files[i][0] for i in indices if 0 <= i < len(date_files)]
        except (ValueError, IndexError):
            print("❌ Invalid selection.")
            return
    elif choice == 'Q':
        return
    else:
        print("❌ Invalid choice.")
        return

    if not dates_to_analyze:
        print("❌ No dates selected for analysis.")
        return

    print(f"\nAnalyzing {len(dates_to_analyze)} dates...")

    # Run analysis for each date
    all_results = {}

    for i, date in enumerate(dates_to_analyze, 1):
        print(f"\n{'=' * 80}")
        print(f"ANALYZING {i}/{len(dates_to_analyze)}: {date}")
        print(f"{'=' * 80}")

        try:
            if analyzer.validate_inputs(date):
                stock_sections = analyzer.parse_html_file(date)
                target_dates = analyzer.calculate_target_dates(date)

                if stock_sections and target_dates:
                    date_results = {}
                    for section_name, symbols in stock_sections.items():
                        if symbols:
                            result = analyzer.analyze_section_performance(section_name, symbols, target_dates)
                            date_results[section_name] = result

                    all_results[date] = date_results
                    print(f"✓ Analysis completed for {date}")
                else:
                    print(f"❌ Failed to analyze {date}")
            else:
                print(f"❌ Validation failed for {date}")

        except Exception as e:
            print(f"❌ Error analyzing {date}: {str(e)}")

    # Export batch results
    if all_results:
        print(f"\n{'=' * 80}")
        print("EXPORTING BATCH RESULTS")
        print(f"{'=' * 80}")

        # Create combined summary
        export_batch_results(all_results)

    print(f"\n{'=' * 80}")
    print("BATCH ANALYSIS COMPLETED")
    print(f"{'=' * 80}")


def export_batch_results(all_results: Dict):
    """Export results from batch analysis"""
    try:
        output_dir = os.path.join(r"C:\Projects\apps\institutional_flow_quant\data", "batch_analysis_output")
        os.makedirs(output_dir, exist_ok=True)

        # Create combined summary across all dates
        combined_summary = []

        for date, date_results in all_results.items():
            for section_name, result in date_results.items():
                if 'summary' in result:
                    summary = result['summary']
                    row = {
                        'date': date,
                        'section': section_name,
                        'total_stocks': summary.get('total_stocks', 0),
                        'stocks_with_data': summary.get('stocks_with_data', 0)
                    }

                    # Add performance metrics
                    if '7d_stats' in summary:
                        stats = summary['7d_stats']
                        row.update({
                            '7d_avg_return': stats.get('avg_return'),
                            '7d_success_rate': stats.get('positive_returns', 0) / max(
                                stats.get('stocks_with_7d_data', 1), 1) * 100
                        })

                    if '15d_stats' in summary:
                        stats = summary['15d_stats']
                        row.update({
                            '15d_avg_return': stats.get('avg_return'),
                            '15d_success_rate': stats.get('positive_returns', 0) / max(
                                stats.get('stocks_with_15d_data', 1), 1) * 100
                        })

                    combined_summary.append(row)

        if combined_summary:
            summary_df = pd.DataFrame(combined_summary)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            summary_filename = f"batch_analysis_summary_{timestamp}.csv"
            summary_filepath = os.path.join(output_dir, summary_filename)
            summary_df.to_csv(summary_filepath, index=False)
            print(f"✓ Batch summary exported to: {summary_filepath}")

    except Exception as e:
        print(f"❌ Error exporting batch results: {str(e)}")


if __name__ == "__main__":
    print("Stock Performance Analysis Tool")
    print("1. Single Date Analysis")
    print("2. Batch Analysis")
    print("3. Quit")

    choice = input("\nSelect mode (1-3): ").strip()

    if choice == '1':
        main()
    elif choice == '2':
        run_batch_analysis()
    elif choice == '3':
        print("Goodbye!")
    else:
        print("Invalid choice. Exiting...")