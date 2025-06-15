#!/usr/bin/env python3
"""
Test script to debug Distribution Trends analysis issues
This isolates the sentiment distribution and sector sentiment trend analysis logic
"""

import os
import re
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import plotly.graph_objects as go
from plotly.subplots import make_subplots


class DistributionTrendsDebugger:
    """
    Debug class to test and fix distribution trends analysis
    """

    def __init__(self, reports_dir="./"):
        self.reports_dir = reports_dir
        self.reports_data = []
        self.trend_periods = [7, 15, 30]

    def scan_dashboard_files(self):
        """Scan for market dashboard HTML files"""
        import glob

        dashboard_files = glob.glob(os.path.join(self.reports_dir, "market_dashboard_*.html"))
        file_date_pairs = []

        for file_path in dashboard_files:
            filename = os.path.basename(file_path)
            match = re.search(r'market_dashboard_(\d{8})\.html', filename)
            if match:
                date_str = match.group(1)
                try:
                    file_date = datetime.strptime(date_str, "%Y%m%d")
                    file_date_pairs.append((file_path, file_date))
                except ValueError:
                    print(f"Warning: Invalid date format in filename: {filename}")

        # Sort by date (newest first)
        file_date_pairs.sort(key=lambda x: x[1], reverse=True)

        print(f"🔍 Found {len(file_date_pairs)} dashboard files:")
        for file_path, file_date in file_date_pairs:
            print(f"   📄 {os.path.basename(file_path)} - {file_date.strftime('%Y-%m-%d')}")

        return file_date_pairs

    def extract_sentiment_distribution_from_html(self, html_file):
        """Extract sentiment distribution data from HTML file"""
        distribution = {
            'LONG': 0,
            'ACCUMULATION': 0,
            'NEUTRAL': 0,
            'DISTRIBUTION': 0,
            'SHORT': 0
        }

        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()

            soup = BeautifulSoup(html_content, 'html.parser')

            # Try to extract sentiment distribution from JavaScript
            sentiment_match = re.search(r'const\s+sentimentFig\s*=\s*({.+?});', html_content, re.DOTALL)
            if sentiment_match:
                try:
                    sentiment_json = sentiment_match.group(1)
                    # Convert JavaScript object to valid JSON
                    sentiment_json = re.sub(r'(\w+):', r'"\1":', sentiment_json)
                    sentiment_data = json.loads(sentiment_json)

                    # Extract data from the sentiment chart
                    if 'data' in sentiment_data and len(sentiment_data['data']) > 0:
                        data = sentiment_data['data'][0]
                        if 'x' in data and 'y' in data:
                            categories = data['x']
                            values = data['y']

                            for i, category in enumerate(categories):
                                if i < len(values):
                                    distribution[category] = values[i]

                    print(f"✅ Extracted sentiment distribution from {os.path.basename(html_file)}: {distribution}")
                except Exception as e:
                    print(f"❌ Error parsing sentiment distribution data: {e}")
            else:
                print(f"⚠️  No sentiment distribution data found in {os.path.basename(html_file)}")

        except Exception as e:
            print(f"❌ Error extracting sentiment distribution from {html_file}: {e}")

        return distribution

    def extract_sector_sentiment_from_html(self, html_file):
        """Extract sector sentiment data from HTML file"""
        sector_data = {}

        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Try to extract sector heatmap data from JavaScript
            sector_match = re.search(r'const\s+sectorFig\s*=\s*({.+?});', html_content, re.DOTALL)
            if sector_match:
                try:
                    sector_json = sector_match.group(1)
                    # Convert JavaScript object to valid JSON
                    sector_json = re.sub(r'(\w+):', r'"\1":', sector_json)
                    sector_data_raw = json.loads(sector_json)

                    # Extract sectors and sentiment categories from heatmap
                    if 'data' in sector_data_raw and len(sector_data_raw['data']) > 0:
                        heatmap_data = sector_data_raw['data'][0]
                        if 'z' in heatmap_data and 'x' in heatmap_data and 'y' in heatmap_data:
                            sentiment_categories = heatmap_data['x']
                            sectors = heatmap_data['y']
                            values = heatmap_data['z']

                            for i, sector in enumerate(sectors):
                                sector_data[sector] = {}
                                for j, category in enumerate(sentiment_categories):
                                    if i < len(values) and j < len(values[i]):
                                        sector_data[sector][category] = values[i][j]

                    print(
                        f"✅ Extracted sector sentiment from {os.path.basename(html_file)}: {len(sector_data)} sectors")
                except Exception as e:
                    print(f"❌ Error parsing sector sentiment data: {e}")
            else:
                print(f"⚠️  No sector sentiment data found in {os.path.basename(html_file)}")

        except Exception as e:
            print(f"❌ Error extracting sector sentiment from {html_file}: {e}")

        return sector_data

    def extract_basic_metrics_from_html(self, html_file):
        """Extract basic metrics from HTML file"""
        metrics = {
            'filename': os.path.basename(html_file),
            'date': '',
            'totalStocks': 0,
            'overallSentiment': {
                'score': 0,
                'bullishPercentage': 0,
                'bearishPercentage': 0
            },
            'sentimentDistribution': {},
            'sectorSentiment': {}
        }

        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()

            soup = BeautifulSoup(html_content, 'html.parser')

            # Extract date and total stocks
            header_text = soup.select_one('.header p')
            if header_text:
                date_match = re.search(r'Analysis Date:\s*([\d-]+)', header_text.text)
                stocks_match = re.search(r'Total Stocks Analyzed:\s*(\d+)', header_text.text)

                if date_match:
                    metrics['date'] = date_match.group(1)
                if stocks_match:
                    metrics['totalStocks'] = int(stocks_match.group(1))

            # If date not found in header, try to extract from filename
            if not metrics['date']:
                filename = os.path.basename(html_file)
                file_match = re.search(r'market_dashboard_(\d{8})\.html', filename)
                if file_match:
                    date_str = file_match.group(1)
                    metrics['date'] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

            # Extract sentiment distribution and sector sentiment
            metrics['sentimentDistribution'] = self.extract_sentiment_distribution_from_html(html_file)
            metrics['sectorSentiment'] = self.extract_sector_sentiment_from_html(html_file)

            print(
                f"📊 Extracted metrics from {os.path.basename(html_file)}: Date={metrics['date']}, Stocks={metrics['totalStocks']}")

        except Exception as e:
            print(f"❌ Error extracting basic metrics from {html_file}: {e}")

        return metrics

    def load_all_reports(self):
        """Load data from all available reports"""
        file_date_pairs = self.scan_dashboard_files()

        self.reports_data = []
        for file_path, file_date in file_date_pairs:
            print(f"\n🔄 Processing {os.path.basename(file_path)}...")
            metrics = self.extract_basic_metrics_from_html(file_path)
            self.reports_data.append(metrics)

        print(f"\n📋 Total reports loaded: {len(self.reports_data)}")
        return len(self.reports_data)

    def analyze_sentiment_distribution_trends(self):
        """Analyze trends in sentiment distribution across time periods"""
        print(f"\n🔍 Analyzing sentiment distribution trends...")
        print(f"📊 Available reports: {len(self.reports_data)}")

        if not self.reports_data or len(self.reports_data) <= 1:
            print("❌ Insufficient data for sentiment distribution trend analysis")
            return {'insufficient': True, 'reason': f'Only {len(self.reports_data)} reports available, need at least 2'}

        # Sort reports by date (newest first)
        sorted_reports = sorted(self.reports_data, key=lambda x: x['date'], reverse=True)
        print(f"📅 Date range: {sorted_reports[-1]['date']} to {sorted_reports[0]['date']}")

        # Create a time series of sentiment distributions
        sentiment_history = {
            'dates': [],
            'LONG': [],
            'ACCUMULATION': [],
            'NEUTRAL': [],
            'DISTRIBUTION': [],
            'SHORT': []
        }

        for report in reversed(sorted_reports):  # Oldest to newest for time series
            distribution = report.get('sentimentDistribution', {})
            if not distribution or all(v == 0 for v in distribution.values()):
                print(f"⚠️  No sentiment distribution data in report for {report['date']}")
                continue

            sentiment_history['dates'].append(report['date'])
            for category in ['LONG', 'ACCUMULATION', 'NEUTRAL', 'DISTRIBUTION', 'SHORT']:
                sentiment_history[category].append(distribution.get(category, 0))

        print(f"📈 Sentiment history compiled: {len(sentiment_history['dates'])} valid data points")

        # Calculate changes for different time periods
        trends = {}

        for days in self.trend_periods:
            print(f"\n🔍 Analyzing {days}-day trend...")

            if len(sentiment_history['dates']) <= 1:
                trends[f'{days}d'] = {'insufficient': True,
                                      'reason': f'Only {len(sentiment_history["dates"])} data points available'}
                continue

            # Find index for comparison (or use the oldest available)
            compare_index = min(days, len(sentiment_history['dates']) - 1)

            period_trend = {
                'days': days,
                'startDate': sentiment_history['dates'][0],
                'endDate': sentiment_history['dates'][-1],
                'changes': {}
            }

            # Calculate changes for each sentiment category
            for category in ['LONG', 'ACCUMULATION', 'NEUTRAL', 'DISTRIBUTION', 'SHORT']:
                if sentiment_history[category]:
                    # Latest value minus the comparison value
                    latest = sentiment_history[category][-1]
                    compare = sentiment_history[category][len(sentiment_history[category]) - 1 - compare_index]

                    change = latest - compare
                    period_trend['changes'][category] = change

                    # Add trend direction
                    threshold = 5  # Consider a change significant if it's more than 5 stocks
                    if change > threshold:
                        period_trend['changes'][f'{category}_trend'] = "IMPROVING"
                    elif change < -threshold:
                        period_trend['changes'][f'{category}_trend'] = "DETERIORATING"
                    else:
                        period_trend['changes'][f'{category}_trend'] = "STABLE"

            # Calculate aggregate bullish/bearish shifts
            bullish_change = period_trend['changes'].get('LONG', 0) + period_trend['changes'].get('ACCUMULATION', 0)
            bearish_change = period_trend['changes'].get('SHORT', 0) + period_trend['changes'].get('DISTRIBUTION', 0)

            period_trend['bullish_change'] = bullish_change
            period_trend['bearish_change'] = bearish_change

            # Determine overall distribution trend
            if bullish_change > 10 and bullish_change > abs(bearish_change):
                period_trend['overall_trend'] = "BULLISH_SHIFT"
            elif bearish_change > 10 and bearish_change > abs(bullish_change):
                period_trend['overall_trend'] = "BEARISH_SHIFT"
            elif abs(bullish_change) < 5 and abs(bearish_change) < 5:
                period_trend['overall_trend'] = "STABLE"
            else:
                period_trend['overall_trend'] = "MIXED"

            trends[f'{days}d'] = period_trend
            print(
                f"✅ {days}d trend: {period_trend['overall_trend']} (Bullish: {bullish_change:+.0f}, Bearish: {bearish_change:+.0f})")

        return {
            'history': sentiment_history,
            'periods': trends
        }

    def analyze_sector_sentiment_trends(self):
        """Analyze trends in sector sentiment across time periods"""
        print(f"\n🔍 Analyzing sector sentiment trends...")

        if not self.reports_data or len(self.reports_data) <= 1:
            print("❌ Insufficient data for sector sentiment trend analysis")
            return {'insufficient': True, 'reason': f'Only {len(self.reports_data)} reports available, need at least 2'}

        # Sort reports by date (newest first)
        sorted_reports = sorted(self.reports_data, key=lambda x: x['date'], reverse=True)

        # Collect all sectors across all reports
        all_sectors = set()
        for report in sorted_reports:
            sector_data = report.get('sectorSentiment', {})
            all_sectors.update(sector_data.keys())

        all_sectors = list(all_sectors)
        print(f"📊 Found {len(all_sectors)} sectors across all reports")

        # Create sector sentiment history
        sector_history = {
            'dates': [],
            'sectors': all_sectors,
            'data': {sector: {'bullish_pct': [], 'bearish_pct': [], 'avg_score': []} for sector in all_sectors}
        }

        for report in reversed(sorted_reports):  # Oldest to newest for time series
            sector_data = report.get('sectorSentiment', {})
            if not sector_data:
                print(f"⚠️  No sector sentiment data in report for {report['date']}")
                continue

            sector_history['dates'].append(report['date'])

            for sector in all_sectors:
                sector_info = sector_data.get(sector, {})

                # Calculate bullish percentage (LONG + ACCUMULATION)
                bullish_pct = sector_info.get('LONG', 0) + sector_info.get('ACCUMULATION', 0)

                # Calculate bearish percentage (SHORT + DISTRIBUTION)
                bearish_pct = sector_info.get('SHORT', 0) + sector_info.get('DISTRIBUTION', 0)

                # Calculate average sentiment score
                # Assign scores: LONG=2, ACCUMULATION=1, NEUTRAL=0, DISTRIBUTION=-1, SHORT=-2
                weights = {'LONG': 2, 'ACCUMULATION': 1, 'NEUTRAL': 0, 'DISTRIBUTION': -1, 'SHORT': -2}
                weighted_sum = sum(sector_info.get(category, 0) * weights[category]
                                   for category in ['LONG', 'ACCUMULATION', 'NEUTRAL', 'DISTRIBUTION', 'SHORT'])
                total_stocks = sum(sector_info.get(category, 0)
                                   for category in ['LONG', 'ACCUMULATION', 'NEUTRAL', 'DISTRIBUTION', 'SHORT'])

                avg_score = weighted_sum / total_stocks if total_stocks > 0 else 0

                # Store values
                sector_history['data'][sector]['bullish_pct'].append(bullish_pct)
                sector_history['data'][sector]['bearish_pct'].append(bearish_pct)
                sector_history['data'][sector]['avg_score'].append(avg_score)

        print(f"📈 Sector history compiled: {len(sector_history['dates'])} valid data points")

        # Calculate trends for each time period
        trends = {}

        for days in self.trend_periods:
            print(f"\n🔍 Analyzing {days}-day sector trends...")

            if len(sector_history['dates']) <= 1:
                trends[f'{days}d'] = {'insufficient': True,
                                      'reason': f'Only {len(sector_history["dates"])} data points available'}
                continue

            # Find index for comparison (or use the oldest available)
            compare_index = min(days, len(sector_history['dates']) - 1)

            period_trend = {
                'days': days,
                'startDate': sector_history['dates'][0],
                'endDate': sector_history['dates'][-1],
                'sector_changes': {}
            }

            # Calculate changes for each sector
            improving_sectors = []
            deteriorating_sectors = []

            for sector in all_sectors:
                if len(sector_history['data'][sector]['avg_score']) <= compare_index:
                    continue

                latest_score = sector_history['data'][sector]['avg_score'][-1]
                compare_score = sector_history['data'][sector]['avg_score'][
                    len(sector_history['data'][sector]['avg_score']) - 1 - compare_index]

                score_change = latest_score - compare_score

                # Determine trend direction
                threshold = 0.2  # Consider a change significant if avg_score changes by 0.2
                if score_change > threshold:
                    trend = "IMPROVING"
                    improving_sectors.append((sector, score_change))
                elif score_change < -threshold:
                    trend = "DETERIORATING"
                    deteriorating_sectors.append((sector, score_change))
                else:
                    trend = "STABLE"

                period_trend['sector_changes'][sector] = {
                    'score_change': score_change,
                    'trend': trend,
                    'latest_score': latest_score
                }

            # Sort by score change magnitude
            improving_sectors.sort(key=lambda x: x[1], reverse=True)
            deteriorating_sectors.sort(key=lambda x: x[1])

            # Get top 5 improving and deteriorating sectors
            period_trend['top_improving'] = [sector for sector, change in improving_sectors[:5]]
            period_trend['top_deteriorating'] = [sector for sector, change in deteriorating_sectors[:5]]

            trends[f'{days}d'] = period_trend
            print(
                f"✅ {days}d sector trends: {len(improving_sectors)} improving, {len(deteriorating_sectors)} deteriorating")

        return {
            'history': sector_history,
            'periods': trends
        }

    def create_test_chart(self, sentiment_trend_data):
        """Create a test chart to visualize sentiment distribution trends"""
        if not sentiment_trend_data or sentiment_trend_data.get('insufficient', False):
            print("❌ Cannot create chart - insufficient data")
            return None

        history = sentiment_trend_data.get('history', {})
        dates = history.get('dates', [])

        if not dates:
            print("❌ Cannot create chart - no dates available")
            return None

        # Create stacked area chart
        fig = go.Figure()

        # Define colors for sentiment categories
        colors = {
            'LONG': '#1E8449',  # Dark green
            'ACCUMULATION': '#82E0AA',  # Light green
            'NEUTRAL': '#F7DC6F',  # Yellow
            'DISTRIBUTION': '#F5B041',  # Orange
            'SHORT': '#C0392B'  # Red
        }

        # Add traces for each sentiment category
        for category in ['LONG', 'ACCUMULATION', 'NEUTRAL', 'DISTRIBUTION', 'SHORT']:
            fig.add_trace(go.Scatter(
                x=dates,
                y=history.get(category, []),
                mode='lines',
                stackgroup='one',
                name=category,
                line=dict(width=0.5, color=colors.get(category, '#333')),
                fillcolor=colors.get(category, '#333')
            ))

        # Update layout
        fig.update_layout(
            title='Sentiment Distribution Trend (Test)',
            xaxis_title='Date',
            yaxis_title='Number of Stocks',
            template='plotly_white',
            height=400
        )

        return fig

    def run_debug_analysis(self):
        """Run complete debug analysis"""
        print("🚀 Starting Distribution Trends Debug Analysis")
        print("=" * 60)

        # Load all reports
        report_count = self.load_all_reports()

        if report_count == 0:
            print("❌ No dashboard files found!")
            return False

        # Test sentiment distribution trends
        print(f"\n{'=' * 60}")
        print("🎯 TESTING SENTIMENT DISTRIBUTION TRENDS")
        print("=" * 60)

        sentiment_trends = self.analyze_sentiment_distribution_trends()

        if sentiment_trends.get('insufficient'):
            print(f"❌ Sentiment distribution analysis failed: {sentiment_trends.get('reason', 'Unknown reason')}")
        else:
            print("✅ Sentiment distribution analysis successful!")

            # Show sample results
            periods = sentiment_trends.get('periods', {})
            for period_key, period_data in periods.items():
                if not period_data.get('insufficient'):
                    print(f"   📊 {period_key}: {period_data.get('overall_trend', 'Unknown')} trend")

        # Test sector sentiment trends
        print(f"\n{'=' * 60}")
        print("🎯 TESTING SECTOR SENTIMENT TRENDS")
        print("=" * 60)

        sector_trends = self.analyze_sector_sentiment_trends()

        if sector_trends.get('insufficient'):
            print(f"❌ Sector sentiment analysis failed: {sector_trends.get('reason', 'Unknown reason')}")
        else:
            print("✅ Sector sentiment analysis successful!")

            # Show sample results
            periods = sector_trends.get('periods', {})
            for period_key, period_data in periods.items():
                if not period_data.get('insufficient'):
                    improving = len(period_data.get('top_improving', []))
                    deteriorating = len(period_data.get('top_deteriorating', []))
                    print(f"   📊 {period_key}: {improving} improving, {deteriorating} deteriorating sectors")

        # Create test visualizations
        print(f"\n{'=' * 60}")
        print("📈 CREATING TEST VISUALIZATIONS")
        print("=" * 60)

        chart = self.create_test_chart(sentiment_trends)
        if chart:
            print("✅ Test chart created successfully!")
            # Save chart as HTML for testing
            chart.write_html("test_sentiment_distribution_chart.html")
            print("💾 Chart saved as 'test_sentiment_distribution_chart.html'")

        # Summary
        print(f"\n{'=' * 60}")
        print("📋 SUMMARY")
        print("=" * 60)
        print(f"📄 Reports processed: {report_count}")
        print(f"📊 Sentiment trends: {'✅ Working' if not sentiment_trends.get('insufficient') else '❌ Failed'}")
        print(f"🏢 Sector trends: {'✅ Working' if not sector_trends.get('insufficient') else '❌ Failed'}")

        return True


def main():
    """Main function to run the debug analysis"""
    print("🔧 Distribution Trends Debugger")
    print("This script will help identify and fix issues with distribution trends analysis")
    print()

    # Check multiple common directories
    possible_directories = [
        "./output/progressive_analysis",  # Most likely location based on your earlier error
        "./output",
        "./",
        "./reports",
        "./data",
        "./analysis"
    ]

    print("🔍 Searching for dashboard files in multiple locations...")
    found_files = False
    reports_directory = "./"

    for directory in possible_directories:
        print(f"   📁 Checking: {os.path.abspath(directory)}")

        if os.path.exists(directory):
            import glob
            dashboard_files = glob.glob(os.path.join(directory, "market_dashboard_*.html"))
            if dashboard_files:
                print(f"   ✅ Found {len(dashboard_files)} dashboard files!")
                for file in dashboard_files[:3]:  # Show first 3 files
                    print(f"      📄 {os.path.basename(file)}")
                if len(dashboard_files) > 3:
                    print(f"      ... and {len(dashboard_files) - 3} more files")

                reports_directory = directory
                found_files = True
                break
            else:
                print(f"   ❌ No dashboard files found")
        else:
            print(f"   ❌ Directory doesn't exist")

    if not found_files:
        print(f"\n❌ No dashboard files found in any of the checked directories!")
        print(f"💡 Please ensure you have market_dashboard_YYYYMMDD.html files")
        print(f"💡 Current working directory: {os.path.abspath('.')}")

        # List all HTML files in current directory for debugging
        import glob
        all_html = glob.glob("*.html")
        if all_html:
            print(f"💡 Found these HTML files in current directory:")
            for html_file in all_html:
                print(f"   📄 {html_file}")

        return False

    print(f"\n🎯 Using directory: {os.path.abspath(reports_directory)}")

    debugger = DistributionTrendsDebugger(reports_directory)
    success = debugger.run_debug_analysis()

    if success:
        print(f"\n✅ Debug analysis completed!")
        print("💡 If you see 'insufficient data' errors, you need more historical dashboard files.")
        print("💡 The trend analysis requires at least 2 different dates to calculate changes.")
    else:
        print(f"\n❌ Debug analysis failed!")
        print("💡 Check that you have market_dashboard_YYYYMMDD.html files in the specified directory.")


if __name__ == "__main__":
    main()