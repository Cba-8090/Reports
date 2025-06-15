import os
import re
import glob
import json
import pandas as pd
import numpy as np
import argparse
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from bs4 import BeautifulSoup
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

class MarketTrendAnalyzer:
    """
    Analyzes trends across multiple market dashboard reports
    to provide insights on market direction and sentiment changes.
    """

    def __init__(self, reports_dir, output_dir=None, days_to_analyze=15):
        """
        Initialize the Market Trend Analyzer.

        Args:
            reports_dir (str): Directory containing market_dashboard_*.html files
            output_dir (str): Directory for output trend report
            days_to_analyze (int): Number of days to include in trend analysis
        """
        self.reports_dir = reports_dir
        self.output_dir = output_dir if output_dir else reports_dir
        self.days_to_analyze = days_to_analyze
        self.reports_data = []
        self.trend_periods = [7, 15, 30]  # Analyze trends for 7, 15, and 30 days

    def scan_dashboard_files(self):
        """
        Scan directory for market dashboard HTML files and sort by date.
        """
        # Find all dashboard HTML files
        dashboard_files = glob.glob(os.path.join(self.reports_dir, "market_dashboard_*.html"))
        
        # Extract date from filenames and create (file, date) pairs
        file_date_pairs = []
        for file_path in dashboard_files:
            filename = os.path.basename(file_path)
            match = re.search(r'market_dashboard_(\d{8})\.html', filename)
            if match:
                date_str = match.group(1)
                # Convert to datetime for proper sorting
                try:
                    file_date = datetime.strptime(date_str, "%Y%m%d")
                    file_date_pairs.append((file_path, file_date))
                except ValueError:
                    print(f"Warning: Invalid date format in filename: {filename}")
        
        # Sort by date (newest first)
        file_date_pairs.sort(key=lambda x: x[1], reverse=True)
        
        # Limit to the specified number of days
        file_date_pairs = file_date_pairs[:self.days_to_analyze]
        
        print(f"Found {len(file_date_pairs)} dashboard files for analysis")
        return file_date_pairs

    def extract_sentiment_distribution_from_html(self, html_file):
        """
        Extract sentiment distribution data from market dashboard HTML file.
        """
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

            # Try to extract sentiment distribution from JavaScript (sentimentFig)
            sentiment_match = re.search(r'const\s+sentimentFig\s*=\s*({.+?});', html_content, re.DOTALL)
            if sentiment_match:
                try:
                    sentiment_json = sentiment_match.group(1)
                    # Convert JavaScript object to valid JSON
                    sentiment_json = re.sub(r'(\w+):', r'"\1":', sentiment_json)
                    sentiment_data = json.loads(sentiment_json)

                    # Extract data from the sentiment chart (pie chart format)
                    if 'data' in sentiment_data and len(sentiment_data['data']) > 0:
                        data = sentiment_data['data'][0]
                        if 'x' in data and 'y' in data:
                            categories = data['x']
                            values = data['y']

                            print(f"Found sentiment categories: {categories}")
                            print(f"Found sentiment values: {values}")

                            for i, category in enumerate(categories):
                                if i < len(values) and category in distribution:
                                    distribution[category] = values[i]
                                    print(f"Set {category} = {values[i]}")

                            print(f"Successfully extracted distribution: {distribution}")
                            return distribution

                except Exception as e:
                    print(f"Error parsing sentiment distribution data: {e}")

            # Fallback: try to extract from HTML text content
            print("No sentiment chart data found, trying HTML content extraction...")
            soup = BeautifulSoup(html_content, 'html.parser')

            # Try to find total stocks first
            header_text = soup.select_one('.header p')
            total_stocks = 0
            if header_text:
                stocks_match = re.search(r'Total Stocks.*?(\d+)', header_text.text)
                if stocks_match:
                    total_stocks = int(stocks_match.group(1))
                    print(f"Found total stocks: {total_stocks}")

            # Try to find bullish/bearish percentages and estimate distribution
            summary_cards = soup.select('.summary-card')
            bullish_pct = 0
            bearish_pct = 0

            for card in summary_cards:
                description = card.select_one('.description')
                if description:
                    bullish_match = re.search(r'([\d.]+)%\s+Bullish', description.text)
                    bearish_match = re.search(r'([\d.]+)%\s+Bearish', description.text)

                    if bullish_match:
                        bullish_pct = float(bullish_match.group(1))
                        print(f"Found bullish percentage: {bullish_pct}%")
                    if bearish_match:
                        bearish_pct = float(bearish_match.group(1))
                        print(f"Found bearish percentage: {bearish_pct}%")

            if total_stocks > 0 and (bullish_pct > 0 or bearish_pct > 0):
                # Estimate distribution based on percentages
                bullish_stocks = int((bullish_pct / 100) * total_stocks)
                bearish_stocks = int((bearish_pct / 100) * total_stocks)
                neutral_stocks = total_stocks - bullish_stocks - bearish_stocks

                # Split bullish between ACCUMULATION and LONG (assume most are ACCUMULATION)
                distribution['ACCUMULATION'] = int(bullish_stocks * 0.9)
                distribution['LONG'] = bullish_stocks - distribution['ACCUMULATION']

                # Split bearish between SHORT and DISTRIBUTION (assume most are SHORT)
                distribution['SHORT'] = int(bearish_stocks * 0.9)
                distribution['DISTRIBUTION'] = bearish_stocks - distribution['SHORT']

                distribution['NEUTRAL'] = max(0, neutral_stocks)

                print(f"✓ Estimated distribution from percentages: {distribution}")

        except Exception as e:
            print(f"Error extracting sentiment distribution from {html_file}: {e}")
            import traceback
            traceback.print_exc()

        print(f"Final distribution: {distribution}")
        return distribution

    def extract_sector_sentiment_from_html(self, html_file):
        """
        Extract sector sentiment data from market dashboard HTML file.

        Args:
            html_file (str): Path to HTML file

        Returns:
            dict: Dictionary with sector sentiment data
        """
        sector_data = {}

        try:
            # Read HTML content
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
                except Exception as e:
                    print(f"Error parsing sector sentiment data: {e}")

        except Exception as e:
            print(f"Error extracting sector sentiment from {html_file}: {e}")

        return sector_data

    def extract_metrics_from_html(self, html_file):
        """
        Extract key metrics from a market dashboard HTML file.
        Updated to include sentiment distribution and sector sentiment data.

        Args:
            html_file (str): Path to HTML file

        Returns:
            dict: Dictionary of extracted metrics
        """
        # Call the original implementation to get basic metrics
        metrics = {
            'filename': os.path.basename(html_file),
            'date': '',
            'totalStocks': 0,
            'overallSentiment': {
                'score': 0,
                'bullishPercentage': 0,
                'bearishPercentage': 0
            },
            'alertDistribution': {
                'redAlerts': 0,
                'yellowAlerts': 0,
                'greenAlerts': 0
            },
            'institutionalFlow': {
                'fiiPositivePercentage': 0,
                'diiPositivePercentage': 0,
                'retailPositivePercentage': 0
            },
            'priceSentimentAlignment': {
                'percentage': 0
            },
            'sentimentDistribution': {},
            'sectorSentiment': {},
            'patternDistribution': {}
        }

        try:
            # Read HTML content
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Create BeautifulSoup object
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

            # Extract overall market sentiment
            sentiment_card = soup.select_one('.summary-card.bullish, .summary-card.bearish, .summary-card.neutral')
            if sentiment_card:
                value_elem = sentiment_card.select_one('.value')
                if value_elem:
                    metrics['overallSentiment']['score'] = float(value_elem.text.strip())

                description_elem = sentiment_card.select_one('.description')
                if description_elem:
                    bullish_match = re.search(r'([\d.]+)%\s+Bullish', description_elem.text)
                    bearish_match = re.search(r'([\d.]+)%\s+Bearish', description_elem.text)

                    if bullish_match:
                        metrics['overallSentiment']['bullishPercentage'] = float(bullish_match.group(1))
                    if bearish_match:
                        metrics['overallSentiment']['bearishPercentage'] = float(bearish_match.group(1))

            # Extract alert distribution (RED alerts)
            summary_cards = soup.select('.summary-card')
            for card in summary_cards:
                heading = card.select_one('h2')
                if heading and "Alert Distribution" in heading.text:
                    value_elem = card.select_one('.value')
                    if value_elem:
                        metrics['alertDistribution']['redAlerts'] = int(value_elem.text.strip())

            # Extract institutional flow (FII Positive Flow)
            for card in summary_cards:
                heading = card.select_one('h2')
                if heading and "Institutional Flow" in heading.text:
                    value_elem = card.select_one('.value')
                    if value_elem:
                        # Remove % sign if present and convert to float
                        flow_text = value_elem.text.strip().replace('%', '')
                        metrics['institutionalFlow']['fiiPositivePercentage'] = float(flow_text)

            # Extract price-sentiment alignment
            for card in summary_cards:
                heading = card.select_one('h2')
                if heading and "Price-Sentiment Alignment" in heading.text:
                    value_elem = card.select_one('.value')
                    if value_elem:
                        # Remove % sign if present and convert to float
                        alignment_text = value_elem.text.strip().replace('%', '')
                        metrics['priceSentimentAlignment']['percentage'] = float(alignment_text)

            # Extract sentiment distribution data
            metrics['sentimentDistribution'] = self.extract_sentiment_distribution_from_html(html_file)

            # Extract sector sentiment data
            metrics['sectorSentiment'] = self.extract_sector_sentiment_from_html(html_file)

            # Try to extract other charts data from embedded JavaScript
            chart_data = self.extract_chart_data(html_content)
            if chart_data:
                # Update metrics with any additional data from charts
                if 'institutionalFlow' in chart_data:
                    # Update with more detailed flow data if available
                    if 'dii' in chart_data['institutionalFlow']:
                        metrics['institutionalFlow']['diiPositivePercentage'] = chart_data['institutionalFlow']['dii']

                    if 'retail' in chart_data['institutionalFlow']:
                        metrics['institutionalFlow']['retailPositivePercentage'] = chart_data['institutionalFlow'][
                            'retail']

            return metrics

        except Exception as e:
            print(f"Error extracting metrics from {html_file}: {e}")
            import traceback
            traceback.print_exc()
            return metrics

    def analyze_sentiment_distribution_trends(self):
        """
        Analyze trends in sentiment distribution across time periods.

        Returns:
            dict: Analysis of sentiment distribution trends
        """
        if not self.reports_data or len(self.reports_data) <= 1:
            return {'insufficient': True}

        # Sort reports by date (newest first)
        sorted_reports = sorted(self.reports_data, key=lambda x: x['date'], reverse=True)

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
            if not distribution:
                continue

            sentiment_history['dates'].append(report['date'])
            for category in ['LONG', 'ACCUMULATION', 'NEUTRAL', 'DISTRIBUTION', 'SHORT']:
                sentiment_history[category].append(distribution.get(category, 0))

        # Calculate changes for different time periods
        trends = {}

        for days in self.trend_periods:
            if len(sentiment_history['dates']) <= 1:
                trends[f'{days}d'] = {'insufficient': True}
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

        return {
            'history': sentiment_history,
            'periods': trends
        }

    def analyze_sector_sentiment_trends(self):
        """
        Analyze trends in sector sentiment across time periods.

        Returns:
            dict: Analysis of sector sentiment trends
        """
        if not self.reports_data or len(self.reports_data) <= 1:
            return {'insufficient': True}

        # Sort reports by date (newest first)
        sorted_reports = sorted(self.reports_data, key=lambda x: x['date'], reverse=True)

        # Collect all sectors across all reports
        all_sectors = set()
        for report in sorted_reports:
            sector_data = report.get('sectorSentiment', {})
            all_sectors.update(sector_data.keys())

        all_sectors = list(all_sectors)

        # Create sector sentiment history
        sector_history = {
            'dates': [],
            'sectors': all_sectors,
            'data': {sector: {'bullish_pct': [], 'bearish_pct': [], 'avg_score': []} for sector in all_sectors}
        }

        for report in reversed(sorted_reports):  # Oldest to newest for time series
            sector_data = report.get('sectorSentiment', {})
            if not sector_data:
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

        # Calculate trends for each time period
        trends = {}

        for days in self.trend_periods:
            if len(sector_history['dates']) <= 1:
                trends[f'{days}d'] = {'insufficient': True}
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
                elif score_change < -threshold:
                    trend = "DETERIORATING"
                else:
                    trend = "STABLE"

                period_trend['sector_changes'][sector] = {
                    'score_change': score_change,
                    'trend': trend,
                    'latest_score': latest_score
                }

            # Find top improving and deteriorating sectors
            sector_changes = [(sector, data['score_change']) for sector, data in period_trend['sector_changes'].items()]

            # Sort by score change (descending for improving, ascending for deteriorating)
            improving_sectors = sorted(sector_changes, key=lambda x: x[1], reverse=True)
            deteriorating_sectors = sorted(sector_changes, key=lambda x: x[1])

            # Get top 5 improving and deteriorating sectors
            period_trend['top_improving'] = [sector for sector, change in improving_sectors[:5] if change > threshold]
            period_trend['top_deteriorating'] = [sector for sector, change in deteriorating_sectors[:5] if
                                                 change < -threshold]

            trends[f'{days}d'] = period_trend

        return {
            'history': sector_history,
            'periods': trends
        }




    def extract_chart_data(self, html_content):
        """
        Extract chart data from JavaScript variables in the HTML.
        
        Args:
            html_content (str): HTML content
            
        Returns:
            dict: Dictionary of extracted chart data
        """
        chart_data = {}
        
        # Try to extract sentiment chart data
        sentiment_match = re.search(r'const\s+sentimentFig\s*=\s*({.+?});', html_content, re.DOTALL)
        if sentiment_match:
            try:
                sentiment_json = sentiment_match.group(1)
                # Convert JavaScript object to valid JSON
                # This might require additional processing for complex objects
                sentiment_json = re.sub(r'(\w+):', r'"\1":', sentiment_json)
                sentiment_data = json.loads(sentiment_json)
                
                # Extract sentiment distribution if available
                if 'data' in sentiment_data:
                    sentiment_dist = {}
                    for trace in sentiment_data['data']:
                        if isinstance(trace, dict) and 'x' in trace and 'y' in trace:
                            categories = trace.get('x', [])
                            values = trace.get('y', [])
                            
                            for i, category in enumerate(categories):
                                if i < len(values):
                                    sentiment_dist[category] = values[i]
                    
                    chart_data['sentimentDistribution'] = sentiment_dist
            except Exception as e:
                print(f"Error parsing sentiment chart data: {e}")
        
        # Try to extract sector chart data
        sector_match = re.search(r'const\s+sectorFig\s*=\s*({.+?});', html_content, re.DOTALL)
        if sector_match:
            try:
                sector_json = sector_match.group(1)
                # Convert JavaScript object to valid JSON
                sector_json = re.sub(r'(\w+):', r'"\1":', sector_json)
                sector_data = json.loads(sector_json)
                
                # Process sector data (this would depend on the specific format)
                chart_data['sectorSentiment'] = {'raw': sector_data}
            except Exception as e:
                print(f"Error parsing sector chart data: {e}")
        
        # Try to extract flow chart data
        flow_match = re.search(r'const\s+flowFig\s*=\s*({.+?});', html_content, re.DOTALL)
        if flow_match:
            try:
                flow_json = flow_match.group(1)
                # Convert JavaScript object to valid JSON
                flow_json = re.sub(r'(\w+):', r'"\1":', flow_json)
                flow_data = json.loads(flow_json)
                
                # Extract flow data if available
                chart_data['institutionalFlow'] = {'raw': flow_data}
            except Exception as e:
                print(f"Error parsing flow chart data: {e}")
        
        return chart_data

    def analyze_market_trends(self):
        """
        Analyze trends across multiple dashboard reports.
        Updated to include sentiment distribution and sector sentiment trends.

        Returns:
            dict: Analysis results including trends for different time periods
        """
        if not self.reports_data:
            print("No report data available for analysis")
            return None

        # Sort reports by date (newest first)
        self.reports_data.sort(key=lambda x: x['date'], reverse=True)

        analysis = {
            'latestReport': self.reports_data[0],
            'periods': {},
            'metrics_history': {
                'dates': [],
                'sentiment_scores': [],
                'bullish_percentages': [],
                'bearish_percentages': [],
                'red_alerts': [],
                'fii_positive': [],
                'price_sentiment_alignment': []
            }
        }

        # Create historical data for charts
        for report in reversed(self.reports_data):  # Oldest to newest for time series
            analysis['metrics_history']['dates'].append(report['date'])
            analysis['metrics_history']['sentiment_scores'].append(report['overallSentiment']['score'])
            analysis['metrics_history']['bullish_percentages'].append(report['overallSentiment']['bullishPercentage'])
            analysis['metrics_history']['bearish_percentages'].append(report['overallSentiment']['bearishPercentage'])
            analysis['metrics_history']['red_alerts'].append(report['alertDistribution']['redAlerts'])
            analysis['metrics_history']['fii_positive'].append(report['institutionalFlow']['fiiPositivePercentage'])
            analysis['metrics_history']['price_sentiment_alignment'].append(
                report['priceSentimentAlignment']['percentage'])

        # For each trend period (7d, 15d, 30d)
        for days in self.trend_periods:
            if len(self.reports_data) <= 1:
                analysis['periods'][f'{days}d'] = {'insufficient': True}
                continue

            # Find report from 'days' ago or the oldest available
            compare_index = min(days, len(self.reports_data) - 1)
            compare_report = self.reports_data[compare_index]
            latest_report = self.reports_data[0]

            period_analysis = {
                'days': days,
                'startDate': compare_report['date'],
                'endDate': latest_report['date'],
                'metrics': {}
            }

            # Calculate changes for key metrics
            period_analysis['metrics']['overallSentiment'] = {
                'scoreChange': latest_report['overallSentiment']['score'] - compare_report['overallSentiment']['score'],
                'bullishChange': latest_report['overallSentiment']['bullishPercentage'] -
                                 compare_report['overallSentiment']['bullishPercentage'],
                'bearishChange': latest_report['overallSentiment']['bearishPercentage'] -
                                 compare_report['overallSentiment']['bearishPercentage']
            }

            period_analysis['metrics']['alertDistribution'] = {
                'redAlertsChange': latest_report['alertDistribution']['redAlerts'] -
                                   compare_report['alertDistribution']['redAlerts']
            }

            period_analysis['metrics']['institutionalFlow'] = {
                'fiiPositiveChange': latest_report['institutionalFlow']['fiiPositivePercentage'] -
                                     compare_report['institutionalFlow']['fiiPositivePercentage']
            }

            period_analysis['metrics']['priceSentimentAlignment'] = {
                'change': latest_report['priceSentimentAlignment']['percentage'] -
                          compare_report['priceSentimentAlignment']['percentage']
            }

            # Get the trend direction for each metric
            period_analysis['metrics']['overallSentiment']['trend'] = self.get_trend_direction(
                period_analysis['metrics']['overallSentiment']['scoreChange'],
                0.1  # Threshold for significant change
            )

            period_analysis['metrics']['alertDistribution']['trend'] = self.get_trend_direction(
                -period_analysis['metrics']['alertDistribution']['redAlertsChange'],
                # Negative because fewer red alerts is better
                5  # Threshold for significant change
            )

            period_analysis['metrics']['institutionalFlow']['trend'] = self.get_trend_direction(
                period_analysis['metrics']['institutionalFlow']['fiiPositiveChange'],
                5  # Threshold percentage points
            )

            period_analysis['metrics']['priceSentimentAlignment']['trend'] = self.get_trend_direction(
                period_analysis['metrics']['priceSentimentAlignment']['change'],
                5  # Threshold percentage points
            )

            # Calculate overall market trend based on weighted metrics
            sentiment_weight = 3.0
            alert_weight = 0.05
            flow_weight = 0.1
            alignment_weight = 0.1

            trendScores = [
                period_analysis['metrics']['overallSentiment']['scoreChange'] * sentiment_weight,
                -period_analysis['metrics']['alertDistribution']['redAlertsChange'] * alert_weight,
                period_analysis['metrics']['institutionalFlow']['fiiPositiveChange'] * flow_weight,
                period_analysis['metrics']['priceSentimentAlignment']['change'] * alignment_weight
            ]

            totalScore = sum(trendScores)

            # Determine overall trend
            if totalScore > 0.2:
                period_analysis['overallTrend'] = "IMPROVING"
            elif totalScore < -0.2:
                period_analysis['overallTrend'] = "DETERIORATING"
            else:
                period_analysis['overallTrend'] = "STABLE"

            # Add trend strength (1-5)
            absScore = abs(totalScore)
            if absScore > 1:
                period_analysis['trendStrength'] = 5
            elif absScore > 0.8:
                period_analysis['trendStrength'] = 4
            elif absScore > 0.5:
                period_analysis['trendStrength'] = 3
            elif absScore > 0.2:
                period_analysis['trendStrength'] = 2
            else:
                period_analysis['trendStrength'] = 1

            analysis['periods'][f'{days}d'] = period_analysis

        # Add sentiment distribution and sector sentiment trend analysis
        analysis['sentiment_distribution_trends'] = self.analyze_sentiment_distribution_trends()
        analysis['sector_sentiment_trends'] = self.analyze_sector_sentiment_trends()

        return analysis

    def create_sentiment_distribution_trend_chart(self, sentiment_trend_data):
        """
        Create a chart showing sentiment distribution trend over time.

        Args:
            sentiment_trend_data (dict): Historical sentiment distribution data

        Returns:
            Figure: Plotly figure
        """
        if not sentiment_trend_data or sentiment_trend_data.get('insufficient', False):
            # Create an empty chart with message if no data
            fig = go.Figure()
            fig.add_annotation(
                text="Insufficient data for sentiment distribution trend",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False
            )
            fig.update_layout(
                title='Sentiment Distribution Trend',
                template='plotly_white',
                height=400
            )
            return fig

        history = sentiment_trend_data.get('history', {})
        dates = history.get('dates', [])

        if not dates:
            # Create an empty chart with message if no dates
            fig = go.Figure()
            fig.add_annotation(
                text="No historical data available for sentiment distribution",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False
            )
            fig.update_layout(
                title='Sentiment Distribution Trend',
                template='plotly_white',
                height=400
            )
            return fig

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
            title='Sentiment Distribution Trend',
            xaxis_title='Date',
            yaxis_title='Number of Stocks',
            template='plotly_white',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=50, r=50, t=80, b=50),
            height=400
        )

        return fig

    def create_sector_sentiment_trend_chart(self, sector_trend_data, period='7d'):
        """
        Create a chart showing sector sentiment trend changes.

        Args:
            sector_trend_data (dict): Sector sentiment trend data
            period (str): Time period to display (e.g., '7d', '15d', '30d')

        Returns:
            Figure: Plotly figure
        """
        if not sector_trend_data or sector_trend_data.get('insufficient', False):
            # Create an empty chart with message if no data
            fig = go.Figure()
            fig.add_annotation(
                text="Insufficient data for sector sentiment trend",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False
            )
            fig.update_layout(
                title='Sector Sentiment Trend',
                template='plotly_white',
                height=400
            )
            return fig

        periods = sector_trend_data.get('periods', {})
        period_data = periods.get(period, {})

        if not period_data or period_data.get('insufficient', False):
            # Create an empty chart with message if no period data
            fig = go.Figure()
            fig.add_annotation(
                text=f"Insufficient data for {period} sector sentiment trend",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False
            )
            fig.update_layout(
                title=f'Sector Sentiment Trend ({period})',
                template='plotly_white',
                height=400
            )
            return fig

        sector_changes = period_data.get('sector_changes', {})

        # Create a list of sectors and their changes
        sectors = []
        changes = []
        colors = []

        for sector, data in sector_changes.items():
            sectors.append(sector)
            changes.append(data.get('score_change', 0))

            # Determine color based on trend
            if data.get('trend') == 'IMPROVING':
                colors.append('#1E8449')  # Green
            elif data.get('trend') == 'DETERIORATING':
                colors.append('#C0392B')  # Red
            else:
                colors.append('#F7DC6F')  # Yellow

        # Sort by change value
        sorted_data = sorted(zip(sectors, changes, colors), key=lambda x: x[1])

        # Get top 10 sectors with most change in either direction
        #if len(sorted_data) > 10:
            # Take 5 most deteriorating and 5 most improving
         #   sorted_data = sorted_data[:5] + sorted_data[-5:]

        if len(sorted_data) > 20:  # Changed from 10 to 20
            # Take 10 most deteriorating and 10 most improving
            sorted_data = sorted_data[:10] + sorted_data[-10:]  # Changed from 5 to 10


        sectors, changes, colors = zip(*sorted_data) if sorted_data else ([], [], [])

        # Create horizontal bar chart
        fig = go.Figure()

        # Add bars
        fig.add_trace(go.Bar(
            y=sectors,
            x=changes,
            orientation='h',
            marker_color=colors,
            text=[f"{change:.2f}" for change in changes],
            textposition='auto'
        ))

        # Add reference line at zero
        fig.add_shape(
            type="line",
            x0=0, x1=0,
            y0=-0.5, y1=len(sectors) - 0.5,
            line=dict(color="gray", width=1, dash="dash")
        )

        # Update layout
        fig.update_layout(
            title=f'Sector Sentiment Changes ({period})',
            xaxis_title='Change in Sentiment Score',
            yaxis_title='Sector',
            template='plotly_white',
            margin=dict(l=150, r=50, t=80, b=50),
            height=400
        )

        return fig

    def generate_sentiment_distribution_trend_html(self, sentiment_trend_data, period='7d'):
        """
        Generate HTML for sentiment distribution trend section.

        Args:
            sentiment_trend_data (dict): Sentiment distribution trend data
            period (str): Time period to display

        Returns:
            str: HTML content for sentiment distribution trend section
        """
        if not sentiment_trend_data or sentiment_trend_data.get('insufficient', True):
            return "<p>Insufficient data for sentiment distribution trend analysis. More historical reports needed.</p>"

        periods = sentiment_trend_data.get('periods', {})
        period_data = periods.get(period, {})

        if not period_data or period_data.get('insufficient', True):
            return f"<p>Insufficient data for {period} sentiment distribution trend analysis.</p>"

        # Get overall trend
        overall_trend = period_data.get('overall_trend', 'STABLE')
        trend_emoji = "📈" if overall_trend == "BULLISH_SHIFT" else "📉" if overall_trend == "BEARISH_SHIFT" else "➡️"

        trend_class = "improving" if overall_trend == "BULLISH_SHIFT" else "deteriorating" if overall_trend == "BEARISH_SHIFT" else "stable"

        html = f"""
        <p><strong>Analysis Period:</strong> {period_data.get('startDate', '')} to {period_data.get('endDate', '')}</p>
        <p><strong>Overall Distribution Trend:</strong> <span class="{trend_class}">{trend_emoji} {overall_trend.replace('_', ' ')}</span></p>

        <h3>Sentiment Category Changes</h3>
        <ul>
        """

        categories = ['LONG', 'ACCUMULATION', 'NEUTRAL', 'DISTRIBUTION', 'SHORT']
        changes = period_data.get('changes', {})

        for category in categories:
            change = changes.get(category, 0)
            trend = changes.get(f'{category}_trend', 'STABLE')

            trend_class = trend.lower()
            trend_emoji = "📈" if trend == "IMPROVING" else "📉" if trend == "DETERIORATING" else "➡️"

            html += f"""
                <li><strong>{category}:</strong> <span class="{trend_class}">{trend_emoji} {change:+.0f} stocks</span></li>
            """

        # Add bullish/bearish summary
        bullish_change = period_data.get('bullish_change', 0)
        bearish_change = period_data.get('bearish_change', 0)

        bullish_class = "improving" if bullish_change > 0 else "deteriorating" if bullish_change < 0 else "stable"
        bearish_class = "improving" if bearish_change < 0 else "deteriorating" if bearish_change > 0 else "stable"

        html += f"""
            </ul>

            <h3>Distribution Shift Summary</h3>
            <ul>
                <li><strong>Net Bullish Change:</strong> <span class="{bullish_class}">{bullish_change:+.0f} stocks</span></li>
                <li><strong>Net Bearish Change:</strong> <span class="{bearish_class}">{bearish_change:+.0f} stocks</span></li>
            </ul>
        """

        # Add key insights
        insights = []

        if bullish_change > 10:
            insights.append(
                "Significant increase in bullish sentiment distribution indicates strengthening market confidence")
        elif bullish_change < -10:
            insights.append("Notable decrease in bullish sentiment distribution suggests weakening market confidence")

        if bearish_change > 10:
            insights.append("Substantial increase in bearish sentiment distribution indicates growing market concerns")
        elif bearish_change < -10:
            insights.append(
                "Considerable reduction in bearish sentiment distribution suggests diminishing market fears")

        if changes.get('LONG', 0) > 10:
            insights.append("Strong growth in LONG sentiment category points to increasing conviction among buyers")
        elif changes.get('LONG', 0) < -10:
            insights.append(
                "Significant decline in LONG sentiment category indicates decreasing conviction among buyers")

        if changes.get('SHORT', 0) > 10:
            insights.append("Notable rise in SHORT sentiment category suggests increasing conviction among sellers")
        elif changes.get('SHORT', 0) < -10:
            insights.append(
                "Marked reduction in SHORT sentiment category points to decreasing conviction among sellers")

        if changes.get('NEUTRAL', 0) > 15:
            insights.append("Large shift toward NEUTRAL sentiment indicates growing uncertainty in the market")

        if len(insights) < 2:
            if overall_trend == "BULLISH_SHIFT":
                insights.append(
                    "The overall shift toward more bullish sentiment distribution may precede price appreciation")
            elif overall_trend == "BEARISH_SHIFT":
                insights.append(
                    "The overall shift toward more bearish sentiment distribution may signal caution is warranted")
            else:
                insights.append("The stable sentiment distribution suggests market participants remain in equilibrium")

        if insights:
            html += """
            <h3>Key Insights</h3>
            <ul>
            """

            for insight in insights:
                html += f"<li>{insight}</li>"

            html += """
            </ul>
            """

        return html

    def generate_sector_sentiment_trend_html(self, sector_trend_data, period='7d'):
        """
        Generate HTML for sector sentiment trend section.

        Args:
            sector_trend_data (dict): Sector sentiment trend data
            period (str): Time period to display

        Returns:
            str: HTML content for sector sentiment trend section
        """
        if not sector_trend_data or sector_trend_data.get('insufficient', True):
            return "<p>Insufficient data for sector sentiment trend analysis. More historical reports needed.</p>"

        periods = sector_trend_data.get('periods', {})
        period_data = periods.get(period, {})

        if not period_data or period_data.get('insufficient', True):
            return f"<p>Insufficient data for {period} sector sentiment trend analysis.</p>"

        # Get top improving and deteriorating sectors
        top_improving = period_data.get('top_improving', [])
        top_deteriorating = period_data.get('top_deteriorating', [])

        html = f"""
        <p><strong>Analysis Period:</strong> {period_data.get('startDate', '')} to {period_data.get('endDate', '')}</p>

        <h3>Sector Sentiment Trends</h3>
        """

        if top_improving:
            html += """
            <h4>📈 Top Improving Sectors</h4>
            <ul>
            """

            for sector in top_improving:
                data = period_data.get('sector_changes', {}).get(sector, {})
                change = data.get('score_change', 0)
                latest = data.get('latest_score', 0)

                html += f"""
                <li><strong>{sector}:</strong> <span class="improving">+{change:.2f}</span> (Current: {latest:.2f})</li>
                """

            html += """
            </ul>
            """
        else:
            html += "<p>No significantly improving sectors found in this time period.</p>"

        if top_deteriorating:
            html += """
            <h4>📉 Top Deteriorating Sectors</h4>
            <ul>
            """

            for sector in top_deteriorating:
                data = period_data.get('sector_changes', {}).get(sector, {})
                change = data.get('score_change', 0)
                latest = data.get('latest_score', 0)

                html += f"""
                <li><strong>{sector}:</strong> <span class="deteriorating">{change:.2f}</span> (Current: {latest:.2f})</li>
                """

            html += """
            </ul>
            """
        else:
            html += "<p>No significantly deteriorating sectors found in this time period.</p>"

        # Add key insights
        insights = []

        if top_improving and top_deteriorating:
            insights.append("Market shows a clear sector rotation pattern with distinct winners and losers")
        elif top_improving and not top_deteriorating:
            insights.append("Broad-based improvement across sectors suggests a healthy market environment")
        elif not top_improving and top_deteriorating:
            insights.append("Weakness across multiple sectors indicates broad market concerns")
        else:
            insights.append("Relatively stable sector sentiment indicates a balanced market environment")

        if top_improving:
            top_sector = top_improving[0]
            insights.append(
                f"The {top_sector} sector shows the strongest sentiment improvement and may lead the market")

        if top_deteriorating:
            bottom_sector = top_deteriorating[0]
            insights.append(
                f"The {bottom_sector} sector shows significant sentiment deterioration and may underperform")

        if top_improving and top_deteriorating:
            common_themes = []
            if any("Tech" in sector or "Technology" in sector for sector in top_improving):
                if any("Energy" in sector or "Utilities" in sector for sector in top_deteriorating):
                    common_themes.append("Growth orientation (Technology improving, Energy/Utilities deteriorating)")

            if any("Energy" in sector or "Materials" in sector or "Mining" in sector for sector in top_improving):
                if any("Tech" in sector or "Technology" in sector for sector in top_deteriorating):
                    common_themes.append(
                        "Value/Commodity orientation (Energy/Materials improving, Technology deteriorating)")

            if any("Consumer" in sector and "Discretionary" in sector for sector in top_improving):
                if any("Consumer" in sector and "Staples" in sector for sector in top_deteriorating):
                    common_themes.append(
                        "Risk-on sentiment (Consumer Discretionary improving, Consumer Staples deteriorating)")

            if any("Consumer" in sector and "Staples" in sector for sector in top_improving):
                if any("Consumer" in sector and "Discretionary" in sector for sector in top_deteriorating):
                    common_themes.append(
                        "Defensive positioning (Consumer Staples improving, Consumer Discretionary deteriorating)")

            if common_themes:
                insights.append(f"Sector rotation indicates: {', '.join(common_themes)}")

        if insights:
            html += """
            <h3>Key Insights</h3>
            <ul>
            """

            for insight in insights:
                html += f"<li>{insight}</li>"

            html += """
            </ul>
            """

        return html







    def get_trend_direction(self, change, threshold):
        """
        Determine the trend direction based on change value.
        
        Args:
            change (float): The change value
            threshold (float): Threshold for significant change
            
        Returns:
            str: "IMPROVING", "DETERIORATING", or "STABLE"
        """
        if change > threshold:
            return "IMPROVING"
        elif change < -threshold:
            return "DETERIORATING"
        else:
            return "STABLE"

    def generate_insights(self, period_data):
        """
        Generate insights based on trend data.
        
        Args:
            period_data (dict): Period analysis data
            
        Returns:
            list: List of insight statements
        """
        insights = []
        
        # Sentiment insights
        sentiment_change = period_data['metrics']['overallSentiment']
        if sentiment_change['trend'] == "IMPROVING" and sentiment_change['scoreChange'] > 0.2:
            insights.append("Significant improvement in market sentiment indicates growing investor confidence")
        elif sentiment_change['trend'] == "DETERIORATING" and sentiment_change['scoreChange'] < -0.2:
            insights.append("Notable deterioration in market sentiment suggests increasing investor caution")
        
        if sentiment_change['bullishChange'] > 10:
            insights.append("Strong increase in bullish sentiment could signal potential upside momentum")
        elif sentiment_change['bearishChange'] > 10:
            insights.append("Significant rise in bearish sentiment may indicate increased downside risk")
        
        # Alert insights
        alert_change = period_data['metrics']['alertDistribution']
        if alert_change['redAlertsChange'] > 10:
            insights.append("Concerning increase in RED alerts suggests growing risks across the market")
        elif alert_change['redAlertsChange'] < -10:
            insights.append("Substantial reduction in RED alerts indicates improving market conditions")
        
        # Institutional flow insights
        flow_change = period_data['metrics']['institutionalFlow']
        if flow_change['fiiPositiveChange'] > 10:
            insights.append("Foreign institutional investors are significantly increasing their positive flows")
        elif flow_change['fiiPositiveChange'] < -10:
            insights.append("Foreign institutional investors are notably reducing their positive flows")
        
        # Price-sentiment alignment insights
        alignment_change = period_data['metrics']['priceSentimentAlignment']
        if alignment_change['change'] > 10:
            insights.append("Increasing price-sentiment alignment suggests more predictable market behavior")
        elif alignment_change['change'] < -10:
            insights.append("Declining price-sentiment alignment may indicate potential market turbulence")
        
        # Trend combination insights
        if sentiment_change['trend'] == "IMPROVING" and alert_change['trend'] == "IMPROVING":
            insights.append("Coincident improvement in sentiment and reduction in alerts strengthens the positive outlook")
        elif sentiment_change['trend'] == "DETERIORATING" and alert_change['trend'] == "DETERIORATING":
            insights.append("Combined deterioration in sentiment and increase in alerts reinforces the negative outlook")
        
        if sentiment_change['trend'] == "IMPROVING" and flow_change['trend'] == "DETERIORATING":
            insights.append("Divergence between improving sentiment and deteriorating institutional flows warrants caution")
        elif sentiment_change['trend'] == "DETERIORATING" and flow_change['trend'] == "IMPROVING":
            insights.append("Institutional flows improving despite deteriorating sentiment could signal a potential turnaround")
        
        # Return at least 3 insights
        if len(insights) < 3:
            insights.append("Monitor for changes in institutional flow patterns as they can precede market moves")
            insights.append("Consider sector rotation strategies based on the evolving sentiment landscape")
            insights.append("Watch for divergences between price action and sentiment as potential trading opportunities")
        
        return insights

    def create_trend_report(self, analysis):
        """
        Generate a comprehensive trend analysis report.
        Updated to include sentiment distribution and sector sentiment trend sections.

        Args:
            analysis (dict): Analysis results

        Returns:
            str: HTML content for the trend report
        """
        if not analysis:
            return "<html><body><h1>No data available for trend analysis</h1></body></html>"

        latest = analysis['latestReport']

        # Create charts for the report
        sentiment_chart = self.create_sentiment_history_chart(analysis['metrics_history'])
        alerts_chart = self.create_alerts_history_chart(analysis['metrics_history'])
        flow_chart = self.create_flow_history_chart(analysis['metrics_history'])
        alignment_chart = self.create_alignment_history_chart(analysis['metrics_history'])

        # Create new trend charts
        sentiment_dist_chart = self.create_sentiment_distribution_trend_chart(
            analysis.get('sentiment_distribution_trends', {}))
        sector_chart = self.create_sector_sentiment_trend_chart(analysis.get('sector_sentiment_trends', {}), '7d')

        # Convert charts to JSON for embedding
        sentiment_json = json.dumps(sentiment_chart.to_dict())
        alerts_json = json.dumps(alerts_chart.to_dict())
        flow_json = json.dumps(flow_chart.to_dict())
        alignment_json = json.dumps(alignment_chart.to_dict())
        sentiment_dist_json = json.dumps(sentiment_dist_chart.to_dict())
        sector_json = json.dumps(sector_chart.to_dict())

        # Determine overall market outlook
        outlook = "Neutral"
        outlook_description = "Neutral"

        # Get the most recent period (7d)
        recent_period = analysis['periods'].get('7d')
        if recent_period and not recent_period.get('insufficient'):
            outlook_map = {
                "IMPROVING": "Bullish",
                "DETERIORATING": "Bearish",
                "STABLE": "Neutral"
            }

            outlook = outlook_map[recent_period['overallTrend']]
            strength = recent_period['trendStrength']

            if outlook == "Bullish":
                outlook_description = "Strongly Bullish" if strength >= 4 else "Moderately Bullish" if strength >= 2 else "Slightly Bullish"
            elif outlook == "Bearish":
                outlook_description = "Strongly Bearish" if strength >= 4 else "Moderately Bearish" if strength >= 2 else "Slightly Bearish"
            else:
                outlook_description = "Neutral"

        # Create the HTML content
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Market Trend Analysis</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                :root {{
                    --primary-color: #2C3E50;
                    --secondary-color: #3498DB;
                    --accent-color: #F39C12;
                    --success-color: #1E8449;
                    --warning-color: #F7DC6F;
                    --danger-color: #C0392B;
                    --neutral-color: #ECF0F1;
                    --bullish-color: #1E8449;
                    --bearish-color: #C0392B;
                    --neutral-bgcolor: #f9f9f9;
                }}

                * {{
                    box-sizing: border-box;
                    margin: 0;
                    padding: 0;
                    font-family: Arial, sans-serif;
                }}

                body {{
                    background-color: var(--neutral-bgcolor);
                    color: #333;
                    line-height: 1.6;
                }}

                .container {{
                    max-width: 1400px;
                    margin: 0 auto;
                    padding: 20px;
                }}

                .header {{
                    background-color: var(--primary-color);
                    color: white;
                    padding: 20px;
                    margin-bottom: 20px;
                    border-radius: 5px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                }}

                .header h1 {{
                    margin: 0;
                    font-size: 24px;
                }}

                .header p {{
                    margin: 5px 0 0;
                    font-size: 14px;
                    opacity: 0.8;
                }}

                .dashboard-summary {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin-bottom: 20px;
                }}

                .summary-card {{
                    background: white;
                    border-radius: 5px;
                    padding: 20px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                    text-align: center;
                }}

                .summary-card.bullish {{
                    border-top: 4px solid var(--bullish-color);
                }}

                .summary-card.bearish {{
                    border-top: 4px solid var(--bearish-color);
                }}

                .summary-card.neutral {{
                    border-top: 4px solid var(--accent-color);
                }}

                .summary-card h2 {{
                    font-size: 16px;
                    margin-bottom: 10px;
                    color: #666;
                }}

                .summary-card .value {{
                    font-size: 24px;
                    font-weight: bold;
                    margin-bottom: 5px;
                }}

                .summary-card .value.bullish {{
                    color: var(--bullish-color);
                }}

                .summary-card .value.bearish {{
                    color: var(--bearish-color);
                }}

                .summary-card .description {{
                    font-size: 14px;
                    color: #666;
                }}

                .tabs {{
                    display: flex;
                    border-bottom: 1px solid #ddd;
                    margin-bottom: 20px;
                }}

                .tab-button {{
                    padding: 10px 20px;
                    border: none;
                    background: none;
                    cursor: pointer;
                    font-size: 16px;
                    font-weight: 500;
                    color: #666;
                    border-bottom: 3px solid transparent;
                }}

                .tab-button:hover {{
                    background-color: #f5f5f5;
                }}

                .tab-button.active {{
                    color: var(--primary-color);
                    border-bottom-color: var(--primary-color);
                }}

                .tab-content {{
                    display: none;
                }}

                .tab-content.active {{
                    display: block;
                }}

                .chart-row {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
                    gap: 20px;
                    margin-bottom: 20px;
                }}

                .chart-container {{
                    background: white;
                    border-radius: 5px;
                    padding: 20px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                }}

                .chart-title {{
                    font-size: 18px;
                    margin-bottom: 15px;
                    color: var(--primary-color);
                }}

                .plot-container {{
                    height: 400px;
                }}

                .trend-section {{
                    background: white;
                    border-radius: 5px;
                    padding: 20px;
                    margin-bottom: 20px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                }}

                .trend-section h2 {{
                    font-size: 20px;
                    margin-bottom: 15px;
                    color: var(--primary-color);
                }}

                .trend-section h3 {{
                    font-size: 18px;
                    margin: 15px 0 10px;
                    color: #444;
                }}

                .trend-section h4 {{
                    font-size: 16px;
                    margin: 12px 0 8px;
                    color: #555;
                }}

                .trend-section p {{
                    margin-bottom: 10px;
                }}

                .trend-section ul {{
                    margin-left: 20px;
                    margin-bottom: 15px;
                }}

                .trend-section li {{
                    margin-bottom: 5px;
                }}

                .improving {{
                    color: var(--bullish-color);
                }}

                .deteriorating {{
                    color: var(--bearish-color);
                }}

                .stable {{
                    color: var(--accent-color);
                }}

                .footer {{
                    text-align: center;
                    margin-top: 50px;
                    padding: 20px;
                    color: #666;
                    font-size: 14px;
                }}

                /* Responsive adjustments */
                @media (max-width: 768px) {{
                    .chart-row {{
                        grid-template-columns: 1fr;
                    }}

                    .tab-button {{
                        padding: 10px;
                        font-size: 14px;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Market Trend Analysis Report</h1>
                    <p>Analysis Date: {datetime.now().strftime("%Y-%m-%d")} | Latest Market Data: {latest['date']} | Total Stocks: {latest['totalStocks']}</p>
                </div>

                <div class="dashboard-summary">
                    <div class="summary-card {outlook.lower()}">
                        <h2>Market Outlook</h2>
                        <div class="value {outlook.lower()}">{outlook_description}</div>
                        <div class="description">
                            Based on {recent_period['days']}d trend analysis
                        </div>
                    </div>

                    <div class="summary-card">
                        <h2>Overall Sentiment</h2>
                        <div class="value">{latest['overallSentiment']['score']:.2f}</div>
                        <div class="description">
                            {latest['overallSentiment']['bullishPercentage']:.1f}% Bullish | {latest['overallSentiment']['bearishPercentage']:.1f}% Bearish
                        </div>
                    </div>

                    <div class="summary-card">
                        <h2>RED Alerts</h2>
                        <div class="value">{latest['alertDistribution']['redAlerts']}</div>
                        <div class="description">
                            Stocks with critical alerts
                        </div>
                    </div>

                    <div class="summary-card">
                        <h2>FII Flow</h2>
                        <div class="value {('bullish' if latest['institutionalFlow']['fiiPositivePercentage'] > 50 else 'bearish')}">
                            {latest['institutionalFlow']['fiiPositivePercentage']:.1f}%
                        </div>
                        <div class="description">
                            FII Positive Flow
                        </div>
                    </div>
                </div>

                <div class="tabs">
                    <button class="tab-button active" onclick="openTab(event, 'trend-tab')">Trend Analysis</button>
                    <button class="tab-button" onclick="openTab(event, 'distribution-tab')">Distribution Trends</button>
                    <button class="tab-button" onclick="openTab(event, 'sector-tab')">Sector Trends</button>
                    <button class="tab-button" onclick="openTab(event, 'history-tab')">Historical Charts</button>
                </div>

                <div id="trend-tab" class="tab-content active">
                    <!-- 7-day trend section -->
                    <div class="trend-section">
                        <h2>Short-Term Trend (7 Days)</h2>
                        {self.generate_trend_section_html(analysis['periods'].get('7d', {'insufficient': True}))}
                    </div>

                    <!-- 15-day trend section -->
                    <div class="trend-section">
                        <h2>Medium-Term Trend (15 Days)</h2>
                        {self.generate_trend_section_html(analysis['periods'].get('15d', {'insufficient': True}))}
                    </div>

                    <!-- 30-day trend section -->
                    <div class="trend-section">
                        <h2>Long-Term Trend (30 Days)</h2>
                        {self.generate_trend_section_html(analysis['periods'].get('30d', {'insufficient': True}))}
                    </div>

                    <!-- Market outlook section -->
                    <div class="trend-section">
                        <h2>Market Outlook & Strategy</h2>
                        <p>Based on comprehensive analysis of multiple timeframes, the current market outlook is <strong class="{outlook.lower()}">{outlook_description}</strong>.</p>

                        <h3>Key Strategy Recommendations</h3>
                        <ul>
                            {self.generate_strategy_html(outlook, recent_period['trendStrength'] if recent_period and not recent_period.get('insufficient') else 0)}
                        </ul>
                    </div>
                </div>

                <div id="distribution-tab" class="tab-content">
                    <!-- Sentiment Distribution Trend Chart -->
                    <div class="chart-row">
                        <div class="chart-container">
                            <h3 class="chart-title">Sentiment Distribution Trend</h3>
                            <div id="sentiment-distribution-chart" class="plot-container"></div>
                        </div>
                    </div>

                    <!-- Sentiment Distribution Trend Analysis -->
                    <div class="trend-section">
                        <h2>Sentiment Distribution Trend Analysis (7 Days)</h2>
                        {self.generate_sentiment_distribution_trend_html(analysis.get('sentiment_distribution_trends', {}), '7d')}
                    </div>

                    <div class="trend-section">
                        <h2>Sentiment Distribution Trend Analysis (15 Days)</h2>
                        {self.generate_sentiment_distribution_trend_html(analysis.get('sentiment_distribution_trends', {}), '15d')}
                    </div>

                    <div class="trend-section">
                        <h2>Sentiment Distribution Trend Analysis (30 Days)</h2>
                        {self.generate_sentiment_distribution_trend_html(analysis.get('sentiment_distribution_trends', {}), '30d')}
                    </div>
                </div>

                <div id="sector-tab" class="tab-content">
                    <!-- Sector Sentiment Trend Chart -->
                    <div class="chart-row">
                        <div class="chart-container">
                            <h3 class="chart-title">Sector Sentiment Changes (7 Days)</h3>
                            <div id="sector-sentiment-chart" class="plot-container"></div>
                        </div>
                    </div>

                    <div class="trend-section">
                        <h2>Notes: How to interpret</h2>
                       <br>Typical Range: In practice, sentiment score changes usually fall within a range of approximately -2.0 to +2.0, 
                       based on how the sentiment scoring system is designed.
                       
                       <br>Calculated Values: The scores are derived from the weighted average of stocks in different sentiment categories:
                        <ul>
                        <li>LONG = +2</li>
                        <li>ACCUMULATION = +1</li>
                        <li>NEUTRAL = 0</li>
                        <li>DISTRIBUTION = -1</li>
                        <li>SHORT = -2</li>
                        </ul>
                        
                        <br>Dynamic Scaling: The chart will automatically adjust its scale to accommodate the largest and smallest values in the current dataset.
                        <br>Significance Threshold: In the code, a change of ±0.2 is considered significant (the threshold used to determine "IMPROVING" vs "DETERIORATING" trends).
                   
                        <br>Interpretation:
                            <ul>
                           <li> A change of +0.75 (Capital Goods) represents a substantial positive shift, indicating a significant increase in bullish sentiment.</li>
                            <li> A change of -1.14 (Consumer Services) indicates a major deterioration in sentiment.</li>
                            <li> The relative differences between sectors are often more important than the absolute values.</li>
                            
                            <li> The system will display whatever range of values is present in the current analysis, adapting the visual scale accordingly for each report generation.</li>
                            <ul>
                    </div>
                    
                    
                    
                    <!-- Sector Sentiment Trend Analysis -->
                    <div class="trend-section">
                        <h2>Sector Sentiment Trend Analysis (7 Days)</h2>
                        {self.generate_sector_sentiment_trend_html(analysis.get('sector_sentiment_trends', {}), '7d')}
                    </div>

                    <div class="trend-section">
                        <h2>Sector Sentiment Trend Analysis (15 Days)</h2>
                        {self.generate_sector_sentiment_trend_html(analysis.get('sector_sentiment_trends', {}), '15d')}
                    </div>

                    <div class="trend-section">
                        <h2>Sector Sentiment Trend Analysis (30 Days)</h2>
                        {self.generate_sector_sentiment_trend_html(analysis.get('sector_sentiment_trends', {}), '30d')}
                    </div>
                </div>

                <div id="history-tab" class="tab-content">
                    <div class="chart-row">
                        <div class="chart-container">
                            <h3 class="chart-title">Market Sentiment History</h3>
                            <div id="sentiment-chart" class="plot-container"></div>
                        </div>

                        <div class="chart-container">
                            <h3 class="chart-title">RED Alerts History</h3>
                            <div id="alerts-chart" class="plot-container"></div>
                        </div>
                    </div>

                    <div class="chart-row">
                        <div class="chart-container">
                            <h3 class="chart-title">Institutional Flow History</h3>
                            <div id="flow-chart" class="plot-container"></div>
                        </div>

                        <div class="chart-container">
                            <h3 class="chart-title">Price-Sentiment Alignment History</h3>
                            <div id="alignment-chart" class="plot-container"></div>
                        </div>
                    </div>
                </div>

                <div class="footer">
                    <p>Generated by Market Trend Analyzer based on Progressive Analysis System</p>
                </div>
            </div>

            <script>
                // Initialize Plotly charts
                document.addEventListener('DOMContentLoaded', function() {{
                    // Parse JSON figures
                    const sentimentFig = {sentiment_json};
                    const alertsFig = {alerts_json};
                    const flowFig = {flow_json};
                    const alignmentFig = {alignment_json};
                    const sentimentDistFig = {sentiment_dist_json};
                    const sectorFig = {sector_json};

                    // Create charts
                    Plotly.newPlot('sentiment-chart', sentimentFig.data, sentimentFig.layout);
                    Plotly.newPlot('alerts-chart', alertsFig.data, alertsFig.layout);
                    Plotly.newPlot('flow-chart', flowFig.data, flowFig.layout);
                    Plotly.newPlot('alignment-chart', alignmentFig.data, alignmentFig.layout);
                    Plotly.newPlot('sentiment-distribution-chart', sentimentDistFig.data, sentimentDistFig.layout);
                    Plotly.newPlot('sector-sentiment-chart', sectorFig.data, sectorFig.layout);

                    // Add resize handler
                    window.addEventListener('resize', function() {{
                        Plotly.Plots.resize('sentiment-chart');
                        Plotly.Plots.resize('alerts-chart');
                        Plotly.Plots.resize('flow-chart');
                        Plotly.Plots.resize('alignment-chart');
                        Plotly.Plots.resize('sentiment-distribution-chart');
                        Plotly.Plots.resize('sector-sentiment-chart');
                    }});
                }});

                // Tab switching function
                function openTab(evt, tabName) {{
                    // Hide all tab contents
                    var tabContents = document.getElementsByClassName("tab-content");
                    for (var i = 0; i < tabContents.length; i++) {{
                        tabContents[i].classList.remove("active");
                    }}

                    // Remove active class from all tab buttons
                    var tabButtons = document.getElementsByClassName("tab-button");
                    for (var i = 0; i < tabButtons.length; i++) {{
                        tabButtons[i].classList.remove("active");
                    }}

                    // Show the specific tab content
                    document.getElementById(tabName).classList.add("active");

                    // Add active class to the clicked button
                    evt.currentTarget.classList.add("active");

                    // Resize Plotly charts after tab switch
                    setTimeout(function() {{
                        window.dispatchEvent(new Event('resize'));
                    }}, 10);
                }}
            </script>
        </body>
        </html>
        """

        return html


    def generate_trend_section_html(self, period_data):
        """
        Generate HTML for a trend section.
        
        Args:
            period_data (dict): Period analysis data
            
        Returns:
            str: HTML content for the trend section
        """
        if period_data.get('insufficient'):
            return "<p>Insufficient data for this time period. More historical reports needed.</p>"
        
        trend_emoji = "📈" if period_data['overallTrend'] == "IMPROVING" else "📉" if period_data['overallTrend'] == "DETERIORATING" else "➡️"
        trend_class = period_data['overallTrend'].lower()
        
        html = f"""
        <p><strong>Analysis Period:</strong> {period_data['startDate']} to {period_data['endDate']}</p>
        <p><strong>Overall Trend:</strong> <span class="{trend_class}">{trend_emoji} {period_data['overallTrend']} (Strength: {period_data['trendStrength']}/5)</span></p>
        
        <h3>Metric Changes</h3>
        <ul>
        """
        
        # Sentiment changes
        sentiment = period_data['metrics']['overallSentiment']
        sentiment_class = sentiment['trend'].lower()
        sentiment_emoji = "📈" if sentiment['trend'] == "IMPROVING" else "📉" if sentiment['trend'] == "DETERIORATING" else "➡️"
        
        html += f"""
            <li><strong>Market Sentiment:</strong> <span class="{sentiment_class}">{sentiment_emoji} {sentiment['trend']}</span>
                <ul>
                    <li>Score change: {sentiment['scoreChange']:+.2f}</li>
                    <li>Bullish percentage: {sentiment['bullishChange']:+.1f}%</li>
                    <li>Bearish percentage: {sentiment['bearishChange']:+.1f}%</li>
                </ul>
            </li>
        """
        
        # Alert changes
        alerts = period_data['metrics']['alertDistribution']
        alerts_class = alerts['trend'].lower()
        alerts_emoji = "📈" if alerts['trend'] == "IMPROVING" else "📉" if alerts['trend'] == "DETERIORATING" else "➡️"
        
        html += f"""
            <li><strong>Alert Distribution:</strong> <span class="{alerts_class}">{alerts_emoji} {alerts['trend']}</span>
                <ul>
                    <li>RED alerts change: {alerts['redAlertsChange']:+d} stocks</li>
                </ul>
            </li>
        """
        
        # Flow changes
        flow = period_data['metrics']['institutionalFlow']
        flow_class = flow['trend'].lower()
        flow_emoji = "📈" if flow['trend'] == "IMPROVING" else "📉" if flow['trend'] == "DETERIORATING" else "➡️"
        
        html += f"""
            <li><strong>Institutional Flow:</strong> <span class="{flow_class}">{flow_emoji} {flow['trend']}</span>
                <ul>
                    <li>FII positive flow change: {flow['fiiPositiveChange']:+.1f}%</li>
                </ul>
            </li>
        """
        
        # Alignment changes
        alignment = period_data['metrics']['priceSentimentAlignment']
        alignment_class = alignment['trend'].lower()
        alignment_emoji = "📈" if alignment['trend'] == "IMPROVING" else "📉" if alignment['trend'] == "DETERIORATING" else "➡️"
        
        html += f"""
            <li><strong>Price-Sentiment Alignment:</strong> <span class="{alignment_class}">{alignment_emoji} {alignment['trend']}</span>
                <ul>
                    <li>Alignment change: {alignment['change']:+.1f}%</li>
                </ul>
            </li>
        </ul>
        """
        
        # Add insights
        insights = self.generate_insights(period_data)
        
        html += """
        <h3>Key Insights</h3>
        <ul>
        """
        
        for insight in insights:
            html += f"<li>{insight}</li>"
        
        html += """
        </ul>
        """
        
        return html

    def generate_strategy_html(self, outlook, strength):
        """
        Generate HTML for strategy recommendations based on outlook.
        
        Args:
            outlook (str): Market outlook
            strength (int): Trend strength
            
        Returns:
            str: HTML content for strategy recommendations
        """
        if outlook == "Bullish":
            if strength >= 4:
                return """
                <li>Consider increasing equity exposure, focusing on sectors showing the strongest bullish sentiment</li>
                <li>Look for stocks with positive institutional flows and high price-sentiment alignment</li>
                <li>Monitor potential overbought conditions in the strongest performers</li>
                <li>Continue to track RED alerts which might indicate isolated sector risks</li>
                """
            else:
                return """
                <li>Selectively increase equity exposure in sectors with positive momentum</li>
                <li>Focus on stocks showing improving institutional flows</li>
                <li>Monitor the price-sentiment alignment for confirmation of the bullish trend</li>
                <li>Be prepared to adjust positioning if RED alerts start to increase</li>
                """
        elif outlook == "Bearish":
            if strength >= 4:
                return """
                <li>Consider reducing equity exposure and increasing defensive positions</li>
                <li>Pay close attention to stocks with RED alerts and deteriorating institutional flows</li>
                <li>Look for potential hedging opportunities</li>
                <li>Monitor for potential countertrend rallies that may provide better exit points</li>
                """
            else:
                return """
                <li>Selectively reduce exposure in the weakest sectors</li>
                <li>Focus on quality stocks with stable institutional flows</li>
                <li>Pay special attention to stocks showing RED alerts</li>
                <li>Prepare a watchlist of quality stocks for potential entry on further market weakness</li>
                """
        else:  # Neutral
            return """
            <li>Maintain balanced portfolio positioning with focus on stock-specific opportunities</li>
            <li>Watch for sector rotation and divergences in institutional flows</li>
            <li>Monitor for breakout signals in either direction</li>
            <li>Consider pair trades to capitalize on relative performance disparities</li>
            """

    def create_sentiment_history_chart(self, metrics_history):
        """
        Create a chart showing sentiment history.
        
        Args:
            metrics_history (dict): Historical metrics data
            
        Returns:
            Figure: Plotly figure
        """
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add sentiment score line
        fig.add_trace(
            go.Scatter(
                x=metrics_history['dates'],
                y=metrics_history['sentiment_scores'],
                mode='lines+markers',
                name='Sentiment Score',
                line=dict(color='#3498db', width=3)
            ),
            secondary_y=False
        )
        
        # Add bullish percentage line
        fig.add_trace(
            go.Scatter(
                x=metrics_history['dates'],
                y=metrics_history['bullish_percentages'],
                mode='lines',
                name='Bullish %',
                line=dict(color='#1E8449', width=2, dash='dash')
            ),
            secondary_y=True
        )
        
        # Add bearish percentage line
        fig.add_trace(
            go.Scatter(
                x=metrics_history['dates'],
                y=metrics_history['bearish_percentages'],
                mode='lines',
                name='Bearish %',
                line=dict(color='#C0392B', width=2, dash='dash')
            ),
            secondary_y=True
        )
        
        # Update layout
        fig.update_layout(
            title='Market Sentiment History',
            template='plotly_white',
            margin=dict(l=50, r=50, t=80, b=50),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
            height=400
        )
        
        # Update axes
        fig.update_xaxes(title_text='Date')
        fig.update_yaxes(title_text='Sentiment Score', secondary_y=False)
        fig.update_yaxes(title_text='Percentage (%)', secondary_y=True)
        
        return fig

    def create_alerts_history_chart(self, metrics_history):
        """
        Create a chart showing RED alerts history.
        
        Args:
            metrics_history (dict): Historical metrics data
            
        Returns:
            Figure: Plotly figure
        """
        fig = go.Figure()
        
        # Add RED alerts bar chart
        fig.add_trace(
            go.Bar(
                x=metrics_history['dates'],
                y=metrics_history['red_alerts'],
                name='RED Alerts',
                marker_color='#C0392B'
            )
        )
        
        # Update layout
        fig.update_layout(
            title='RED Alerts History',
            template='plotly_white',
            margin=dict(l=50, r=50, t=80, b=50),
            height=400
        )
        
        # Update axes
        fig.update_xaxes(title_text='Date')
        fig.update_yaxes(title_text='Number of Stocks with RED Alerts')
        
        return fig

    def create_flow_history_chart(self, metrics_history):
        """
        Create a chart showing institutional flow history.
        
        Args:
            metrics_history (dict): Historical metrics data
            
        Returns:
            Figure: Plotly figure
        """
        fig = go.Figure()
        
        # Add FII positive flow line
        fig.add_trace(
            go.Scatter(
                x=metrics_history['dates'],
                y=metrics_history['fii_positive'],
                mode='lines+markers',
                name='FII Positive Flow %',
                line=dict(color='#3498db', width=3)
            )
        )
        
        # Add 50% reference line
        fig.add_shape(
            type="line",
            x0=metrics_history['dates'][0],
            y0=50,
            x1=metrics_history['dates'][-1],
            y1=50,
            line=dict(color="gray", width=1, dash="dash")
        )
        
        # Update layout
        fig.update_layout(
            title='Institutional Flow History',
            template='plotly_white',
            margin=dict(l=50, r=50, t=80, b=50),
            height=400
        )
        
        # Update axes
        fig.update_xaxes(title_text='Date')
        fig.update_yaxes(title_text='FII Positive Flow Percentage (%)')
        
        return fig

    def create_alignment_history_chart(self, metrics_history):
        """
        Create a chart showing price-sentiment alignment history.
        
        Args:
            metrics_history (dict): Historical metrics data
            
        Returns:
            Figure: Plotly figure
        """
        fig = go.Figure()
        
        # Add alignment percentage line
        fig.add_trace(
            go.Scatter(
                x=metrics_history['dates'],
                y=metrics_history['price_sentiment_alignment'],
                mode='lines+markers',
                name='Alignment %',
                line=dict(color='#9B59B6', width=3)
            )
        )
        
        # Update layout
        fig.update_layout(
            title='Price-Sentiment Alignment History',
            template='plotly_white',
            margin=dict(l=50, r=50, t=80, b=50),
            height=400
        )
        
        # Update axes
        fig.update_xaxes(title_text='Date')
        fig.update_yaxes(title_text='Alignment Percentage (%)')
        
        return fig

    def process_reports(self):
        """
        Process all dashboard reports and generate trend analysis.
        Updated to include sentiment distribution and sector sentiment trends.

        Returns:
            str: Path to generated trend report
        """
        # Get list of dashboard files sorted by date
        file_date_pairs = self.scan_dashboard_files()
        if not file_date_pairs:
            print("No dashboard files found for analysis")
            return None
        
        # Process each file and extract metrics
        for file_path, file_date in file_date_pairs:
            print(f"Processing {os.path.basename(file_path)}...")
            metrics = self.extract_metrics_from_html(file_path)
            self.reports_data.append(metrics)
        
        # Analyze trends
        analysis = self.analyze_market_trends()
        if not analysis:
            print("Failed to analyze trends")
            return None
        
        # Generate trend report
        html_content = self.create_trend_report(analysis)
        
        # Construct output filename
        current_date = datetime.now().strftime("%Y%m%d")
        output_filename = f"market_trend_analysis_{current_date}.html"
        output_path = os.path.join(self.output_dir, output_filename)
        
        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Market trend analysis report generated at: {output_path}")
        return output_path

def main():
    """
    Main function to run the market trend analyzer with enhanced trend analysis
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate market trend analysis from dashboard reports')
    parser.add_argument('--reports-dir', type=str, required=True,
                    help='Directory containing market_dashboard_*.html files')
    parser.add_argument('--output-dir', type=str, default=None,
                    help='Directory for output trend report (default: same as reports_dir)')
    parser.add_argument('--days', type=int, default=15,
                    help='Number of days to include in trend analysis (default: 15)')

    args = parser.parse_args()

    # Create trend analyzer
    analyzer = MarketTrendAnalyzer(
        reports_dir=args.reports_dir,
        output_dir=args.output_dir,
        days_to_analyze=args.days
    )

    # Process reports and generate trend analysis with the enhanced features
    report_path = analyzer.process_reports()

    if report_path:
        print(f"Market trend analysis completed successfully.")
        print(f"Report saved at: {report_path}")
        print(f"The report now includes sentiment distribution and sector sentiment trend analysis.")
    else:
        print("Failed to generate market trend analysis report.")

if __name__ == "__main__":
    main()