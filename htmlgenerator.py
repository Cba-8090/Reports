#!/usr/bin/env python3
"""
HTML Publication Generator Module

This module provides the HTMLPublicationGenerator class for creating
beautiful HTML publications from market intelligence data.

Author: Market Intelligence Team
Date: 2025-06-06
"""

from datetime import datetime
from typing import Dict, List, Any


class HTMLPublicationGenerator:
    """Generate beautiful HTML publications from market intelligence data"""

    def __init__(self, global_data, publication_sections: Dict[str, str]):
        self.data = global_data
        self.sections = publication_sections
        self.report_date = datetime.now()

    def generate_html_publication(self, output_path: str) -> str:
        """Generate complete HTML publication"""

        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Global Market Intelligence Weekly | {self.report_date.strftime('%B %d, %Y')}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>{self._get_css_styles()}</style>
</head>
<body>
    <div class="publication-container">
        {self._generate_header()}
        {self._generate_executive_summary()}
        {self._generate_key_metrics_dashboard()}
        {self._generate_section_content()}
        {self._generate_footer()}
    </div>
    <script>{self._get_javascript()}</script>
</body>
</html>"""

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"✅ HTML publication successfully generated: {output_path}")
        except Exception as e:
            print(f"❌ Error generating HTML publication: {e}")
            raise

        return output_path

    def _get_css_styles(self) -> str:
        """Professional CSS styles for the publication"""
        return """
        :root {
            --primary-color: #1a365d; --secondary-color: #2d3748; --accent-color: #3182ce;
            --success-color: #38a169; --warning-color: #d69e2e; --danger-color: #e53e3e;
            --neutral-color: #718096; --background-color: #f7fafc; --card-background: #ffffff;
            --text-primary: #2d3748; --text-secondary: #4a5568; --border-color: #e2e8f0;
            --shadow-sm: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06);
            --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6; color: var(--text-primary); background-color: var(--background-color); font-size: 16px;
        }
        .publication-container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .publication-header {
            background: linear-gradient(135deg, var(--primary-color) 0%, var(--accent-color) 100%);
            color: white; padding: 60px 40px; border-radius: 16px; margin-bottom: 40px; box-shadow: var(--shadow-lg);
            position: relative; overflow: hidden;
        }
        .publication-header::before {
            content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0;
            background: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><defs><pattern id="grid" width="10" height="10" patternUnits="userSpaceOnUse"><path d="M 10 0 L 0 0 0 10" fill="none" stroke="rgba(255,255,255,0.1)" stroke-width="0.5"/></pattern></defs><rect width="100" height="100" fill="url(%23grid)"/></svg>');
            opacity: 0.3;
        }
        .header-content { position: relative; z-index: 1; }
        .publication-title { font-size: 3rem; font-weight: 700; margin-bottom: 12px; line-height: 1.2; }
        .publication-subtitle { font-size: 1.25rem; font-weight: 400; opacity: 0.9; margin-bottom: 24px; }
        .publication-meta { display: flex; align-items: center; gap: 24px; font-size: 0.95rem; opacity: 0.8; flex-wrap: wrap; }
        .meta-item { display: flex; align-items: center; gap: 8px; }
        .executive-summary {
            background: var(--card-background); border-radius: 16px; padding: 40px; margin-bottom: 40px;
            box-shadow: var(--shadow-md); border-left: 6px solid var(--accent-color);
        }
        .section-title {
            font-size: 2rem; font-weight: 600; color: var(--primary-color); margin-bottom: 24px;
            display: flex; align-items: center; gap: 12px;
        }
        .section-icon {
            width: 32px; height: 32px; background: var(--accent-color); border-radius: 8px;
            display: flex; align-items: center; justify-content: center; color: white; font-size: 18px;
        }
        .metrics-dashboard {
            display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 24px; margin-bottom: 40px;
        }
        .metric-card {
            background: var(--card-background); border-radius: 12px; padding: 24px; box-shadow: var(--shadow-sm);
            border: 1px solid var(--border-color); transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        .metric-card:hover { transform: translateY(-2px); box-shadow: var(--shadow-md); }
        .metric-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px; }
        .metric-title {
            font-size: 0.9rem; font-weight: 500; color: var(--text-secondary);
            text-transform: uppercase; letter-spacing: 0.5px;
        }
        .metric-status { padding: 4px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }
        .status-bullish { background: rgba(56, 161, 105, 0.1); color: var(--success-color); }
        .status-bearish { background: rgba(229, 62, 62, 0.1); color: var(--danger-color); }
        .status-neutral { background: rgba(113, 128, 150, 0.1); color: var(--neutral-color); }
        .metric-value { font-size: 2.5rem; font-weight: 700; line-height: 1; margin-bottom: 8px; }
        .metric-change { font-size: 0.9rem; display: flex; align-items: center; gap: 4px; }
        .change-positive { color: var(--success-color); }
        .change-negative { color: var(--danger-color); }
        .content-section {
            background: var(--card-background); border-radius: 16px; padding: 40px; margin-bottom: 32px;
            box-shadow: var(--shadow-sm); border: 1px solid var(--border-color);
        }
        .section-content { font-size: 1.1rem; line-height: 1.8; color: var(--text-primary); }
        .section-content h3 {
            color: var(--primary-color); font-size: 1.3rem; font-weight: 600; margin: 32px 0 16px 0;
            padding-left: 16px; border-left: 4px solid var(--accent-color);
        }
        .section-content h4 { color: var(--secondary-color); font-size: 1.1rem; font-weight: 600; margin: 24px 0 12px 0; }
        .section-content ul { margin: 16px 0; padding-left: 24px; }
        .section-content li { margin-bottom: 8px; position: relative; }
        .section-content li::marker { color: var(--accent-color); }
        .section-content strong { color: var(--primary-color); font-weight: 600; }
        .section-content p { margin-bottom: 16px; }
        .progress-bar {
            width: 100%; height: 8px; background: rgba(0, 0, 0, 0.1); border-radius: 4px;
            overflow: hidden; margin: 12px 0;
        }
        .progress-fill { height: 100%; border-radius: 4px; transition: width 0.3s ease; }
        .progress-bullish { background: linear-gradient(90deg, var(--success-color), #48bb78); }
        .progress-bearish { background: linear-gradient(90deg, var(--danger-color), #f56565); }
        .publication-footer {
            background: var(--secondary-color); color: white; padding: 40px; border-radius: 16px;
            margin-top: 40px; text-align: center;
        }
        .footer-content { max-width: 800px; margin: 0 auto; }
        .footer-title { font-size: 1.5rem; font-weight: 600; margin-bottom: 16px; }
        .footer-text { opacity: 0.8; margin-bottom: 8px; }
        @media (max-width: 768px) {
            .publication-container { padding: 16px; }
            .publication-header { padding: 40px 24px; }
            .publication-title { font-size: 2.5rem; }
            .executive-summary, .content-section { padding: 24px; }
            .metrics-dashboard { grid-template-columns: 1fr; }
            .section-title { font-size: 1.75rem; }
            .publication-meta { justify-content: center; }
        }
        """

    def _generate_header(self) -> str:
        """Generate publication header"""
        return f"""
        <header class="publication-header">
            <div class="header-content">
                <h1 class="publication-title">Global Market Intelligence</h1>
                <p class="publication-subtitle">Weekly Strategic Analysis & Investment Outlook</p>
                <div class="publication-meta">
                    <div class="meta-item">
                        <span>📅</span>
                        <span>Report Date: {self.report_date.strftime('%B %d, %Y')}</span>
                    </div>
                    <div class="meta-item">
                        <span>⏰</span>
                        <span>Generated: {self.report_date.strftime('%I:%M %p')}</span>
                    </div>
                    <div class="meta-item">
                        <span>📊</span>
                        <span>Data as of: {self.data.report_date}</span>
                    </div>
                </div>
            </div>
        </header>
        """

    def _generate_executive_summary(self) -> str:
        """Generate executive summary section"""
        return f"""
        <section class="executive-summary">
            <h2 class="section-title">
                <span class="section-icon">📋</span>
                Executive Summary
            </h2>
            <div class="section-content">
                {self._format_content_for_html(self.sections.get('executive_summary', ''))}
            </div>
        </section>
        """

    def _generate_key_metrics_dashboard(self) -> str:
        """Generate key metrics dashboard"""
        try:
            sentiment_status = self._get_sentiment_status()
            risk_status = self._get_risk_status()
            credit_status = self._get_credit_status()

            return f"""
            <section class="metrics-dashboard">
                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-title">Market Sentiment</span>
                        <span class="metric-status {sentiment_status['class']}">{sentiment_status['label']}</span>
                    </div>
                    <div class="metric-value" style="color: {sentiment_status['color']}">{self.data.sentiment_score:.1f}</div>
                    <div class="metric-change">Confidence: {self.data.sentiment_confidence}</div>
                    <div class="progress-bar">
                        <div class="progress-fill {sentiment_status['progress_class']}" 
                             style="width: {min(abs(self.data.sentiment_score) * 5, 100)}%"></div>
                    </div>
                </div>

                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-title">Economic Status</span>
                        <span class="metric-status {risk_status['class']}">{self.data.economic_status}</span>
                    </div>
                    <div class="metric-value" style="color: {risk_status['color']}">{self.data.risk_index:.1f}</div>
                    <div class="metric-change">Risk Index • {self.data.economic_confidence} Confidence</div>
                    <div class="progress-bar">
                        <div class="progress-fill {risk_status['progress_class']}" 
                             style="width: {self.data.risk_index}%"></div>
                    </div>
                </div>

                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-title">Credit Markets</span>
                        <span class="metric-status {credit_status['class']}">{self.data.hyg_alert_level}</span>
                    </div>
                    <div class="metric-value" style="color: {credit_status['color']}">{self.data.hyg_spread:.2f}%</div>
                    <div class="metric-change">HYG Spread • {self.data.hyg_confidence}% Confidence</div>
                    <div class="progress-bar">
                        <div class="progress-fill {credit_status['progress_class']}" 
                             style="width: {min(self.data.hyg_spread * 20, 100)}%"></div>
                    </div>
                </div>

                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-title">Signal Balance</span>
                        <span class="metric-status status-neutral">Mixed</span>
                    </div>
                    <div class="metric-value" style="color: var(--accent-color)">
                        {len(self.data.bullish_signals)}:{len(self.data.bearish_signals)}
                    </div>
                    <div class="metric-change">Bullish vs Bearish Signals</div>
                    <div class="progress-bar">
                        <div class="progress-fill progress-bullish" 
                             style="width: {self._calculate_signal_balance()}%"></div>
                    </div>
                </div>
            </section>
            """
        except Exception as e:
            print(f"Error generating metrics dashboard: {e}")
            return "<section class='metrics-dashboard'><p>Error loading metrics dashboard</p></section>"

    def _generate_section_content(self) -> str:
        """Generate main content sections"""
        sections_html = ""

        section_configs = [
            {'key': 'market_sentiment', 'title': 'Market Sentiment Deep Dive', 'icon': '📈'},
            {'key': 'economic_backdrop', 'title': 'Economic Backdrop', 'icon': '🏛️'},
            {'key': 'credit_intelligence', 'title': 'Credit Market Intelligence', 'icon': '💳'}
        ]

        for config in section_configs:
            if config['key'] in self.sections:
                sections_html += f"""
                <section class="content-section">
                    <h2 class="section-title">
                        <span class="section-icon">{config['icon']}</span>
                        {config['title']}
                    </h2>
                    <div class="section-content">
                        {self._format_content_for_html(self.sections[config['key']])}
                    </div>
                </section>
                """

        return sections_html

    def _generate_footer(self) -> str:
        """Generate publication footer"""
        data_quality = getattr(self.data, 'data_quality', {'completeness': 'N/A', 'status': 'N/A'})

        return f"""
        <footer class="publication-footer">
            <div class="footer-content">
                <h3 class="footer-title">Market Intelligence System</h3>
                <p class="footer-text">
                    <strong>Generated:</strong> {self.report_date.strftime('%B %d, %Y at %I:%M %p')}
                </p>
                <p class="footer-text">
                    <strong>Data Quality:</strong> {data_quality.get('completeness', 'N/A')} • 
                    Status: {data_quality.get('status', 'N/A')}
                </p>
                <p class="footer-text">
                    <strong>Disclaimer:</strong> This report provides market analysis for informational purposes only. 
                    Investment decisions should be made based on individual circumstances and risk tolerance.
                </p>
                <p class="footer-text" style="margin-top: 16px; font-size: 0.9rem; opacity: 0.7;">
                    <strong>Data Sources:</strong> Federal Reserve Economic Data (FRED) | ICE BofA Indices | 
                    Global Economic Indicators | HYG Credit Analysis
                </p>
            </div>
        </footer>
        """

    def _format_content_for_html(self, content: str) -> str:
        """Format markdown-style content for HTML"""
        if not content:
            return "<p>Content not available</p>"

        # Convert basic markdown to HTML
        content = content.replace('**', '<strong>').replace('**', '</strong>')

        lines = content.split('\n')
        formatted_lines = []
        in_list = False

        for line in lines:
            line = line.strip()
            if line.startswith('• '):
                if not in_list:
                    formatted_lines.append('<ul>')
                    in_list = True
                formatted_lines.append(f'<li>{line[2:]}</li>')
            elif line.startswith('**') and line.endswith('**'):
                if in_list:
                    formatted_lines.append('</ul>')
                    in_list = False
                formatted_lines.append(f'<h4>{line[2:-2]}</h4>')
            elif line:
                if in_list:
                    formatted_lines.append('</ul>')
                    in_list = False
                formatted_lines.append(f'<p>{line}</p>')

        if in_list:
            formatted_lines.append('</ul>')

        return '\n'.join(formatted_lines)

    def _get_sentiment_status(self):
        """Get sentiment status styling"""
        try:
            if self.data.sentiment_score > 5:
                return {'class': 'status-bullish', 'label': 'Bullish', 'color': 'var(--success-color)',
                        'progress_class': 'progress-bullish'}
            elif self.data.sentiment_score < -5:
                return {'class': 'status-bearish', 'label': 'Bearish', 'color': 'var(--danger-color)',
                        'progress_class': 'progress-bearish'}
            else:
                return {'class': 'status-neutral', 'label': 'Neutral', 'color': 'var(--neutral-color)',
                        'progress_class': 'progress-bullish'}
        except:
            return {'class': 'status-neutral', 'label': 'Unknown', 'color': 'var(--neutral-color)',
                    'progress_class': 'progress-bullish'}

    def _get_risk_status(self):
        """Get risk status styling"""
        try:
            if self.data.risk_index > 70:
                return {'class': 'status-bearish', 'color': 'var(--danger-color)', 'progress_class': 'progress-bearish'}
            elif self.data.risk_index > 40:
                return {'class': 'status-neutral', 'color': 'var(--warning-color)',
                        'progress_class': 'progress-bearish'}
            else:
                return {'class': 'status-bullish', 'color': 'var(--success-color)',
                        'progress_class': 'progress-bullish'}
        except:
            return {'class': 'status-neutral', 'color': 'var(--neutral-color)', 'progress_class': 'progress-bullish'}

    def _get_credit_status(self):
        """Get credit status styling"""
        try:
            if 'EXTREME' in str(self.data.hyg_alert_level).upper():
                return {'class': 'status-bearish', 'color': 'var(--danger-color)', 'progress_class': 'progress-bearish'}
            elif 'MODERATE' in str(self.data.hyg_alert_level).upper():
                return {'class': 'status-neutral', 'color': 'var(--warning-color)',
                        'progress_class': 'progress-bearish'}
            else:
                return {'class': 'status-bullish', 'color': 'var(--success-color)',
                        'progress_class': 'progress-bullish'}
        except:
            return {'class': 'status-neutral', 'color': 'var(--neutral-color)', 'progress_class': 'progress-bullish'}

    def _calculate_signal_balance(self):
        """Calculate signal balance percentage"""
        try:
            total_signals = len(self.data.bullish_signals) + len(self.data.bearish_signals)
            if total_signals == 0:
                return 50
            return (len(self.data.bullish_signals) / total_signals) * 100
        except:
            return 50

    def _get_javascript(self):
        """JavaScript for interactive features"""
        return """
        document.addEventListener('DOMContentLoaded', function() {
            console.log('Global Market Intelligence Report loaded successfully');

            // Add hover effects to metric cards
            document.querySelectorAll('.metric-card').forEach(card => {
                card.addEventListener('mouseenter', function() {
                    this.style.transform = 'translateY(-4px)';
                    this.style.boxShadow = 'var(--shadow-lg)';
                });

                card.addEventListener('mouseleave', function() {
                    this.style.transform = 'translateY(-2px)';
                    this.style.boxShadow = 'var(--shadow-md)';
                });
            });

            // Print functionality
            window.printReport = function() {
                window.print();
            };
        });
        """