#!/usr/bin/env python3
"""
Gamma-Enhanced Options Trading System - SELL Strategy Version
Part 1: Enhanced Configuration & Market Regime Detection

Focuses on:
- Range-bound market detection
- Volatility analysis
- OTM SELL strategies for premium collection
- Pullback identification in trending markets
"""

import os
import sys
import sqlite3
import logging
import json
import re
from datetime import datetime, date, timedelta
from pathlib import Path
import time
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple, Any
import threading
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
import numpy as np

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.append(str(project_root))

@dataclass
class TradePosition:
    """Enhanced data class for SELL trade positions"""
    trade_id: str
    signal_type: str  # SELL_CE, SELL_PE, BUY_CE, BUY_PE
    option_type: str  # CE or PE
    strike_price: float
    lots: int
    quantity: int
    entry_price: float  # Premium received for SELL trades
    target_price: float  # Target for closing (lower for SELL)
    stop_loss_price: float  # Stop loss (higher for SELL)
    entry_time: datetime
    expiry_date: date
    investment_amount: float  # Margin blocked for SELL trades
    premium_received: float  # Premium collected for SELL trades
    signal_strength: str
    cumulative_trend: float
    confidence: float
    market_regime: str  # 'TRENDING', 'RANGE_BOUND', 'HIGH_VOLATILITY', 'LOW_VOLATILITY'
    volatility_percentile: float  # Current volatility vs historical
    range_bounds: Dict[str, float]  # {'upper': 19600, 'lower': 19400}
    current_price: float = 0.0
    current_pnl: float = 0.0
    roi_percent: float = 0.0
    max_profit_achieved: float = 0.0
    max_drawdown_faced: float = 0.0
    status: str = 'ACTIVE'

class MarketRegimeDetector:
    """Detects market regime for appropriate strategy selection"""
    
    def __init__(self, data_provider, logger):
        self.data_provider = data_provider
        self.logger = logger
        self.volatility_window = 20  # Days for volatility calculation
        self.range_detection_hours = 2  # Hours after opening to detect range
        
    def detect_market_regime(self, current_time: datetime = None) -> Dict:
        """Detect current market regime"""
        if current_time is None:
            current_time = datetime.now()
            
        try:
            # Get current market data
            spot_price = self.data_provider.get_current_spot_price()
            
            # Detect market opening behavior
            opening_analysis = self._analyze_opening_behavior(current_time)
            
            # Calculate volatility metrics
            volatility_analysis = self._calculate_volatility_metrics()
            
            # Detect range-bound conditions
            range_analysis = self._detect_range_conditions(spot_price, current_time)
            
            # Determine overall market regime
            regime = self._determine_regime(opening_analysis, volatility_analysis, range_analysis)
            
            self.logger.info(f"📊 Market Regime Detection:")
            self.logger.info(f"   Primary Regime: {regime['primary_regime']}")
            self.logger.info(f"   Volatility: {regime['volatility_regime']} ({regime['volatility_percentile']:.1f}%ile)")
            self.logger.info(f"   Range Status: {regime['range_status']}")
            self.logger.info(f"   Recommended Strategy: {regime['recommended_strategy']}")
            
            return regime
            
        except Exception as e:
            self.logger.error(f"❌ Market regime detection failed: {e}")
            return self._get_default_regime()
    
    def _analyze_opening_behavior(self, current_time: datetime) -> Dict:
        """Analyze market opening behavior to detect gaps and subsequent consolidation"""
        try:
            market_open_time = current_time.replace(hour=9, minute=15, second=0, microsecond=0)
            
            # Check if we're within first 2 hours of trading
            hours_since_open = (current_time - market_open_time).total_seconds() / 3600
            
            if hours_since_open < 0 or hours_since_open > 6.5:  # Outside market hours
                return {'phase': 'PRE_MARKET', 'gap_detected': False, 'consolidation_likely': False}
            
            # Get opening range data (would use real data in production)
            opening_range = self._get_opening_range_data()
            
            gap_size = abs(opening_range['gap_percent'])
            range_compression = opening_range['range_compression']
            
            phase = 'OPENING' if hours_since_open < 0.5 else 'EARLY' if hours_since_open < 2 else 'MID_DAY'
            
            # Detect significant gaps
            gap_detected = gap_size > 0.5  # 0.5% gap threshold
            
            # Detect consolidation after gap
            consolidation_likely = gap_detected and range_compression > 0.7  # 70% range compression
            
            return {
                'phase': phase,
                'hours_since_open': hours_since_open,
                'gap_detected': gap_detected,
                'gap_size_percent': gap_size,
                'consolidation_likely': consolidation_likely,
                'range_compression': range_compression
            }
            
        except Exception as e:
            self.logger.error(f"❌ Opening analysis failed: {e}")
            return {'phase': 'UNKNOWN', 'gap_detected': False, 'consolidation_likely': False}
    
    def _get_opening_range_data(self) -> Dict:
        """Get opening range and gap analysis (mock data - would use real data)"""
        # This would fetch real opening data from your database
        # Mock implementation for demonstration
        import random
        
        # Simulate gap and range data
        gap_percent = random.uniform(-2.0, 2.0)  # -2% to +2% gap
        range_compression = random.uniform(0.3, 0.9)  # Range compression ratio
        
        return {
            'gap_percent': gap_percent,
            'range_compression': range_compression,
            'opening_high': 19550,
            'opening_low': 19480,
            'current_range': 70
        }
    
    def _calculate_volatility_metrics(self) -> Dict:
        """Calculate current volatility vs historical"""
        try:
            # Get historical volatility data (would use real data)
            historical_volatility = self._get_historical_volatility()
            current_volatility = self._get_current_volatility()
            
            # Calculate percentile
            volatility_percentile = self._calculate_percentile(current_volatility, historical_volatility)
            
            # Classify volatility regime
            if volatility_percentile < 25:
                volatility_regime = 'LOW_VOLATILITY'
            elif volatility_percentile > 75:
                volatility_regime = 'HIGH_VOLATILITY'
            else:
                volatility_regime = 'NORMAL_VOLATILITY'
            
            return {
                'current_volatility': current_volatility,
                'volatility_percentile': volatility_percentile,
                'volatility_regime': volatility_regime,
                'iv_rank': volatility_percentile  # Implied Volatility Rank
            }
            
        except Exception as e:
            self.logger.error(f"❌ Volatility calculation failed: {e}")
            return {
                'current_volatility': 20.0,
                'volatility_percentile': 50.0,
                'volatility_regime': 'NORMAL_VOLATILITY',
                'iv_rank': 50.0
            }
    
    def _get_historical_volatility(self) -> List[float]:
        """Get historical volatility data"""
        # Mock data - would fetch from database
        return [15.2, 18.5, 22.3, 16.8, 25.1, 19.7, 21.4, 17.9, 23.6, 20.2]
    
    def _get_current_volatility(self) -> float:
        """Calculate current volatility"""
        # Mock calculation - would use real option chain data
        import random
        return random.uniform(15.0, 25.0)
    
    def _calculate_percentile(self, current_value: float, historical_values: List[float]) -> float:
        """Calculate percentile rank of current value"""
        if not historical_values:
            return 50.0
        
        sorted_values = sorted(historical_values)
        position = sum(1 for x in sorted_values if x <= current_value)
        return (position / len(sorted_values)) * 100
    
    def _detect_range_conditions(self, spot_price: float, current_time: datetime) -> Dict:
        """Detect if market is in range-bound condition"""
        try:
            # Get recent price action (would use real data)
            recent_high, recent_low = self._get_recent_range()
            
            range_size = recent_high - recent_low
            range_midpoint = (recent_high + recent_low) / 2
            
            # Calculate position within range
            position_in_range = (spot_price - recent_low) / range_size if range_size > 0 else 0.5
            
            # Determine if range-bound
            range_size_percent = (range_size / spot_price) * 100
            is_range_bound = range_size_percent < 2.0  # Less than 2% range indicates consolidation
            
            # Detect support/resistance levels
            support_level = recent_low
            resistance_level = recent_high
            
            return {
                'is_range_bound': is_range_bound,
                'range_size_percent': range_size_percent,
                'support_level': support_level,
                'resistance_level': resistance_level,
                'range_midpoint': range_midpoint,
                'position_in_range': position_in_range,
                'range_bounds': {'upper': resistance_level, 'lower': support_level}
            }
            
        except Exception as e:
            self.logger.error(f"❌ Range detection failed: {e}")
            return {
                'is_range_bound': False,
                'range_size_percent': 3.0,
                'support_level': spot_price - 100,
                'resistance_level': spot_price + 100,
                'range_midpoint': spot_price,
                'position_in_range': 0.5,
                'range_bounds': {'upper': spot_price + 100, 'lower': spot_price - 100}
            }
    
    def _get_recent_range(self) -> Tuple[float, float]:
        """Get recent high/low range (mock data)"""
        # Would fetch real data from database
        import random
        base_price = 19500
        range_size = random.uniform(50, 150)
        recent_high = base_price + range_size/2
        recent_low = base_price - range_size/2
        return recent_high, recent_low
    
    def _determine_regime(self, opening_analysis: Dict, volatility_analysis: Dict, range_analysis: Dict) -> Dict:
        """Determine overall market regime and recommended strategy"""
        
        # Primary regime determination
        if range_analysis['is_range_bound'] and volatility_analysis['volatility_regime'] == 'LOW_VOLATILITY':
            primary_regime = 'RANGE_BOUND_LOW_VOL'
            recommended_strategy = 'SELL_PREMIUM'
        elif opening_analysis['consolidation_likely']:
            primary_regime = 'POST_GAP_CONSOLIDATION'
            recommended_strategy = 'SELL_PREMIUM'
        elif volatility_analysis['volatility_regime'] == 'HIGH_VOLATILITY':
            primary_regime = 'HIGH_VOLATILITY_TRENDING'
            recommended_strategy = 'BUY_OPTIONS'
        else:
            primary_regime = 'NORMAL_TRENDING'
            recommended_strategy = 'BUY_OPTIONS'
        
        return {
            'primary_regime': primary_regime,
            'volatility_regime': volatility_analysis['volatility_regime'],
            'volatility_percentile': volatility_analysis['volatility_percentile'],
            'range_status': 'RANGE_BOUND' if range_analysis['is_range_bound'] else 'TRENDING',
            'recommended_strategy': recommended_strategy,
            'opening_analysis': opening_analysis,
            'range_analysis': range_analysis,
            'volatility_analysis': volatility_analysis
        }
    
    def _get_default_regime(self) -> Dict:
        """Return default regime when detection fails"""
        return {
            'primary_regime': 'NORMAL_TRENDING',
            'volatility_regime': 'NORMAL_VOLATILITY',
            'volatility_percentile': 50.0,
            'range_status': 'TRENDING',
            'recommended_strategy': 'BUY_OPTIONS',
            'opening_analysis': {'phase': 'UNKNOWN'},
            'range_analysis': {'is_range_bound': False},
            'volatility_analysis': {'volatility_regime': 'NORMAL_VOLATILITY'}
        }

class EnhancedTradingSystemConfig:
    """Enhanced configuration for SELL strategy system"""
    
    def __init__(self):
        # Inherit from original config
        from main import TradingSystemConfig
        base_config = TradingSystemConfig()
        
        # Copy all base configuration
        for attr in dir(base_config):
            if not attr.startswith('_'):
                setattr(self, attr, getattr(base_config, attr))
        
        # Enhanced SELL strategy configuration
        self._add_sell_strategy_config()
        
        # Market regime configuration
        self._add_market_regime_config()
        
        # Load enhanced configuration if exists
        self._load_enhanced_config()
    
    def _add_sell_strategy_config(self):
        """Add SELL strategy specific configuration"""
        
        # SELL Strategy Position Sizing (% of capital per signal strength)
        self.SELL_POSITION_SIZES = {
            'SUPER_EXTREME': 0.15,  # Smaller positions for SELL (higher risk)
            'EXTREME': 0.12,
            'STRONG': 0.10,
            'MODERATE': 0.08,
            'WEAK': 0.05
        }
        
        # SELL Strategy Lot Sizing
        self.SELL_LOTS_PER_SIGNAL = {
            'SUPER_EXTREME': 2,  # Conservative lot sizing for SELL
            'EXTREME': 2,
            'STRONG': 1,
            'MODERATE': 1,
            'WEAK': 0  # No SELL trades for weak signals
        }
        
        # SELL Strategy Risk Management
        self.SELL_STOP_LOSS_PERCENT = {
            'SUPER_EXTREME': 2.0,  # 200% of premium (unlimited risk awareness)
            'EXTREME': 1.5,        # 150% of premium
            'STRONG': 1.2,         # 120% of premium
            'MODERATE': 1.0        # 100% of premium
        }
        
        self.SELL_TARGET_PERCENT = {
            'SUPER_EXTREME': 0.5,  # 50% of premium (quick profits)
            'EXTREME': 0.6,        # 60% of premium
            'STRONG': 0.7,         # 70% of premium
            'MODERATE': 0.8        # 80% of premium
        }
        
        # Margin requirements (estimated)
        self.MARGIN_MULTIPLIER = {
            'CE': 1.8,  # 1.8x of premium as margin
            'PE': 2.0   # 2.0x of premium as margin (higher for puts)
        }
        
        # Strike selection for SELL strategies
        self.SELL_STRIKE_SELECTION = {
            'OTM_DISTANCE': {
                'RANGE_BOUND': 150,      # 150 points OTM in range-bound markets
                'LOW_VOLATILITY': 100,   # 100 points OTM in low vol
                'POST_GAP': 200,         # 200 points OTM after gaps
                'PULLBACK': 75           # 75 points OTM on pullbacks
            }
        }
    
    def _add_market_regime_config(self):
        """Add market regime detection configuration"""
        
        # Volatility thresholds
        self.VOLATILITY_THRESHOLDS = {
            'LOW_VOLATILITY': 25,      # Below 25th percentile
            'HIGH_VOLATILITY': 75,     # Above 75th percentile
            'EXTREME_VOLATILITY': 90   # Above 90th percentile
        }
        
        # Range detection parameters
        self.RANGE_DETECTION = {
            'MAX_RANGE_PERCENT': 2.0,    # Maximum 2% range for range-bound detection
            'MIN_CONSOLIDATION_HOURS': 2, # Minimum 2 hours of consolidation
            'RANGE_BREAKOUT_THRESHOLD': 1.5  # 1.5% breakout from range
        }
        
        # Market timing preferences
        self.MARKET_TIMING = {
            'SELL_PREFERRED_HOURS': [(10, 0), (15, 0)],  # 10 AM to 3 PM preferred for SELL
            'BUY_PREFERRED_HOURS': [(9, 15), (10, 0)],   # 9:15 AM to 10 AM preferred for BUY
            'AVOID_EXPIRY_HOURS': 2  # Avoid SELL trades 2 hours before expiry
        }
        
        # Strategy switching thresholds
        self.STRATEGY_SWITCHING = {
            'MIN_VOLATILITY_FOR_SELL': 15,  # Minimum 15% volatility for SELL
            'MAX_VOLATILITY_FOR_SELL': 35,  # Maximum 35% volatility for SELL
            'TREND_STRENGTH_FOR_BUY': 25000  # Above 25k trend strength prefers BUY
        }
    
    def _load_enhanced_config(self):
        """Load enhanced configuration from JSON"""
        config_file = self.CONFIG_DIR / "sell_strategy_config.json"
        
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    enhanced_config = json.load(f)
                
                # Load SELL strategy config
                if 'sell_strategy' in enhanced_config:
                    sell_config = enhanced_config['sell_strategy']
                    if 'position_sizing' in sell_config:
                        self.SELL_POSITION_SIZES.update(sell_config['position_sizing'])
                    if 'risk_management' in sell_config:
                        risk_config = sell_config['risk_management']
                        self.SELL_STOP_LOSS_PERCENT.update(risk_config.get('stop_loss_percent', {}))
                        self.SELL_TARGET_PERCENT.update(risk_config.get('target_percent', {}))
                
                # Load market regime config
                if 'market_regime' in enhanced_config:
                    regime_config = enhanced_config['market_regime']
                    self.VOLATILITY_THRESHOLDS.update(regime_config.get('volatility_thresholds', {}))
                    self.RANGE_DETECTION.update(regime_config.get('range_detection', {}))
                
                print("✅ Enhanced SELL strategy configuration loaded")
                
            except Exception as e:
                print(f"⚠️  Failed to load enhanced config: {e}")
    
    def save_enhanced_config(self):
        """Save enhanced configuration"""
        enhanced_config = {
            "sell_strategy": {
                "position_sizing": self.SELL_POSITION_SIZES,
                "lots_per_signal": self.SELL_LOTS_PER_SIGNAL,
                "risk_management": {
                    "stop_loss_percent": self.SELL_STOP_LOSS_PERCENT,
                    "target_percent": self.SELL_TARGET_PERCENT
                },
                "strike_selection": self.SELL_STRIKE_SELECTION,
                "margin_multiplier": self.MARGIN_MULTIPLIER
            },
            "market_regime": {
                "volatility_thresholds": self.VOLATILITY_THRESHOLDS,
                "range_detection": self.RANGE_DETECTION,
                "market_timing": self.MARKET_TIMING,
                "strategy_switching": self.STRATEGY_SWITCHING
            }
        }
        
        config_file = self.CONFIG_DIR / "sell_strategy_config.json"
        with open(config_file, 'w') as f:
            json.dump(enhanced_config, f, indent=2)
        
        print(f"✅ Enhanced configuration saved to {config_file}")
    
    def get_strategy_for_regime(self, market_regime: Dict) -> str:
        """Get recommended strategy based on market regime"""
        
        primary_regime = market_regime.get('primary_regime', 'NORMAL_TRENDING')
        volatility_percentile = market_regime.get('volatility_percentile', 50)
        
        # Strategy decision logic
        if primary_regime in ['RANGE_BOUND_LOW_VOL', 'POST_GAP_CONSOLIDATION']:
            return 'SELL_PREMIUM'
        elif volatility_percentile > self.VOLATILITY_THRESHOLDS['HIGH_VOLATILITY']:
            return 'BUY_OPTIONS'
        elif volatility_percentile < self.VOLATILITY_THRESHOLDS['LOW_VOLATILITY']:
            return 'SELL_PREMIUM'
        else:
            return 'BUY_OPTIONS'  # Default to original BUY strategy
    
    def is_sell_preferred_time(self) -> bool:
        """Check if current time is preferred for SELL strategies"""
        now = datetime.now()
        current_time = (now.hour, now.minute)
        
        for start_hour, start_min in self.MARKET_TIMING['SELL_PREFERRED_HOURS']:
            # Create time ranges (this is simplified - would need proper range checking)
            if start_hour <= now.hour <= 15:  # General SELL window
                return True
        
        return False


# Continue from Part 1...

# Import base classes from main.py
from main import RealTimeDataProvider, DashboardParser, TradingSystemLogger, DatabaseManager

class EnhancedSignalAnalyzer:
    """Enhanced signal analyzer that supports both BUY and SELL strategies based on market regime"""
    
    def __init__(self, config: EnhancedTradingSystemConfig, logger, market_regime_detector: MarketRegimeDetector):
        self.config = config
        self.logger = logger
        self.market_regime_detector = market_regime_detector
    
    def analyze_signal(self, dashboard_data: Dict) -> Optional[Dict]:
        """Enhanced signal analysis with market regime consideration"""
        try:
            cumulative_trend = dashboard_data.get('cumulative_trend', 0)
            confidence = dashboard_data.get('confidence', 0)
            combined_flow = dashboard_data.get('combined_flow', 0)
            
            self.logger.info(f"🔍 Enhanced Signal Analysis...")
            self.logger.info(f"   Cumulative Trend: {cumulative_trend:,.0f}")
            self.logger.info(f"   Confidence: {confidence}%")
            self.logger.info(f"   Combined Flow: {combined_flow:+.2f}M")
            
            # Detect current market regime
            market_regime = self.market_regime_detector.detect_market_regime()
            
            # Get recommended strategy based on regime
            recommended_strategy = self.config.get_strategy_for_regime(market_regime)
            
            self.logger.info(f"   Market Regime: {market_regime['primary_regime']}")
            self.logger.info(f"   Recommended Strategy: {recommended_strategy}")
            
            # Analyze signal based on strategy type
            if recommended_strategy == 'SELL_PREMIUM':
                signal = self._analyze_sell_signal(dashboard_data, market_regime)
            else:
                signal = self._analyze_buy_signal(dashboard_data, market_regime)
            
            if signal:
                signal['market_regime'] = market_regime
                signal['recommended_strategy'] = recommended_strategy
            
            return signal
            
        except Exception as e:
            self.logger.error(f"❌ Enhanced signal analysis failed: {str(e)}")
            return None
    
    def _analyze_sell_signal(self, dashboard_data: Dict, market_regime: Dict) -> Optional[Dict]:
        """Analyze signal for SELL premium strategies"""
        
        cumulative_trend = dashboard_data.get('cumulative_trend', 0)
        confidence = dashboard_data.get('confidence', 0)
        combined_flow = dashboard_data.get('combined_flow', 0)
        
        # Classify trend strength
        strength = self._classify_trend_strength(cumulative_trend)
        
        # Determine SELL signal direction based on overall bias and pullbacks
        sell_signal = self._determine_sell_direction(cumulative_trend, combined_flow, market_regime)
        
        if not sell_signal:
            return {
                'timestamp': dashboard_data.get('timestamp'),
                'signal_type': 'SKIP',
                'signal_direction': 'NEUTRAL',
                'option_type': 'NONE',
                'strength': strength,
                'should_trade': False,
                'skip_reason': 'No suitable SELL opportunity identified',
                'strategy_type': 'SELL_PREMIUM'
            }
        
        signal_type = sell_signal['signal_type']
        signal_direction = sell_signal['direction']
        option_type = sell_signal['option_type']
        
        # Check if SELL signal meets requirements
        should_trade, reason = self._should_sell_trade(strength, confidence, combined_flow, market_regime)
        
        # Calculate SELL position sizing
        lots = self.config.SELL_LOTS_PER_SIGNAL.get(strength, 0) if should_trade else 0
        position_size_pct = self.config.SELL_POSITION_SIZES.get(strength, 0) if should_trade else 0
        
        signal = {
            'timestamp': dashboard_data.get('timestamp'),
            'signal_type': signal_type,
            'signal_direction': signal_direction,
            'option_type': option_type,
            'strength': strength,
            'cumulative_trend': cumulative_trend,
            'confidence': confidence,
            'combined_flow': combined_flow,
            'should_trade': should_trade,
            'skip_reason': reason if not should_trade else None,
            'lots': lots,
            'position_size_pct': position_size_pct,
            'stop_loss_pct': self.config.SELL_STOP_LOSS_PERCENT.get(strength, 1.0),
            'target_pct': self.config.SELL_TARGET_PERCENT.get(strength, 0.5),
            'strategy_type': 'SELL_PREMIUM',
            'sell_reasoning': sell_signal.get('reasoning', ''),
            'volatility_percentile': market_regime.get('volatility_percentile', 50),
            'range_bounds': market_regime.get('range_analysis', {}).get('range_bounds', {})
        }
        
        if should_trade:
            self.logger.info(f"✅ SELL SIGNAL: {signal_type} - {strength} ({confidence}% confidence)")
            self.logger.info(f"   Reasoning: {sell_signal.get('reasoning', '')}")
            self.logger.info(f"   Recommended lots: {signal['lots']}")
            self.logger.info(f"   Position size: {position_size_pct:.1%}")
        else:
            self.logger.info(f"❌ SKIP SELL SIGNAL: {reason}")
        
        return signal
    
    def _analyze_buy_signal(self, dashboard_data: Dict, market_regime: Dict) -> Optional[Dict]:
        """Analyze signal for traditional BUY strategies (fallback to original logic)"""
        
        # Use original BUY signal logic from main.py
        from main import SignalAnalyzer
        original_analyzer = SignalAnalyzer(self.config, self.logger)
        
        signal = original_analyzer.analyze_signal(dashboard_data)
        
        if signal:
            signal['strategy_type'] = 'BUY_OPTIONS'
            signal['market_regime'] = market_regime
        
        return signal
    
    def _determine_sell_direction(self, cumulative_trend: float, combined_flow: float, market_regime: Dict) -> Optional[Dict]:
        """Determine SELL signal direction based on market conditions"""
        
        abs_trend = abs(cumulative_trend)
        regime_type = market_regime.get('primary_regime', 'NORMAL_TRENDING')
        range_analysis = market_regime.get('range_analysis', {})
        
        # Strategy 1: Range-bound market - sell both sides
        if regime_type in ['RANGE_BOUND_LOW_VOL', 'POST_GAP_CONSOLIDATION']:
            
            # In range-bound markets, sell based on position within range
            position_in_range = range_analysis.get('position_in_range', 0.5)
            
            if position_in_range > 0.7:  # Near resistance, sell calls
                return {
                    'signal_type': 'SELL_CE',
                    'direction': 'BEARISH_BIAS',
                    'option_type': 'CE',
                    'reasoning': f'Range-bound market, price near resistance ({position_in_range:.1%} of range)'
                }
            elif position_in_range < 0.3:  # Near support, sell puts
                return {
                    'signal_type': 'SELL_PE',
                    'direction': 'BULLISH_BIAS',
                    'option_type': 'PE',
                    'reasoning': f'Range-bound market, price near support ({position_in_range:.1%} of range)'
                }
            else:  # Middle of range - prefer selling in direction of overall flow
                if combined_flow > 0:  # Bullish flow, sell puts on dips
                    return {
                        'signal_type': 'SELL_PE',
                        'direction': 'BULLISH_BIAS',
                        'option_type': 'PE',
                        'reasoning': 'Range-bound with bullish flow, selling puts on dip'
                    }
                elif combined_flow < 0:  # Bearish flow, sell calls on rallies
                    return {
                        'signal_type': 'SELL_CE',
                        'direction': 'BEARISH_BIAS',
                        'option_type': 'CE',
                        'reasoning': 'Range-bound with bearish flow, selling calls on rally'
                    }
        
        # Strategy 2: Trending market pullbacks
        elif abs_trend > 15000:  # Strong trend detected
            
            # Bullish trend - look for pullbacks to sell puts
            if cumulative_trend > 0 and combined_flow > 0:
                # Check if this is a pullback in bullish trend
                if self._is_pullback_opportunity(cumulative_trend, combined_flow, 'BULLISH'):
                    return {
                        'signal_type': 'SELL_PE',
                        'direction': 'BULLISH_TREND_PULLBACK',
                        'option_type': 'PE',
                        'reasoning': f'Pullback in bullish trend (trend: {cumulative_trend:,.0f}, flow: {combined_flow:+.1f}M)'
                    }
            
            # Bearish trend - look for pullbacks to sell calls
            elif cumulative_trend < 0 and combined_flow < 0:
                # Check if this is a pullback in bearish trend
                if self._is_pullback_opportunity(cumulative_trend, combined_flow, 'BEARISH'):
                    return {
                        'signal_type': 'SELL_CE',
                        'direction': 'BEARISH_TREND_PULLBACK',
                        'option_type': 'CE',
                        'reasoning': f'Pullback in bearish trend (trend: {cumulative_trend:,.0f}, flow: {combined_flow:+.1f}M)'
                    }
        
        # Strategy 3: Low volatility environment - sell premium
        volatility_percentile = market_regime.get('volatility_percentile', 50)
        if volatility_percentile < 30:  # Low volatility
            
            # Sell in direction opposite to recent small moves
            if abs_trend > 5000:  # Some directional bias
                if cumulative_trend > 0:  # Recent bullish, sell calls expecting reversion
                    return {
                        'signal_type': 'SELL_CE',
                        'direction': 'MEAN_REVERSION',
                        'option_type': 'CE',
                        'reasoning': f'Low volatility mean reversion, recent move up {cumulative_trend:,.0f}'
                    }
                else:  # Recent bearish, sell puts expecting reversion
                    return {
                        'signal_type': 'SELL_PE',
                        'direction': 'MEAN_REVERSION',
                        'option_type': 'PE',
                        'reasoning': f'Low volatility mean reversion, recent move down {cumulative_trend:,.0f}'
                    }
        
        # No suitable SELL opportunity
        return None
    
    def _is_pullback_opportunity(self, cumulative_trend: float, combined_flow: float, trend_direction: str) -> bool:
        """Determine if current conditions represent a pullback opportunity"""
        
        abs_trend = abs(cumulative_trend)
        
        if trend_direction == 'BULLISH':
            # Bullish trend pullback: strong overall trend but recent weakening
            return (cumulative_trend > 25000 and  # Strong bullish trend
                    combined_flow > -5 and          # Not too negative flow
                    abs_trend < 50000)               # Not extremely strong (allow for pullback)
        
        elif trend_direction == 'BEARISH':
            # Bearish trend pullback: strong overall trend but recent weakening
            return (cumulative_trend < -25000 and   # Strong bearish trend
                    combined_flow < 5 and           # Not too positive flow
                    abs_trend < 50000)               # Not extremely strong (allow for pullback)
        
        return False
    
    def _should_sell_trade(self, strength: str, confidence: float, combined_flow: float, 
                          market_regime: Dict) -> Tuple[bool, str]:
        """Determine if we should execute SELL trade based on enhanced criteria"""
        
        # Skip weak signals entirely for SELL trades
        if strength == 'WEAK':
            return False, "SELL strategy requires stronger signals (minimum MODERATE)"
        
        # Check confidence requirements (higher for SELL due to unlimited risk)
        min_confidence = self.config.MIN_CONFIDENCE.get(strength, 90)
        min_confidence += 5  # Add 5% confidence requirement for SELL trades
        
        if confidence < min_confidence:
            return False, f"SELL requires higher confidence: {confidence}% < {min_confidence}%"
        
        # Check market timing
        if not self.config.is_sell_preferred_time():
            return False, "Outside preferred SELL trading hours (10 AM - 3 PM)"
        
        # Check volatility suitability for SELL
        volatility_percentile = market_regime.get('volatility_percentile', 50)
        
        if volatility_percentile > 80:  # Too high volatility
            return False, f"Volatility too high for SELL ({volatility_percentile:.0f}%ile > 80%ile)"
        
        if volatility_percentile < 10:  # Too low volatility
            return False, f"Volatility too low for SELL ({volatility_percentile:.0f}%ile < 10%ile)"
        
        # Check range-bound conditions for range-based SELL
        regime_type = market_regime.get('primary_regime', '')
        if regime_type in ['RANGE_BOUND_LOW_VOL', 'POST_GAP_CONSOLIDATION']:
            range_analysis = market_regime.get('range_analysis', {})
            range_size_percent = range_analysis.get('range_size_percent', 3.0)
            
            if range_size_percent > 2.5:  # Range too wide
                return False, f"Range too wide for SELL strategy ({range_size_percent:.1f}% > 2.5%)"
        
        # For moderate signals, require additional confirmation
        if strength == 'MODERATE':
            if abs(combined_flow) < 3:  # Require at least 3M flow for moderate SELL signals
                return False, "Moderate SELL signal needs stronger flow confirmation (>3M)"
        
        # Check expiry proximity (avoid SELL near expiry)
        current_time = datetime.now()
        if current_time.hour >= 14:  # After 2 PM on expiry day
            if current_time.weekday() == 3:  # Thursday (weekly expiry)
                return False, "Too close to weekly expiry for SELL trades"
        
        # All checks passed
        return True, None
    
    def _classify_trend_strength(self, trend_value: float) -> str:
        """Classify trend strength (same as original but with SELL considerations)"""
        abs_trend = abs(trend_value)
        
        if abs_trend >= self.config.TREND_THRESHOLDS['SUPER_EXTREME']:
            return 'SUPER_EXTREME'
        elif abs_trend >= self.config.TREND_THRESHOLDS['EXTREME']:
            return 'EXTREME'
        elif abs_trend >= self.config.TREND_THRESHOLDS['STRONG']:
            return 'STRONG'
        elif abs_trend >= self.config.TREND_THRESHOLDS['MODERATE']:
            return 'MODERATE'
        else:
            return 'WEAK'




###


# !/usr/bin/env python3
"""
Gamma-Enhanced Options Trading System - SELL Strategy Version - COMPLETE
Part 2: Complete Implementation with SELL Strategy Support

Continuation from where main_sell.py left off...
"""


# ... (All previous code from main_sell.py remains the same until EnhancedPortfolioManager) ...

class EnhancedPortfolioManager:
    """Enhanced portfolio manager supporting both BUY and SELL strategies"""

    def __init__(self, config: EnhancedTradingSystemConfig, db_manager, logger):
        # Initialize with base portfolio manager functionality
        from main import RealTimeDataProvider

        self.config = config
        self.db = db_manager
        self.logger = logger

        # Portfolio state (inherited from base)
        self.current_capital = config.STARTING_CAPITAL
        self.available_capital = config.STARTING_CAPITAL
        self.capital_at_risk = 0.0
        self.total_pnl = 0.0
        self.daily_pnl = 0.0

        # Enhanced tracking for SELL trades
        self.margin_utilized = 0.0  # Total margin for SELL trades
        self.premium_collected = 0.0  # Total premium collected today
        self.max_margin_limit = config.STARTING_CAPITAL * 0.6  # 60% max margin utilization

        # Trade tracking
        self.active_positions: Dict[str, TradePosition] = {}
        self.trades_today = 0
        self.consecutive_losses = 0
        self.last_trade_outcome = None

        # Risk tracking
        self.daily_loss_amount = 0.0
        self.max_drawdown_today = 0.0

        # Initialize data provider and load state
        self.data_provider = RealTimeDataProvider()
        self.data_provider.set_logger(logger)
        self._load_portfolio_state()

    def _load_portfolio_state(self):
        """Load current portfolio state from database including SELL trades"""
        try:
            # Load active positions
            conn = self.db.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT * FROM active_trades WHERE status = 'ACTIVE'
            """)

            active_trades = cursor.fetchall()
            for trade_row in active_trades:
                position = self._row_to_enhanced_position(trade_row)
                self.active_positions[position.trade_id] = position

                # For SELL trades, track margin utilization
                if position.signal_type.startswith('SELL_'):
                    self.margin_utilized += position.investment_amount
                    if hasattr(position, 'premium_received'):
                        self.premium_collected += position.premium_received
                else:
                    # For BUY trades, track capital at risk
                    self.capital_at_risk += position.investment_amount

            # Load today's trade count
            cursor.execute("""
                SELECT COUNT(*) FROM trade_history 
                WHERE DATE(entry_time) = DATE('now')
            """)
            self.trades_today = cursor.fetchone()[0]

            # Load portfolio metrics
            cursor.execute("""
                SELECT value FROM system_state WHERE key = 'current_capital'
            """)
            result = cursor.fetchone()
            if result:
                self.current_capital = float(result[0])
                self.available_capital = self.current_capital - self.capital_at_risk

            conn.close()

            self.logger.info(f"💼 Enhanced Portfolio loaded:")
            self.logger.info(f"   Active positions: {len(self.active_positions)}")
            self.logger.info(f"   Available capital: ₹{self.available_capital:,.2f}")
            self.logger.info(f"   Margin utilized: ₹{self.margin_utilized:,.2f}")
            self.logger.info(f"   Premium collected: ₹{self.premium_collected:,.2f}")

        except Exception as e:
            self.logger.error(f"❌ Failed to load enhanced portfolio state: {str(e)}")

    def _row_to_enhanced_position(self, row) -> TradePosition:
        """Convert database row to enhanced TradePosition object"""
        # Basic position creation
        position = TradePosition(
            trade_id=row[0],
            signal_type=row[1],
            option_type=row[2] if len(row) > 2 else 'CE',
            strike_price=row[3] if len(row) > 3 else 0,
            lots=row[4] if len(row) > 4 else 1,
            quantity=row[5] if len(row) > 5 else 75,
            entry_price=row[6] if len(row) > 6 else 0,
            target_price=row[7] if len(row) > 7 else 0,
            stop_loss_price=row[8] if len(row) > 8 else 0,
            entry_time=datetime.fromisoformat(row[9]) if len(row) > 9 else datetime.now(),
            expiry_date=date.fromisoformat(row[10]) if len(row) > 10 else date.today(),
            investment_amount=row[11] if len(row) > 11 else 0,
            premium_received=row[12] if len(row) > 12 else 0,  # Enhanced for SELL
            signal_strength=row[13] if len(row) > 13 else 'MODERATE',
            cumulative_trend=row[14] if len(row) > 14 else 0,
            confidence=row[15] if len(row) > 15 else 75,
            market_regime=row[16] if len(row) > 16 else 'UNKNOWN',
            volatility_percentile=row[17] if len(row) > 17 else 50,
            range_bounds=eval(row[18]) if len(row) > 18 and row[18] else {},
            current_price=row[19] if len(row) > 19 else 0,
            current_pnl=row[20] if len(row) > 20 else 0,
            roi_percent=row[21] if len(row) > 21 else 0,
            max_profit_achieved=row[22] if len(row) > 22 else 0,
            max_drawdown_faced=row[23] if len(row) > 23 else 0,
            status=row[24] if len(row) > 24 else 'ACTIVE'
        )
        return position

    def execute_trade(self, signal: Dict) -> Optional[TradePosition]:
        """Execute trade based on strategy type (BUY or SELL)"""
        try:
            strategy_type = signal.get('strategy_type', 'BUY_OPTIONS')

            if strategy_type == 'SELL_PREMIUM':
                return self.execute_sell_trade(signal)
            else:
                return self.execute_buy_trade(signal)

        except Exception as e:
            self.logger.error(f"❌ Trade execution failed: {str(e)}")
            return None

    def execute_buy_trade(self, signal: Dict) -> Optional[TradePosition]:
        """Execute traditional BUY trade (fallback to original logic)"""
        try:
            # Use original BUY trade logic from main.py
            from main import PortfolioManager

            # Create temporary portfolio manager for BUY execution
            temp_portfolio = PortfolioManager(self.config, self.db, self.logger)
            temp_portfolio.current_capital = self.current_capital
            temp_portfolio.available_capital = self.available_capital
            temp_portfolio.capital_at_risk = self.capital_at_risk
            temp_portfolio.active_positions = {k: v for k, v in self.active_positions.items()
                                               if not v.signal_type.startswith('SELL_')}

            # Execute BUY trade
            position = temp_portfolio.execute_trade(signal)

            if position:
                # Add to our enhanced portfolio
                self.active_positions[position.trade_id] = position
                self.capital_at_risk += position.investment_amount
                self.available_capital -= position.investment_amount
                self.trades_today += 1

                self.logger.info(f"🎯 BUY TRADE EXECUTED via enhanced portfolio: {position.trade_id}")

            return position

        except Exception as e:
            self.logger.error(f"❌ BUY trade execution failed: {str(e)}")
            return None

    def execute_sell_trade(self, signal: Dict) -> Optional[TradePosition]:
        """Execute SELL trade with enhanced margin and premium tracking"""
        try:
            # Check if we can trade
            can_trade, reason = self.can_trade(signal)
            if not can_trade:
                self.logger.warning(f"⚠️  Cannot execute SELL trade: {reason}")
                return None

            # Calculate SELL position sizing
            position_data = self.calculate_sell_position_size(signal)
            if not position_data:
                self.logger.error("❌ Failed to calculate SELL position size")
                return None

            # Generate trade ID
            trade_id = self._generate_trade_id()

            # Get current spot price and calculate OTM strike
            spot_price = self._get_current_nifty_spot()
            strike_price = self._calculate_sell_optimal_strike(spot_price, signal)

            # Get real option price for SELL (premium we'll receive)
            option_type = signal.get('option_type', 'CE')
            entry_price = self._get_real_option_price(strike_price, option_type)

            # For SELL trades: target is lower price (buy back cheaper)
            # Stop loss is higher price (buy back more expensive)
            target_price = entry_price * (1 - signal.get('target_pct', 0.5))  # 50% profit target
            stop_loss_price = entry_price * (1 + signal.get('stop_loss_pct', 1.0))  # 100% loss limit

            # Calculate actual amounts
            margin_required = position_data['margin_required']
            premium_collected = entry_price * position_data['quantity']

            # Create enhanced trade position for SELL
            position = TradePosition(
                trade_id=trade_id,
                signal_type=signal['signal_type'],  # SELL_CE or SELL_PE
                option_type=option_type,
                strike_price=strike_price,
                lots=position_data['lots'],
                quantity=position_data['quantity'],
                entry_price=entry_price,  # Premium received
                target_price=target_price,
                stop_loss_price=stop_loss_price,
                entry_time=datetime.now(),
                expiry_date=self.config.calculate_expiry_date(),
                investment_amount=margin_required,  # Margin blocked
                premium_received=premium_collected,  # NEW: Track premium
                signal_strength=signal['strength'],
                cumulative_trend=signal['cumulative_trend'],
                confidence=signal['confidence'],
                market_regime=signal.get('market_regime', {}).get('primary_regime', 'UNKNOWN'),
                volatility_percentile=signal.get('volatility_percentile', 50),
                range_bounds=signal.get('range_bounds', {}),
                current_price=entry_price,
                current_pnl=0.0,  # Will be calculated as (entry_price - current_price) * quantity
                roi_percent=0.0
            )

            # Save to enhanced database
            if self._save_sell_trade_to_db(position):
                # Update portfolio state for SELL trade
                self.active_positions[trade_id] = position
                self.margin_utilized += margin_required
                self.premium_collected += premium_collected
                self.available_capital += premium_collected  # We receive premium immediately
                self.trades_today += 1

                # Log SELL trade execution
                self.logger.info(f"💰 SELL TRADE EXECUTED: {trade_id}")
                self.logger.info(f"   Type: {position.signal_type} | Strike: {position.strike_price} (OTM)")
                self.logger.info(f"   Premium Received: ₹{premium_collected:,.2f}")
                self.logger.info(f"   Margin Required: ₹{margin_required:,.2f}")
                self.logger.info(f"   Real Spot Price: ₹{spot_price:.2f}")
                self.logger.info(f"   Lots: {position.lots} | Quantity: {position.quantity}")
                self.logger.info(f"   Target: ₹{position.target_price:.2f} | SL: ₹{position.stop_loss_price:.2f}")
                self.logger.info(f"   Market Regime: {position.market_regime}")
                self.logger.info(f"   Reasoning: {signal.get('sell_reasoning', 'N/A')}")
                self.logger.info(f"   Expiry: {position.expiry_date}")

                # Create portfolio snapshot
                self._create_portfolio_snapshot("SELL_TRADE_ENTRY", trade_id)

                return position
            else:
                self.logger.error(f"❌ Failed to save SELL trade to database")
                return None

        except Exception as e:
            self.logger.error(f"❌ SELL trade execution failed: {str(e)}")
            return None

    def can_trade(self, signal: Dict) -> Tuple[bool, str]:
        """Enhanced trade validation for both BUY and SELL strategies"""

        # Base checks
        if self.trades_today >= self.config.MAX_TRADES_PER_DAY:
            return False, f"Daily trade limit reached ({self.config.MAX_TRADES_PER_DAY})"

        if len(self.active_positions) >= self.config.MAX_ACTIVE_POSITIONS:
            return False, f"Max active positions reached ({self.config.MAX_ACTIVE_POSITIONS})"

        if not self.config.is_market_hours():
            return False, "Market is closed"

        if abs(self.daily_pnl) > self.current_capital * self.config.DAILY_LOSS_LIMIT:
            return False, f"Daily loss limit exceeded ({self.config.DAILY_LOSS_LIMIT:.1%})"

        if self.consecutive_losses >= self.config.CONSECUTIVE_LOSS_LIMIT:
            return False, f"Consecutive loss limit reached ({self.config.CONSECUTIVE_LOSS_LIMIT})"

        # Strategy-specific checks
        strategy_type = signal.get('strategy_type', 'BUY_OPTIONS')

        if strategy_type == 'SELL_PREMIUM':
            return self._can_sell_trade(signal)
        else:
            return self._can_buy_trade(signal)

    def _can_buy_trade(self, signal: Dict) -> Tuple[bool, str]:
        """Validate BUY trade"""
        required_capital = self.current_capital * signal.get('position_size_pct', 0.15)

        if required_capital > self.available_capital:
            return False, f"Insufficient capital for BUY (need ₹{required_capital:,.0f}, have ₹{self.available_capital:,.0f})"

        potential_risk = self.capital_at_risk + required_capital
        risk_pct = potential_risk / self.current_capital

        if risk_pct > self.config.PORTFOLIO_RISK_LIMIT:
            return False, f"Portfolio risk limit exceeded ({risk_pct:.1%} > {self.config.PORTFOLIO_RISK_LIMIT:.1%})"

        return True, "BUY trade validation passed"

    def _can_sell_trade(self, signal: Dict) -> Tuple[bool, str]:
        """Validate SELL trade with margin considerations"""

        # Estimate required margin for SELL trade
        estimated_premium = self._estimate_sell_premium(signal)
        option_type = signal.get('option_type', 'CE')
        estimated_margin = estimated_premium * self.config.MARGIN_MULTIPLIER.get(option_type, 2.0)
        estimated_margin *= signal.get('lots', 1) * self.config.NIFTY_LOT_SIZE

        # Check margin availability
        if self.margin_utilized + estimated_margin > self.max_margin_limit:
            return False, f"Insufficient margin for SELL (need ₹{estimated_margin:,.0f}, available ₹{self.max_margin_limit - self.margin_utilized:,.0f})"

        # Check if we have too many SELL positions (higher risk)
        sell_positions = sum(1 for pos in self.active_positions.values()
                             if pos.signal_type.startswith('SELL_'))

        max_sell_positions = 2  # Maximum 2 SELL positions due to higher risk
        if sell_positions >= max_sell_positions:
            return False, f"Maximum SELL positions reached ({sell_positions}/{max_sell_positions})"

        # Check margin risk vs total portfolio
        margin_risk_pct = (self.margin_utilized + estimated_margin) / self.current_capital
        max_margin_risk = 0.4  # Maximum 40% of capital as margin

        if margin_risk_pct > max_margin_risk:
            return False, f"Margin risk too high ({margin_risk_pct:.1%} > {max_margin_risk:.1%})"

        return True, "SELL trade validation passed"

    def _estimate_sell_premium(self, signal: Dict) -> float:
        """Estimate premium that would be received for SELL trade"""
        signal_strength = signal.get('strength', 'MODERATE')
        market_regime = signal.get('market_regime', {})
        volatility_percentile = market_regime.get('volatility_percentile', 50)

        # Base premium estimates (what we'd receive for selling)
        base_premiums = {
            'SUPER_EXTREME': 80.0,  # Lower premium for far OTM
            'EXTREME': 100.0,  # Medium premium
            'STRONG': 120.0,  # Higher premium for closer strikes
            'MODERATE': 90.0,  # Medium premium
            'WEAK': 60.0  # Lower premium
        }

        base_premium = base_premiums.get(signal_strength, 90.0)

        # Adjust for volatility (higher volatility = higher premiums)
        vol_multiplier = 0.8 + (volatility_percentile / 100) * 0.4  # 0.8 to 1.2 multiplier

        return base_premium * vol_multiplier

    def calculate_sell_position_size(self, signal: Dict) -> Dict[str, Any]:
        """Calculate position sizing for SELL trades with margin considerations"""
        try:
            signal_strength = signal.get('strength', 'MODERATE')

            # Use SELL-specific position sizing
            position_size_pct = self.config.SELL_POSITION_SIZES.get(signal_strength, 0.10)

            # Calculate lots (conservative for SELL)
            lots = max(1, self.config.SELL_LOTS_PER_SIGNAL.get(signal_strength, 1))

            # Calculate quantity
            quantity = lots * self.config.NIFTY_LOT_SIZE

            # Estimate premium we'll receive
            estimated_premium = self._estimate_sell_premium(signal)

            # Calculate margin requirement
            option_type = signal.get('option_type', 'CE')
            margin_multiplier = self.config.MARGIN_MULTIPLIER.get(option_type, 2.0)
            estimated_margin = estimated_premium * quantity * margin_multiplier

            # Premium collected (our income)
            premium_collected = estimated_premium * quantity

            # Ensure margin doesn't exceed our limits
            max_affordable_margin = self.max_margin_limit - self.margin_utilized

            if estimated_margin > max_affordable_margin:
                # Reduce lots to fit within margin limit
                max_lots = int(
                    max_affordable_margin / (estimated_premium * self.config.NIFTY_LOT_SIZE * margin_multiplier))
                lots = max(1, max_lots)
                quantity = lots * self.config.NIFTY_LOT_SIZE
                estimated_margin = estimated_premium * quantity * margin_multiplier
                premium_collected = estimated_premium * quantity

            return {
                'lots': lots,
                'quantity': quantity,
                'estimated_premium': estimated_premium,
                'margin_required': estimated_margin,
                'premium_collected': premium_collected,
                'position_size_pct': estimated_margin / self.current_capital
            }

        except Exception as e:
            self.logger.error(f"❌ SELL position sizing calculation failed: {str(e)}")
            return None

    def _calculate_sell_optimal_strike(self, spot_price: float, signal: Dict) -> float:
        """Calculate optimal OTM strike for SELL trades"""
        signal_type = signal.get('signal_type', 'SELL_CE')
        market_regime = signal.get('market_regime', {})
        regime_type = market_regime.get('primary_regime', 'NORMAL_TRENDING')

        # Get OTM distance based on market regime
        if regime_type == 'RANGE_BOUND_LOW_VOL':
            otm_distance = self.config.SELL_STRIKE_SELECTION['OTM_DISTANCE']['RANGE_BOUND']
        elif regime_type == 'POST_GAP_CONSOLIDATION':
            otm_distance = self.config.SELL_STRIKE_SELECTION['OTM_DISTANCE']['POST_GAP']
        elif 'PULLBACK' in signal.get('signal_direction', ''):
            otm_distance = self.config.SELL_STRIKE_SELECTION['OTM_DISTANCE']['PULLBACK']
        else:
            otm_distance = self.config.SELL_STRIKE_SELECTION['OTM_DISTANCE']['LOW_VOLATILITY']

        # Calculate strike based on signal type
        if signal_type == 'SELL_CE':
            # Sell calls above current price (expect price to stay below)
            strike = spot_price + otm_distance
        else:  # SELL_PE
            # Sell puts below current price (expect price to stay above)
            strike = spot_price - otm_distance

        # Round to nearest 50 (standard Nifty strike intervals)
        strike = round(strike / 50) * 50

        return strike

    def _get_current_nifty_spot(self) -> float:
        """Get current Nifty spot price"""
        try:
            spot_price = self.data_provider.get_current_spot_price()
            return spot_price
        except Exception as e:
            self.logger.error(f"❌ Failed to get real spot price: {e}")
            return 19500.0  # Fallback

    def _get_real_option_price(self, strike_price: float, option_type: str) -> float:
        """Get real option price from database"""
        try:
            option_price = self.data_provider.get_option_price(strike_price, option_type)
            return option_price
        except Exception as e:
            self.logger.error(f"❌ Failed to get real option price: {e}")
            return self._estimate_sell_premium({'strength': 'MODERATE'})

    def _generate_trade_id(self) -> str:
        """Generate unique trade ID"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"S_{timestamp}"  # S for SELL trades

    def update_position_prices(self):
        """Update current prices for all active positions (both BUY and SELL)"""
        try:
            if not self.active_positions:
                return

            for trade_id, position in self.active_positions.items():
                if position.signal_type.startswith('SELL_'):
                    # Update SELL position with inverted P&L calculation
                    self.update_sell_position_prices(trade_id)
                else:
                    # Update BUY position with normal P&L calculation
                    self.update_buy_position_prices(trade_id)

        except Exception as e:
            self.logger.error(f"❌ Position price update failed: {str(e)}")

    def update_buy_position_prices(self, trade_id: str, manual_price: float = None) -> bool:
        """Update BUY position prices (original logic)"""
        try:
            if trade_id not in self.active_positions:
                return False

            position = self.active_positions[trade_id]

            if manual_price is not None:
                current_price = manual_price
            else:
                current_price = self._get_real_option_price(position.strike_price, position.option_type)

            # For BUY trades: P&L = (current_price - entry_price) * quantity
            price_change = current_price - position.entry_price
            position.current_price = current_price
            position.current_pnl = price_change * position.quantity
            position.roi_percent = (price_change / position.entry_price) * 100

            # Update max profit/drawdown
            if position.current_pnl > position.max_profit_achieved:
                position.max_profit_achieved = position.current_pnl

            if position.current_pnl < position.max_drawdown_faced:
                position.max_drawdown_faced = position.current_pnl

            # Update in database
            self._update_position_in_db(position)

            # Check BUY exit conditions
            exit_reason = self._check_buy_exit_conditions(position)
            if exit_reason:
                self.exit_buy_trade(trade_id, exit_reason, current_price)

            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to update BUY position prices: {str(e)}")
            return False

    def update_sell_position_prices(self, trade_id: str, manual_price: float = None) -> bool:
        """Update SELL position with current market price (P&L calculation is inverted)"""
        try:
            if trade_id not in self.active_positions:
                return False

            position = self.active_positions[trade_id]

            if manual_price is not None:
                current_price = manual_price
            else:
                # Get real current price
                current_price = self._get_real_option_price(position.strike_price, position.option_type)

            # For SELL trades: P&L = (entry_price - current_price) * quantity
            # When current price goes down, we profit (can buy back cheaper)
            # When current price goes up, we lose (have to buy back more expensive)
            price_change = position.entry_price - current_price
            position.current_price = current_price
            position.current_pnl = price_change * position.quantity
            position.roi_percent = (price_change / position.entry_price) * 100

            # Update max profit/drawdown
            if position.current_pnl > position.max_profit_achieved:
                position.max_profit_achieved = position.current_pnl

            if position.current_pnl < position.max_drawdown_faced:
                position.max_drawdown_faced = position.current_pnl

            # Update in database
            self._update_position_in_db(position)

            # Check SELL-specific exit conditions
            exit_reason = self._check_sell_exit_conditions(position)
            if exit_reason:
                self.exit_sell_trade(trade_id, exit_reason, current_price)

            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to update SELL position prices: {str(e)}")
            return False

    def _check_buy_exit_conditions(self, position: TradePosition) -> Optional[str]:
        """Check if BUY position should be exited (original logic)"""
        current_price = position.current_price

        # Target hit
        if ((position.signal_type == 'BUY_CE' and current_price >= position.target_price) or
                (position.signal_type == 'BUY_PE' and current_price >= position.target_price)):
            return "TARGET_HIT"

        # Stop loss hit
        if current_price <= position.stop_loss_price:
            return "STOP_LOSS"

        # Expiry check
        if position.expiry_date == date.today():
            return "EXPIRY"

        # Time-based exit
        hours_since_entry = (datetime.now() - position.entry_time).total_seconds() / 3600
        if hours_since_entry > 6:
            return "TIME_EXIT"

        return None

    def _check_sell_exit_conditions(self, position: TradePosition) -> Optional[str]:
        """Check if SELL position should be exited"""
        current_price = position.current_price

        # Target hit (buy back at lower price for profit)
        if current_price <= position.target_price:
            return "TARGET_HIT"

        # Stop loss hit (buy back at higher price to limit loss)
        if current_price >= position.stop_loss_price:
            return "STOP_LOSS"

        # Expiry check (close on expiry day)
        if position.expiry_date == date.today():
            return "EXPIRY"

        # Time-based exit for SELL trades (close before last hour on expiry day)
        if position.expiry_date == date.today() and datetime.now().hour >= 14:
            return "EXPIRY_PROXIMITY"

        # Volatility expansion exit (close SELL when vol spikes)
        if hasattr(position, 'volatility_percentile'):
            current_vol = self._get_current_volatility_percentile()
            if current_vol > position.volatility_percentile + 30:  # 30 percentile increase
                return "VOLATILITY_EXPANSION"

        return None

    def _get_current_volatility_percentile(self) -> float:
        """Get current volatility percentile"""
        # Would implement real volatility calculation
        import random
        return random.uniform(20, 80)

    def exit_buy_trade(self, trade_id: str, exit_reason: str, exit_price: float) -> bool:
        """Exit BUY trade position (original logic)"""
        try:
            if trade_id not in self.active_positions:
                return False

            position = self.active_positions[trade_id]
            exit_time = datetime.now()

            # Calculate final P&L for BUY trade
            final_pnl = (exit_price - position.entry_price) * position.quantity
            final_roi = ((exit_price - position.entry_price) / position.entry_price) * 100

            # Calculate brokerage
            brokerage = position.lots * self.config.BROKERAGE_PER_LOT * 2
            net_pnl = final_pnl - brokerage

            # Update position
            position.current_price = exit_price
            position.current_pnl = final_pnl
            position.roi_percent = final_roi
            position.status = "COMPLETED"

            # Save to trade history
            if self._save_to_trade_history(position, exit_time, exit_reason, exit_price, net_pnl):
                # Remove from active trades
                self._remove_from_active_trades(trade_id)

                # Update portfolio state
                del self.active_positions[trade_id]
                self.capital_at_risk -= position.investment_amount
                self.available_capital += position.investment_amount + net_pnl
                self.current_capital += net_pnl
                self.daily_pnl += net_pnl

                # Update consecutive losses/wins
                if net_pnl > 0:
                    self.consecutive_losses = 0
                    self.last_trade_outcome = "WIN"
                else:
                    self.consecutive_losses += 1
                    self.last_trade_outcome = "LOSS"

                self.logger.info(f"🚪 BUY TRADE EXITED: {trade_id}")
                self.logger.info(f"   Reason: {exit_reason} | Net P&L: ₹{net_pnl:,.2f}")

                return True

        except Exception as e:
            self.logger.error(f"❌ Failed to exit BUY trade: {str(e)}")
            return False

    def exit_sell_trade(self, trade_id: str, exit_reason: str, exit_price: float) -> bool:
        """Exit SELL trade position with proper P&L calculation"""
        try:
            if trade_id not in self.active_positions:
                self.logger.warning(f"⚠️  SELL trade {trade_id} not found in active positions")
                return False

            position = self.active_positions[trade_id]
            exit_time = datetime.now()

            # For SELL trades: Final P&L = (entry_price - exit_price) * quantity
            # We received entry_price, now we pay exit_price to close
            final_pnl = (position.entry_price - exit_price) * position.quantity
            final_roi = ((position.entry_price - exit_price) / position.entry_price) * 100

            # Calculate brokerage (entry + exit)
            brokerage = position.lots * self.config.BROKERAGE_PER_LOT * 2
            net_pnl = final_pnl - brokerage

            # Update position
            position.current_price = exit_price
            position.current_pnl = final_pnl
            position.roi_percent = final_roi
            position.status = "COMPLETED"

            # Save to trade history
            if self._save_to_trade_history(position, exit_time, exit_reason, exit_price, net_pnl):
                # Remove from active trades
                self._remove_from_active_trades(trade_id)

                # Update portfolio state for SELL trade
                del self.active_positions[trade_id]
                self.margin_utilized -= position.investment_amount  # Release margin
                self.available_capital += net_pnl  # Add/subtract final P&L
                self.current_capital += net_pnl
                self.daily_pnl += net_pnl

                # Update consecutive losses/wins
                if net_pnl > 0:
                    self.consecutive_losses = 0
                    self.last_trade_outcome = "WIN"
                else:
                    self.consecutive_losses += 1
                    self.last_trade_outcome = "LOSS"

                # Log SELL trade exit
                self.logger.info(f"🚪 SELL TRADE EXITED: {trade_id}")
                self.logger.info(f"   Reason: {exit_reason}")
                self.logger.info(f"   Exit Price: ₹{exit_price:.2f} (vs Entry: ₹{position.entry_price:.2f})")
                self.logger.info(f"   Net P&L: ₹{net_pnl:,.2f} (after brokerage)")
                self.logger.info(f"   ROI: {final_roi:.2f}%")
                self.logger.info(f"   Margin Released: ₹{position.investment_amount:,.2f}")

                # Create portfolio snapshot
                self._create_portfolio_snapshot("SELL_TRADE_EXIT", trade_id)

                return True

        except Exception as e:
            self.logger.error(f"❌ Failed to exit SELL trade: {str(e)}")
            return False

    def _save_sell_trade_to_db(self, position: TradePosition) -> bool:
        """Save SELL trade position to enhanced database"""
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()

            # Check if we need to add columns for SELL trades
            self._ensure_enhanced_database_schema(cursor)

            cursor.execute("""
                INSERT INTO active_trades (
                    trade_id, signal_type, option_type, strike_price, lots, quantity,
                    entry_price, target_price, stop_loss_price, entry_time, expiry_date,
                    investment_amount, premium_received, signal_strength, cumulative_trend, confidence,
                    market_regime, volatility_percentile, range_bounds,
                    current_price, current_pnl, roi_percent, max_profit_achieved,
                    max_drawdown_faced, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                position.trade_id, position.signal_type, position.option_type,
                position.strike_price, position.lots, position.quantity,
                position.entry_price, position.target_price, position.stop_loss_price,
                position.entry_time.isoformat(), position.expiry_date.isoformat(),
                position.investment_amount, position.premium_received, position.signal_strength,
                position.cumulative_trend, position.confidence, position.market_regime,
                position.volatility_percentile, str(position.range_bounds),
                position.current_price, position.current_pnl, position.roi_percent,
                position.max_profit_achieved, position.max_drawdown_faced, position.status
            ))

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to save SELL trade to database: {str(e)}")
            return False

    def _ensure_enhanced_database_schema(self, cursor):
        """Ensure database has columns for SELL strategy"""
        try:
            # Add new columns for SELL trades if they don't exist
            enhanced_columns = [
                "ALTER TABLE active_trades ADD COLUMN premium_received REAL DEFAULT 0",
                "ALTER TABLE active_trades ADD COLUMN market_regime TEXT DEFAULT 'UNKNOWN'",
                "ALTER TABLE active_trades ADD COLUMN volatility_percentile REAL DEFAULT 50",
                "ALTER TABLE active_trades ADD COLUMN range_bounds TEXT DEFAULT '{}'"
            ]

            for sql in enhanced_columns:
                try:
                    cursor.execute(sql)
                except sqlite3.OperationalError:
                    # Column already exists
                    pass

        except Exception as e:
            self.logger.warning(f"⚠️  Database schema enhancement failed: {e}")

    def _update_position_in_db(self, position: TradePosition):
        """Update position data in database"""
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE active_trades SET
                    current_price = ?, current_pnl = ?, roi_percent = ?,
                    max_profit_achieved = ?, max_drawdown_faced = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE trade_id = ?
            """, (
                position.current_price, position.current_pnl, position.roi_percent,
                position.max_profit_achieved, position.max_drawdown_faced,
                position.trade_id
            ))

            conn.commit()
            conn.close()

        except Exception as e:
            self.logger.error(f"❌ Failed to update position in database: {str(e)}")

    def _save_to_trade_history(self, position: TradePosition, exit_time: datetime,
                               exit_reason: str, exit_price: float, net_pnl: float) -> bool:
        """Save completed trade to history table"""
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO trade_history (
                    trade_id, signal_type, signal_strength, entry_time, exit_time,
                    entry_price, exit_price, lots, investment_amount, realized_pnl,
                    roi_percentage, exit_reason, trading_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                position.trade_id, position.signal_type, position.signal_strength,
                position.entry_time.isoformat(), exit_time.isoformat(),
                position.entry_price, exit_price, position.lots,
                position.investment_amount, net_pnl, position.roi_percent,
                exit_reason, date.today().isoformat()
            ))

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to save to trade history: {str(e)}")
            return False

    def _remove_from_active_trades(self, trade_id: str):
        """Remove trade from active trades table"""
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM active_trades WHERE trade_id = ?", (trade_id,))
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"❌ Failed to remove from active trades: {str(e)}")

    def _create_portfolio_snapshot(self, trigger_event: str, related_trade_id: str = None):
        """Create a portfolio snapshot"""
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()

            # Calculate unrealized P&L
            unrealized_pnl = sum(pos.current_pnl for pos in self.active_positions.values())

            cursor.execute("""
                INSERT INTO portfolio_snapshots (
                    timestamp, current_capital, available_capital, capital_at_risk,
                    active_positions, total_pnl, unrealized_pnl, realized_pnl,
                    trigger_event
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(), self.current_capital, self.available_capital,
                self.capital_at_risk + self.margin_utilized, len(self.active_positions),
                self.total_pnl, unrealized_pnl, self.daily_pnl, trigger_event
            ))

            conn.commit()
            conn.close()

        except Exception as e:
            self.logger.error(f"❌ Failed to create portfolio snapshot: {str(e)}")

    def get_enhanced_portfolio_summary(self) -> Dict:
        """Get enhanced portfolio summary including SELL trades"""
        unrealized_pnl = sum(pos.current_pnl for pos in self.active_positions.values())
        total_pnl = self.daily_pnl + unrealized_pnl

        # Separate BUY and SELL positions
        buy_positions = [pos for pos in self.active_positions.values() if not pos.signal_type.startswith('SELL_')]
        sell_positions = [pos for pos in self.active_positions.values() if pos.signal_type.startswith('SELL_')]

        return {
            'current_capital': self.current_capital,
            'available_capital': self.available_capital,
            'capital_at_risk': self.capital_at_risk,
            'margin_utilized': self.margin_utilized,
            'premium_collected': self.premium_collected,
            'daily_pnl': self.daily_pnl,
            'unrealized_pnl': unrealized_pnl,
            'total_pnl': total_pnl,
            'active_positions': len(self.active_positions),
            'buy_positions': len(buy_positions),
            'sell_positions': len(sell_positions),
            'trades_today': self.trades_today,
            'consecutive_losses': self.consecutive_losses,
            'risk_exposure_pct': ((self.capital_at_risk + self.margin_utilized) / self.current_capital) * 100
        }


class EnhancedTradingSystemMain:
    """Enhanced trading system main controller supporting both BUY and SELL strategies"""

    def __init__(self):
        self.config = EnhancedTradingSystemConfig()

        # Use enhanced logger from main.py
        from main import TradingSystemLogger, DatabaseManager
        self.logger = TradingSystemLogger(self.config)
        self.db = DatabaseManager(self.config, self.logger)

        # Enhanced components
        self.data_provider = RealTimeDataProvider()
        self.data_provider.set_logger(self.logger)

        self.market_regime_detector = MarketRegimeDetector(self.data_provider, self.logger)
        self.dashboard_parser = DashboardParser(self.config, self.logger)
        self.signal_analyzer = EnhancedSignalAnalyzer(self.config, self.logger, self.market_regime_detector)
        self.portfolio_manager = EnhancedPortfolioManager(self.config, self.db, self.logger)

        # Risk manager from main.py
        from main import RiskManager
        self.risk_manager = RiskManager(self.config, self.portfolio_manager, self.logger)

        # System state
        self.is_running = False
        self.trading_enabled = True
        self.last_dashboard_check = None
        self.last_dashboard_modified = None
        self.signals_processed_today = 0
        self.last_price_update = datetime.now()
        self.price_update_interval = 30

        # Threading for real-time price updates
        self.price_update_thread = None
        self.real_data_thread = None

        self.logger.info("=" * 70)
        self.logger.info("🚀 ENHANCED GAMMA TRADING SYSTEM V4.0 - SELL STRATEGY MODE")
        self.logger.info("=" * 70)
        self.logger.info(f"Starting Capital: ₹{self.config.STARTING_CAPITAL:,}")
        self.logger.info(f"Enhanced Database: {self.config.DB_PATH}")
        self.logger.info(f"Real Data Source: C:\\Projects\\apps\\_nifty_optionanalyser\\OptionAnalyser.db")
        self.logger.info("📊 Market Regime Detection: ENABLED")
        self.logger.info("💰 SELL Strategy Support: ENABLED")
        self.logger.info("📈 Range-bound Market Detection: ENABLED")
        self.logger.info("🎯 OTM Strike Selection: ENHANCED")

    def startup_checks(self):
        """Enhanced startup validation checks"""
        self.logger.info("🔍 Performing enhanced startup checks...")

        checks_passed = True

        # Database connectivity
        try:
            conn = self.db.get_connection()
            conn.close()
            self.logger.info("✅ Enhanced database connection: OK")
        except Exception as e:
            self.logger.error(f"❌ Enhanced database connection failed: {e}")
            checks_passed = False

        # Market regime detector
        try:
            test_regime = self.market_regime_detector.detect_market_regime()
            if test_regime:
                self.logger.info("✅ Market regime detection: OK")
            else:
                self.logger.warning("⚠️  Market regime detection issues")
        except Exception as e:
            self.logger.error(f"❌ Market regime detection failed: {e}")
            checks_passed = False

        # Enhanced signal analyzer
        try:
            # Test enhanced signal analysis
            test_data = {
                'cumulative_trend': 25000,
                'confidence': 80,
                'combined_flow': 5.0,
                'timestamp': datetime.now().strftime('%d-%m-%Y %H:%M')
            }
            test_signal = self.signal_analyzer.analyze_signal(test_data)
            if test_signal:
                self.logger.info("✅ Enhanced signal analysis: OK")
            else:
                self.logger.warning("⚠️  Enhanced signal analysis issues")
        except Exception as e:
            self.logger.error(f"❌ Enhanced signal analysis failed: {e}")

        # Enhanced portfolio manager
        try:
            portfolio_summary = self.portfolio_manager.get_enhanced_portfolio_summary()
            self.logger.info(f"✅ Enhanced Portfolio Summary:")
            self.logger.info(f"   Active positions: {portfolio_summary['active_positions']}")
            self.logger.info(f"   BUY positions: {portfolio_summary['buy_positions']}")
            self.logger.info(f"   SELL positions: {portfolio_summary['sell_positions']}")
            self.logger.info(f"   Available capital: ₹{portfolio_summary['available_capital']:,.2f}")
            self.logger.info(f"   Margin utilized: ₹{portfolio_summary['margin_utilized']:,.2f}")
            self.logger.info(f"   Premium collected: ₹{portfolio_summary['premium_collected']:,.2f}")
        except Exception as e:
            self.logger.error(f"❌ Enhanced portfolio check failed: {e}")
            checks_passed = False

        # Market timing check
        if self.config.is_market_hours():
            self.logger.info("✅ Market hours: OPEN")
        else:
            self.logger.warning("⚠️  Market hours: CLOSED")

        # SELL strategy configuration
        try:
            self.logger.info("✅ SELL Strategy Configuration:")
            self.logger.info(f"   Max margin utilization: {(self.config.STARTING_CAPITAL * 0.6):,.0f}")
            self.logger.info(f"   SELL position sizes: {self.config.SELL_POSITION_SIZES}")
            self.logger.info(f"   Range detection enabled: TRUE")
            self.logger.info(f"   Volatility analysis enabled: TRUE")
        except Exception as e:
            self.logger.error(f"❌ SELL strategy configuration check failed: {e}")

        if checks_passed:
            self.logger.info("🎯 All enhanced startup checks passed!")
            return True
        else:
            self.logger.error("❌ Enhanced startup checks failed!")
            return False

    def check_for_new_dashboard_data(self) -> Optional[Dict]:
        """Enhanced dashboard data processing with market regime analysis"""
        try:
            latest_dashboard = self.config.get_latest_dashboard_file()
            if not latest_dashboard:
                return None

            current_modified = latest_dashboard.stat().st_mtime

            # Check if dashboard file has been updated
            if self.last_dashboard_modified is None or current_modified > self.last_dashboard_modified:
                self.logger.info(f"📊 New enhanced dashboard data detected: {latest_dashboard.name}")
                self.last_dashboard_modified = current_modified

                # Parse dashboard
                dashboard_data = self.dashboard_parser.parse_dashboard_file(latest_dashboard)
                if dashboard_data:
                    # Enhanced signal analysis with market regime
                    signal = self.signal_analyzer.analyze_signal(dashboard_data)
                    if signal:
                        # Save enhanced signal to database
                        signal_id = self.db.save_signal(signal)
                        if signal_id:
                            self.signals_processed_today += 1
                            self.logger.info(f"💾 Enhanced signal saved: {signal_id}")

                        # Execute trade based on strategy type
                        if signal['should_trade'] and self.trading_enabled:
                            position = self.portfolio_manager.execute_trade(signal)
                            if position:
                                # Update signal record with trade execution
                                self.db.save_signal(signal, trade_executed=True, trade_id=position.trade_id)

                                strategy_type = signal.get('strategy_type', 'BUY_OPTIONS')
                                if strategy_type == 'SELL_PREMIUM':
                                    self.logger.info(f"💰 SELL trade executed successfully: {position.trade_id}")
                                else:
                                    self.logger.info(f"🎯 BUY trade executed successfully: {position.trade_id}")
                            else:
                                self.logger.warning("⚠️  Enhanced trade execution failed")

                        return signal

        except Exception as e:
            self.logger.error(f"❌ Enhanced dashboard check failed: {str(e)}")

        return None

    def display_enhanced_status(self):
        """Display comprehensive enhanced system status"""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("📊 ENHANCED SYSTEM STATUS (SELL STRATEGY ENABLED)")
        self.logger.info("=" * 70)

        # System status
        status_emoji = "🟢" if self.is_running else "🔴"
        trading_emoji = "🟢" if self.trading_enabled else "🔴"

        self.logger.info(f"System Status: {status_emoji} {'RUNNING' if self.is_running else 'STOPPED'}")
        self.logger.info(f"Trading Status: {trading_emoji} {'ENABLED' if self.trading_enabled else 'DISABLED'}")

        # Enhanced portfolio summary
        portfolio_summary = self.portfolio_manager.get_enhanced_portfolio_summary()
        self.logger.info(f"💰 Current Capital: ₹{portfolio_summary['current_capital']:,.2f}")
        self.logger.info(f"💸 Available Capital: ₹{portfolio_summary['available_capital']:,.2f}")
        self.logger.info(f"⚖️  Capital at Risk (BUY): ₹{portfolio_summary['capital_at_risk']:,.2f}")
        self.logger.info(f"🏦 Margin Utilized (SELL): ₹{portfolio_summary['margin_utilized']:,.2f}")
        self.logger.info(f"💵 Premium Collected: ₹{portfolio_summary['premium_collected']:,.2f}")
        self.logger.info(f"📈 Daily P&L: ₹{portfolio_summary['daily_pnl']:+,.2f}")
        self.logger.info(f"💼 Total Active Positions: {portfolio_summary['active_positions']}")
        self.logger.info(
            f"📊 BUY Positions: {portfolio_summary['buy_positions']} | SELL Positions: {portfolio_summary['sell_positions']}")
        self.logger.info(f"📊 Trades Today: {portfolio_summary['trades_today']}")

        # Risk metrics
        if portfolio_summary['consecutive_losses'] > 0:
            self.logger.info(f"📉 Consecutive Losses: {portfolio_summary['consecutive_losses']}")

        risk_pct = portfolio_summary['risk_exposure_pct']
        risk_emoji = "🟢" if risk_pct < 15 else "🟡" if risk_pct < 30 else "🔴"
        self.logger.info(f"{risk_emoji} Total Risk Exposure: {risk_pct:.1f}%")

        # Market regime info
        try:
            current_regime = self.market_regime_detector.detect_market_regime()
            self.logger.info(f"🏛️  Market Regime: {current_regime['primary_regime']}")
            self.logger.info(
                f"📊 Volatility: {current_regime['volatility_percentile']:.0f}%ile ({current_regime['volatility_regime']})")
            self.logger.info(f"🎯 Recommended Strategy: {current_regime['recommended_strategy']}")
        except Exception as e:
            self.logger.warning(f"⚠️  Market regime detection error: {e}")

        # Active positions detail
        if self.portfolio_manager.active_positions:
            self.logger.info("\n📋 ACTIVE POSITIONS:")
            for trade_id, position in self.portfolio_manager.active_positions.items():
                pnl_emoji = "🟢" if position.current_pnl >= 0 else "🔴"
                strategy_emoji = "💰" if position.signal_type.startswith('SELL_') else "🎯"

                self.logger.info(f"   {strategy_emoji} {pnl_emoji} {trade_id}: {position.signal_type} | "
                                 f"₹{position.current_price:.2f} | "
                                 f"P&L: ₹{position.current_pnl:+,.2f} ({position.roi_percent:+.1f}%)")

                if position.signal_type.startswith('SELL_'):
                    self.logger.info(f"      Premium Received: ₹{position.premium_received:,.2f} | "
                                     f"Margin: ₹{position.investment_amount:,.2f}")

        # Strategy thresholds
        self.logger.info("\n🎯 ENHANCED STRATEGY THRESHOLDS:")
        self.logger.info("  BUY Strategy (Trending Markets):")
        self.logger.info(
            f"    SUPER_EXTREME: ±{self.config.TREND_THRESHOLDS['SUPER_EXTREME']:,} ({self.config.LOTS_PER_SIGNAL['SUPER_EXTREME']} lots)")
        self.logger.info(
            f"    EXTREME: ±{self.config.TREND_THRESHOLDS['EXTREME']:,} ({self.config.LOTS_PER_SIGNAL['EXTREME']} lots)")
        self.logger.info("  SELL Strategy (Range-bound/Low Vol):")
        self.logger.info(f"    Range Detection: <2.0% price range")
        self.logger.info(f"    Volatility Threshold: 15-35%ile")
        self.logger.info(f"    Max SELL Positions: 2")
        self.logger.info(f"    Max Margin Utilization: 60%")

        self.logger.info("=" * 70)

    def start_enhanced_background_tasks(self):
        """Start enhanced background tasks for price updates"""

        def enhanced_price_update_worker():
            while self.is_running:
                try:
                    # Update all positions (both BUY and SELL)
                    self.portfolio_manager.update_position_prices()
                    time.sleep(self.price_update_interval)
                except Exception as e:
                    self.logger.error(f"❌ Enhanced background price update error: {str(e)}")
                    time.sleep(60)

        if self.price_update_thread is None or not self.price_update_thread.is_alive():
            self.price_update_thread = threading.Thread(target=enhanced_price_update_worker, daemon=True)
            self.price_update_thread.start()
            self.logger.info("🔄 Enhanced background price updates started")

    def start_enhanced_real_data_updates(self):
        """Start enhanced real-time price updates"""

        def enhanced_real_price_update_worker():
            while self.is_running:
                try:
                    if self.portfolio_manager.active_positions:
                        self.logger.info("🔄 Updating positions with enhanced real database prices...")

                        for trade_id in list(self.portfolio_manager.active_positions.keys()):
                            position = self.portfolio_manager.active_positions[trade_id]

                            if position.signal_type.startswith('SELL_'):
                                self.portfolio_manager.update_sell_position_prices(trade_id)
                            else:
                                self.portfolio_manager.update_buy_position_prices(trade_id)

                        self.logger.info(f"✅ Updated {len(self.portfolio_manager.active_positions)} enhanced positions")

                    # Wait 5 minutes (matches your data collection frequency)
                    time.sleep(300)

                except Exception as e:
                    self.logger.error(f"❌ Enhanced real price update error: {str(e)}")
                    time.sleep(60)

        if self.real_data_thread is None or not self.real_data_thread.is_alive():
            self.real_data_thread = threading.Thread(target=enhanced_real_price_update_worker, daemon=True)
            self.real_data_thread.start()
            self.logger.info("🟢 Enhanced real-time price updates started (5-minute intervals)")

    def main_loop(self):
        """Enhanced main trading loop with SELL strategy support"""
        self.logger.info("🔄 Starting enhanced main trading loop with SELL strategy...")
        self.is_running = True

        # Start enhanced background tasks
        self.start_enhanced_background_tasks()
        self.start_enhanced_real_data_updates()

        try:
            loop_count = 0
            while self.is_running:
                current_time = datetime.now()
                loop_count += 1

                self.logger.info(
                    f"⏰ Enhanced Loop #{loop_count}: {current_time.strftime('%H:%M:%S')} [SELL STRATEGY MODE]")

                # Update last check time
                self.last_dashboard_check = current_time.strftime('%H:%M:%S')

                # Check for new signals with enhanced analysis
                signal = self.check_for_new_dashboard_data()
                if signal:
                    strategy_type = signal.get('strategy_type', 'BUY_OPTIONS')
                    if signal['should_trade'] and self.trading_enabled:
                        if strategy_type == 'SELL_PREMIUM':
                            self.logger.info("🔥 SELL PREMIUM SIGNAL PROCESSED WITH REAL PRICES!")
                            self.logger.info(
                                f"   Market Regime: {signal.get('market_regime', {}).get('primary_regime', 'UNKNOWN')}")
                            self.logger.info(f"   SELL Reasoning: {signal.get('sell_reasoning', 'N/A')}")
                        else:
                            self.logger.info("🔥 BUY SIGNAL PROCESSED WITH REAL PRICES!")
                    else:
                        reason = signal.get('skip_reason', 'Trading disabled')
                        self.logger.info(f"⏭️  Enhanced signal skipped: {reason}")

                # Run enhanced risk checks every 5 minutes
                if loop_count % 10 == 0:
                    self.run_enhanced_risk_checks()

                # Display enhanced status every 10 minutes
                if loop_count % 20 == 0:
                    self.display_enhanced_status()
                    self._display_enhanced_real_data_status()

                # Sleep for 30 seconds between checks
                time.sleep(30)

        except KeyboardInterrupt:
            self.logger.info("\n🛑 Keyboard interrupt received")
            self.shutdown()
        except Exception as e:
            self.logger.error(f"❌ Enhanced main loop error: {str(e)}")
            self.shutdown()

    def run_enhanced_risk_checks(self):
        """Run enhanced risk checks for both BUY and SELL positions"""
        try:
            risk_alerts = self.risk_manager.check_risk_conditions()

            # Enhanced risk checks for SELL positions
            sell_specific_alerts = self._check_sell_specific_risks()
            risk_alerts.extend(sell_specific_alerts)

            for alert in risk_alerts:
                # Save risk event to database
                self.db.save_risk_event(
                    alert['type'], alert['level'], alert['message'],
                    alert.get('action'), alert.get('trade_id')
                )

                # Execute enhanced risk response
                self._execute_enhanced_risk_response(alert)

                # Disable trading for critical alerts
                if alert['level'] == 'CRITICAL':
                    self.trading_enabled = False
                    self.logger.critical(f"🚨 ENHANCED TRADING DISABLED: {alert['message']}")

        except Exception as e:
            self.logger.error(f"❌ Enhanced risk check failed: {str(e)}")

    def _check_sell_specific_risks(self) -> List[Dict]:
        """Check SELL-specific risk conditions"""
        alerts = []

        try:
            portfolio_summary = self.portfolio_manager.get_enhanced_portfolio_summary()

            # Margin utilization check
            margin_utilization_pct = (self.portfolio_manager.margin_utilized / self.config.STARTING_CAPITAL) * 100
            if margin_utilization_pct > 50:  # 50% margin utilization warning
                alerts.append({
                    'level': 'HIGH',
                    'type': 'MARGIN_UTILIZATION',
                    'message': f"High margin utilization: {margin_utilization_pct:.1f}%",
                    'action': 'REDUCE_SELL_POSITIONS'
                })

            # SELL positions concentration check
            if portfolio_summary['sell_positions'] > 2:
                alerts.append({
                    'level': 'MEDIUM',
                    'type': 'SELL_CONCENTRATION',
                    'message': f"Too many SELL positions: {portfolio_summary['sell_positions']}",
                    'action': 'LIMIT_NEW_SELLS'
                })

            # Volatility expansion check for SELL positions
            for trade_id, position in self.portfolio_manager.active_positions.items():
                if position.signal_type.startswith('SELL_'):
                    current_vol = self.portfolio_manager._get_current_volatility_percentile()
                    if hasattr(position, 'volatility_percentile') and current_vol > position.volatility_percentile + 25:
                        alerts.append({
                            'level': 'HIGH',
                            'type': 'VOLATILITY_EXPANSION',
                            'message': f"Volatility expansion detected for SELL position {trade_id}",
                            'action': 'CLOSE_SELL_POSITION',
                            'trade_id': trade_id
                        })

        except Exception as e:
            self.logger.error(f"❌ SELL-specific risk check failed: {str(e)}")

        return alerts

    def _execute_enhanced_risk_response(self, alert: Dict):
        """Execute enhanced risk response including SELL-specific actions"""
        try:
            action = alert.get('action')

            if action == 'REDUCE_SELL_POSITIONS':
                self.logger.warning(f"📉 Reducing SELL position exposure: {alert['message']}")
                # Would implement SELL position reduction logic here

            elif action == 'CLOSE_SELL_POSITION':
                trade_id = alert.get('trade_id')
                if trade_id and trade_id in self.portfolio_manager.active_positions:
                    position = self.portfolio_manager.active_positions[trade_id]
                    self.logger.warning(f"🚪 Force closing SELL position {trade_id}: {alert['message']}")
                    self.portfolio_manager.exit_sell_trade(trade_id, "RISK_MANAGEMENT", position.current_price)

            elif action == 'LIMIT_NEW_SELLS':
                self.logger.warning(f"⏸️  Limiting new SELL trades: {alert['message']}")
                # Would implement SELL trade limiting logic here

            else:
                # Use original risk response for standard actions
                self.risk_manager.execute_risk_response(alert)

        except Exception as e:
            self.logger.error(f"❌ Enhanced risk response execution failed: {str(e)}")

    def _display_enhanced_real_data_status(self):
        """Display enhanced real-time data status"""
        try:
            spot_price = self.data_provider.get_current_spot_price()

            self.logger.info("\n" + "=" * 50)
            self.logger.info("📊 ENHANCED REAL-TIME DATA STATUS")
            self.logger.info("=" * 50)
            self.logger.info(f"📈 Current Nifty Spot: ₹{spot_price:.2f}")

            if self.portfolio_manager.active_positions:
                self.logger.info("💰 Current Option Prices:")
                for trade_id, position in self.portfolio_manager.active_positions.items():
                    real_price = self.data_provider.get_option_price(position.strike_price, position.option_type)
                    strategy_type = "SELL" if position.signal_type.startswith('SELL_') else "BUY"
                    self.logger.info(
                        f"   {strategy_type} {position.option_type} {position.strike_price}: ₹{real_price:.2f}")

            # Market regime status
            try:
                current_regime = self.market_regime_detector.detect_market_regime()
                self.logger.info(f"🏛️  Current Market Regime: {current_regime['primary_regime']}")
                self.logger.info(f"📊 Volatility Regime: {current_regime['volatility_regime']}")
                self.logger.info(f"🎯 Strategy Recommendation: {current_regime['recommended_strategy']}")
            except Exception as e:
                self.logger.warning(f"⚠️  Market regime detection error: {e}")

            self.logger.info("🕒 Data Source: Real-time option chain database")
            self.logger.info("🔄 Update Frequency: Every 5 minutes")
            self.logger.info("🎯 SELL Strategy: ACTIVE")
            self.logger.info("=" * 50)

        except Exception as e:
            self.logger.error(f"❌ Failed to display enhanced real data status: {e}")

    def shutdown(self):
        """Enhanced graceful system shutdown"""
        self.logger.info("🔄 Shutting down enhanced trading system...")
        self.is_running = False

        # Close positions based on strategy type
        if not self.config.is_market_hours() and self.portfolio_manager.active_positions:
            self.logger.info("🚪 Market closed - reviewing active positions...")

            for trade_id, position in list(self.portfolio_manager.active_positions.items()):
                if position.expiry_date == date.today():
                    if position.signal_type.startswith('SELL_'):
                        self.logger.info(f"🚪 Force closing expired SELL position: {trade_id}")
                        self.portfolio_manager.exit_sell_trade(trade_id, "SYSTEM_SHUTDOWN", position.current_price)
                    else:
                        self.logger.info(f"🚪 Force closing expired BUY position: {trade_id}")
                        self.portfolio_manager.exit_buy_trade(trade_id, "SYSTEM_SHUTDOWN", position.current_price)

        # Final enhanced portfolio summary
        final_summary = self.portfolio_manager.get_enhanced_portfolio_summary()

        # Update system state with enhanced metrics
        self.db.set_system_state('system_status', 'stopped')
        self.db.set_system_state('last_shutdown', datetime.now().isoformat())
        self.db.set_system_state('final_capital', str(final_summary['current_capital']))
        self.db.set_system_state('final_pnl', str(final_summary['daily_pnl']))
        self.db.set_system_state('final_signals_count', str(self.signals_processed_today))
        self.db.set_system_state('final_margin_utilized', str(final_summary['margin_utilized']))
        self.db.set_system_state('final_premium_collected', str(final_summary['premium_collected']))

        # Final enhanced statistics
        self.logger.info("\n" + "=" * 70)
        self.logger.info("📊 FINAL ENHANCED SESSION SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"💰 Final Capital: ₹{final_summary['current_capital']:,.2f}")
        self.logger.info(f"📈 Session P&L: ₹{final_summary['daily_pnl']:+,.2f}")
        session_return = (final_summary['daily_pnl'] / self.config.STARTING_CAPITAL) * 100
        self.logger.info(f"📊 Session Return: {session_return:+.2f}%")
        self.logger.info(f"📊 Signals Processed: {self.signals_processed_today}")
        self.logger.info(f"🎯 Total Trades Executed: {final_summary['trades_today']}")
        self.logger.info(f"💼 Final BUY Positions: {final_summary['buy_positions']}")
        self.logger.info(f"💰 Final SELL Positions: {final_summary['sell_positions']}")
        self.logger.info(f"🏦 Final Margin Utilized: ₹{final_summary['margin_utilized']:,.2f}")
        self.logger.info(f"💵 Total Premium Collected: ₹{final_summary['premium_collected']:,.2f}")

        self.logger.info("✅ Enhanced trading system shutdown complete")
        self.logger.info("🎯 SELL Strategy Performance: Monitor premium collection vs margin utilization")
        self.logger.info("=" * 70)


def main():
    """Main entry point for enhanced trading system with SELL strategy"""
    try:
        # Initialize enhanced trading system
        trading_system = EnhancedTradingSystemMain()

        # Perform enhanced startup checks
        if not trading_system.startup_checks():
            print("\n❌ Enhanced startup checks failed. Please fix issues and try again.")
            print("\nRequired fixes:")
            print("1. pip install beautifulsoup4 numpy")
            print("2. Place dashboard HTML files in project root")
            print("3. Ensure enhanced database is accessible")
            print("4. Check for critical risk conditions")
            print("5. Verify market regime detection")
            return 1

        # Display initial enhanced status
        trading_system.display_enhanced_status()

        # Start enhanced main loop with SELL strategy support
        trading_system.main_loop()

        return 0

    except Exception as e:
        print(f"❌ Critical enhanced system error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)