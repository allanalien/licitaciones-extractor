"""
Optimized LicitaYa API Extractor with Rate Limiting and Smart Strategies.

This extractor implements advanced strategies to maximize data extraction
while respecting API limits and avoiding saturation.
"""

import asyncio
import time
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional, Set
import requests
from dataclasses import dataclass
import json
import hashlib
from enum import Enum
import random

try:
    from src.extractors.base_extractor import BaseExtractor, ExtractionResult
    from src.utils.logger import get_logger
except ImportError:
    import logging
    def get_logger(name):
        return logging.getLogger(name)

    class BaseExtractor:
        def __init__(self, source_name, config):
            self.source_name = source_name
            self.config = config or {}
            self.logger = get_logger(__name__)

    class ExtractionResult:
        def __init__(self, success, records, errors, source, extraction_date, metadata):
            self.success = success
            self.records = records
            self.errors = errors
            self.source = source
            self.extraction_date = extraction_date
            self.metadata = metadata


class Priority(Enum):
    """Priority levels for keyword searches."""
    HIGH = 1
    MEDIUM = 2
    LOW = 3


@dataclass
class KeywordStrategy:
    """Smart keyword search strategy."""
    keyword: str
    priority: Priority
    expected_results: int
    last_run: Optional[datetime]
    success_rate: float
    avg_results: float


@dataclass
class RateLimitState:
    """Rate limiting state tracker."""
    requests_this_minute: int
    minute_start: datetime
    daily_requests: int
    day_start: datetime
    last_request: Optional[datetime]


class OptimizedLicitaYaExtractor(BaseExtractor):
    """
    Optimized LicitaYa API extractor with intelligent rate limiting.

    Key features:
    - Respects 10 requests/minute limit
    - Smart keyword prioritization
    - Dynamic backoff strategies
    - Result caching and deduplication
    - Error recovery and retry logic
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize optimized LicitaYa extractor.

        Args:
            config: Configuration dictionary with API key and strategies
        """
        super().__init__("licita_ya", config)

        # Rate limiting configuration based on API documentation
        self.max_requests_per_minute = 10
        self.max_daily_requests = self.config.get('max_daily_requests', 1000)  # Conservative estimate

        # Rate limiting state
        self.rate_limit_state = RateLimitState(
            requests_this_minute=0,
            minute_start=datetime.utcnow(),
            daily_requests=0,
            day_start=datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0),
            last_request=None
        )

        # Smart keyword strategies
        self.keyword_strategies = self._initialize_keyword_strategies()

        # Deduplication cache
        self.seen_records = set()
        self.session_cache = {}

    def _setup_extractor(self):
        """Setup LicitaYa specific configuration with optimization."""
        self.api_key = self.config.get('api_key')
        self.base_url = self.config.get('base_url', 'https://www.licitaya.com.mx/api/v1')
        self.timeout = self.config.get('timeout', 20)  # Shorter timeout for more requests
        self.retry_attempts = self.config.get('retry_attempts', 2)  # Fewer retries to save quota
        self.retry_delay_base = self.config.get('retry_delay_base', 2)

        if not self.api_key:
            raise ValueError("API key is required for optimized LicitaYa extractor")

        # Setup session with optimized headers
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'User-Agent': 'LicitacionesExtractor/2.0-Optimized',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Connection': 'keep-alive'  # Reuse connections
        })

    def _initialize_keyword_strategies(self) -> List[KeywordStrategy]:
        """
        Initialize smart keyword strategies based on effectiveness.

        Returns:
            List of keyword strategies sorted by priority
        """
        # High-value keywords based on common government procurement
        high_priority_keywords = [
            'software', 'tecnología', 'sistema', 'desarrollo',
            'consultoría', 'servicios', 'mantenimiento'
        ]

        medium_priority_keywords = [
            'adquisición', 'suministro', 'obra', 'construcción',
            'equipos', 'mobiliario', 'vehículos'
        ]

        low_priority_keywords = [
            'limpieza', 'seguridad', 'alimentación', 'papelería',
            'combustible', 'material'
        ]

        strategies = []

        # Create strategies with different priorities
        for keyword in high_priority_keywords:
            strategies.append(KeywordStrategy(
                keyword=keyword,
                priority=Priority.HIGH,
                expected_results=50,
                last_run=None,
                success_rate=1.0,
                avg_results=25.0
            ))

        for keyword in medium_priority_keywords:
            strategies.append(KeywordStrategy(
                keyword=keyword,
                priority=Priority.MEDIUM,
                expected_results=30,
                last_run=None,
                success_rate=1.0,
                avg_results=15.0
            ))

        for keyword in low_priority_keywords:
            strategies.append(KeywordStrategy(
                keyword=keyword,
                priority=Priority.LOW,
                expected_results=20,
                last_run=None,
                success_rate=1.0,
                avg_results=10.0
            ))

        # Sort by priority and expected results
        return sorted(strategies, key=lambda x: (x.priority.value, -x.expected_results))

    def extract_data(self, target_date: date, **kwargs) -> ExtractionResult:
        """
        Optimized data extraction with intelligent strategies.

        Args:
            target_date: Date to extract data for
            **kwargs: Additional parameters

        Returns:
            ExtractionResult with optimized extraction
        """
        self.logger.info(f"Starting optimized LicitaYa extraction for {target_date}")

        # Reset daily counters if new day
        self._check_and_reset_daily_counters()

        # Check if we have remaining quota
        remaining_quota = self._calculate_remaining_quota()
        self.logger.info(f"Remaining daily quota: {remaining_quota} requests")

        if remaining_quota < 5:
            return ExtractionResult(
                success=False,
                records=[],
                errors=["Daily API quota nearly exhausted"],
                source=self.source_name,
                extraction_date=datetime.utcnow(),
                metadata={"quota_exhausted": True}
            )

        all_records = []
        all_errors = []
        extraction_stats = {
            "strategies_used": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "cached_results": 0,
            "deduplicated_records": 0
        }

        try:
            # Get optimized keyword list based on remaining quota
            selected_strategies = self._select_optimal_strategies(remaining_quota, target_date)

            self.logger.info(f"Selected {len(selected_strategies)} keyword strategies for extraction")

            for strategy in selected_strategies:
                try:
                    # Check rate limits before each request
                    if not self._can_make_request():
                        wait_time = self._calculate_wait_time()
                        self.logger.info(f"Rate limit reached, waiting {wait_time:.1f} seconds")
                        time.sleep(wait_time)

                    # Extract data for this keyword
                    keyword_records = self._extract_by_keyword_optimized(target_date, strategy)

                    if keyword_records:
                        # Deduplicate records
                        new_records = self._deduplicate_records(keyword_records)
                        all_records.extend(new_records)

                        # Update strategy statistics
                        self._update_strategy_stats(strategy, len(new_records), True)

                        extraction_stats["successful_requests"] += 1
                        extraction_stats["deduplicated_records"] += len(keyword_records) - len(new_records)

                    extraction_stats["strategies_used"] += 1

                    # Smart delay between requests
                    self._smart_delay()

                except Exception as e:
                    error_msg = f"Error with keyword '{strategy.keyword}': {str(e)}"
                    self.logger.warning(error_msg)
                    all_errors.append(error_msg)

                    self._update_strategy_stats(strategy, 0, False)
                    extraction_stats["failed_requests"] += 1

                    # Check if we should stop due to persistent errors
                    if self._should_stop_extraction(extraction_stats):
                        self.logger.warning("Stopping extraction due to persistent errors")
                        break

            # Final statistics
            self.logger.info(f"Optimized extraction complete: {len(all_records)} unique records from {extraction_stats['strategies_used']} strategies")

            return ExtractionResult(
                success=True,
                records=all_records,
                errors=all_errors,
                source=self.source_name,
                extraction_date=datetime.utcnow(),
                metadata={
                    "target_date": target_date.isoformat(),
                    "extraction_stats": extraction_stats,
                    "rate_limit_info": {
                        "requests_used": self.rate_limit_state.daily_requests,
                        "quota_remaining": remaining_quota
                    },
                    "optimization_enabled": True
                }
            )

        except Exception as e:
            error_msg = f"Critical error in optimized extraction: {str(e)}"
            self.logger.error(error_msg)
            return ExtractionResult(
                success=False,
                records=all_records,
                errors=[error_msg] + all_errors,
                source=self.source_name,
                extraction_date=datetime.utcnow(),
                metadata={"target_date": target_date.isoformat()}
            )

    def _select_optimal_strategies(self, remaining_quota: int, target_date: date) -> List[KeywordStrategy]:
        """
        Select optimal keyword strategies based on quota and effectiveness.

        Args:
            remaining_quota: Remaining API requests for the day
            target_date: Target extraction date

        Returns:
            List of selected strategies
        """
        # Prioritize strategies based on:
        # 1. Success rate
        # 2. Average results
        # 3. Time since last run
        # 4. Priority level

        scored_strategies = []

        for strategy in self.keyword_strategies:
            score = 0.0

            # Success rate weight (40%)
            score += strategy.success_rate * 0.4

            # Priority weight (30%)
            priority_score = {
                Priority.HIGH: 1.0,
                Priority.MEDIUM: 0.7,
                Priority.LOW: 0.4
            }[strategy.priority]
            score += priority_score * 0.3

            # Time since last run weight (20%)
            if strategy.last_run:
                hours_since = (datetime.utcnow() - strategy.last_run).total_seconds() / 3600
                time_score = min(1.0, hours_since / 24)  # Full score after 24 hours
            else:
                time_score = 1.0  # Never run gets full score
            score += time_score * 0.2

            # Expected results efficiency (10%)
            efficiency_score = min(1.0, strategy.avg_results / 50)  # Normalized to 50 max
            score += efficiency_score * 0.1

            scored_strategies.append((score, strategy))

        # Sort by score (highest first)
        scored_strategies.sort(key=lambda x: x[0], reverse=True)

        # Select top strategies within quota
        selected = []
        quota_used = 0

        for score, strategy in scored_strategies:
            if quota_used >= remaining_quota * 0.8:  # Use 80% of quota to leave buffer
                break

            selected.append(strategy)
            quota_used += 1  # Each strategy uses at least 1 request

            # High priority strategies can use more quota
            if strategy.priority == Priority.HIGH and quota_used < remaining_quota * 0.6:
                quota_used += 1  # Allow pagination for high priority

        self.logger.info(f"Selected {len(selected)} strategies, estimated quota usage: {quota_used}")
        return selected

    def _extract_by_keyword_optimized(self, target_date: date, strategy: KeywordStrategy) -> List[Dict[str, Any]]:
        """
        Extract data for a specific keyword with optimization.

        Args:
            target_date: Target date
            strategy: Keyword strategy

        Returns:
            List of extracted records
        """
        # Check cache first
        cache_key = f"{strategy.keyword}_{target_date.isoformat()}"
        if cache_key in self.session_cache:
            self.logger.debug(f"Using cached results for {strategy.keyword}")
            return self.session_cache[cache_key]

        url = f"{self.base_url}/tender/search"
        date_str = target_date.strftime('%Y%m%d')

        params = {
            'q': strategy.keyword,
            'date': date_str
        }

        records = []

        try:
            # Track request
            self._track_request()

            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout
            )

            if response.status_code == 200:
                data = response.json()

                # Handle different response structures
                if isinstance(data, list):
                    records = data
                elif isinstance(data, dict):
                    records = data.get('data', data.get('results', data.get('tenders', [])))

                # Add search metadata to records
                for record in records:
                    record['search_keyword'] = strategy.keyword
                    record['search_date'] = date_str
                    record['extraction_timestamp'] = datetime.utcnow().isoformat()

                # Cache results
                self.session_cache[cache_key] = records

                self.logger.debug(f"Extracted {len(records)} records for keyword '{strategy.keyword}'")

            elif response.status_code == 401:
                raise Exception("API key authentication failed")
            elif response.status_code == 429:
                raise Exception("Rate limit exceeded - need to backoff")
            else:
                raise Exception(f"API request failed with status {response.status_code}")

        except requests.exceptions.Timeout:
            raise Exception(f"Request timeout for keyword '{strategy.keyword}'")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Network error: {str(e)}")

        return records

    def _can_make_request(self) -> bool:
        """
        Check if we can make a request without violating rate limits.

        Returns:
            True if request can be made
        """
        now = datetime.utcnow()

        # Check minute limit
        if (now - self.rate_limit_state.minute_start).total_seconds() >= 60:
            # Reset minute counter
            self.rate_limit_state.requests_this_minute = 0
            self.rate_limit_state.minute_start = now

        if self.rate_limit_state.requests_this_minute >= self.max_requests_per_minute:
            return False

        # Check daily limit
        if self.rate_limit_state.daily_requests >= self.max_daily_requests:
            return False

        return True

    def _calculate_wait_time(self) -> float:
        """
        Calculate optimal wait time for rate limiting.

        Returns:
            Wait time in seconds
        """
        now = datetime.utcnow()

        # Time until next minute
        minute_wait = 60 - (now - self.rate_limit_state.minute_start).total_seconds()

        # Add small jitter to avoid thundering herd
        jitter = random.uniform(0.1, 0.5)

        return max(minute_wait + jitter, 1.0)

    def _smart_delay(self):
        """
        Implement smart delay between requests to optimize throughput.
        """
        # Base delay to stay under rate limit
        base_delay = 60 / self.max_requests_per_minute + 0.1  # 6.1 seconds for 10 req/min

        # Add adaptive delay based on recent performance
        if hasattr(self, '_recent_response_times'):
            avg_response_time = sum(self._recent_response_times[-5:]) / min(5, len(self._recent_response_times))
            adaptive_delay = avg_response_time * 0.5  # Wait half of average response time
        else:
            adaptive_delay = 0.5

        total_delay = base_delay + adaptive_delay

        # Cap maximum delay
        total_delay = min(total_delay, 10.0)

        self.logger.debug(f"Smart delay: {total_delay:.1f} seconds")
        time.sleep(total_delay)

    def _track_request(self):
        """Track request for rate limiting."""
        now = datetime.utcnow()

        self.rate_limit_state.requests_this_minute += 1
        self.rate_limit_state.daily_requests += 1
        self.rate_limit_state.last_request = now

    def _check_and_reset_daily_counters(self):
        """Reset daily counters if it's a new day."""
        now = datetime.utcnow()
        current_day = now.replace(hour=0, minute=0, second=0, microsecond=0)

        if current_day > self.rate_limit_state.day_start:
            self.rate_limit_state.daily_requests = 0
            self.rate_limit_state.day_start = current_day
            self.logger.info("Reset daily API quota counters")

    def _calculate_remaining_quota(self) -> int:
        """Calculate remaining daily quota."""
        return max(0, self.max_daily_requests - self.rate_limit_state.daily_requests)

    def _deduplicate_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicate records using content hashing.

        Args:
            records: List of records to deduplicate

        Returns:
            List of unique records
        """
        unique_records = []

        for record in records:
            # Create content hash for deduplication
            content_parts = [
                record.get('title', record.get('name', '')),
                record.get('entity', record.get('institution', '')),
                str(record.get('amount', record.get('value', ''))),
                str(record.get('date', record.get('publication_date', '')))
            ]

            content_hash = hashlib.md5('|'.join(content_parts).encode()).hexdigest()

            if content_hash not in self.seen_records:
                self.seen_records.add(content_hash)
                record['content_hash'] = content_hash
                unique_records.append(record)

        return unique_records

    def _update_strategy_stats(self, strategy: KeywordStrategy, results_count: int, success: bool):
        """
        Update strategy statistics for optimization.

        Args:
            strategy: Strategy to update
            results_count: Number of results obtained
            success: Whether the request was successful
        """
        strategy.last_run = datetime.utcnow()

        # Update success rate (exponential moving average)
        alpha = 0.3
        strategy.success_rate = alpha * (1.0 if success else 0.0) + (1 - alpha) * strategy.success_rate

        # Update average results
        if success:
            strategy.avg_results = alpha * results_count + (1 - alpha) * strategy.avg_results

    def _should_stop_extraction(self, stats: Dict[str, int]) -> bool:
        """
        Determine if extraction should stop due to persistent errors.

        Args:
            stats: Current extraction statistics

        Returns:
            True if extraction should stop
        """
        total_attempts = stats['successful_requests'] + stats['failed_requests']

        if total_attempts < 3:
            return False

        failure_rate = stats['failed_requests'] / total_attempts
        return failure_rate > 0.8  # Stop if >80% failure rate

    def get_source_info(self) -> Dict[str, Any]:
        """
        Get information about the optimized LicitaYa extractor.

        Returns:
            Dictionary with source information including optimization details
        """
        base_info = {
            "source_name": self.source_name,
            "api_base_url": self.base_url,
            "timeout": self.timeout,
            "api_type": "private_optimized",
            "optimization_features": [
                "rate_limiting",
                "smart_keyword_selection",
                "result_caching",
                "deduplication",
                "adaptive_delays",
                "error_recovery"
            ],
            "rate_limits": {
                "requests_per_minute": self.max_requests_per_minute,
                "estimated_daily_limit": self.max_daily_requests
            },
            "current_state": {
                "daily_requests_used": self.rate_limit_state.daily_requests,
                "remaining_quota": self._calculate_remaining_quota(),
                "strategies_count": len(self.keyword_strategies)
            }
        }

        return base_info

    def reset_daily_quota(self):
        """Manually reset daily quota for testing."""
        self.rate_limit_state.daily_requests = 0
        self.rate_limit_state.day_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        self.logger.info("Manually reset daily quota")

    def get_optimization_stats(self) -> Dict[str, Any]:
        """
        Get detailed optimization statistics.

        Returns:
            Dictionary with optimization statistics
        """
        # Calculate strategy performance
        strategy_stats = []
        for strategy in self.keyword_strategies:
            strategy_stats.append({
                'keyword': strategy.keyword,
                'priority': strategy.priority.name,
                'success_rate': strategy.success_rate,
                'avg_results': strategy.avg_results,
                'last_run': strategy.last_run.isoformat() if strategy.last_run else None
            })

        return {
            'rate_limit_state': {
                'daily_requests': self.rate_limit_state.daily_requests,
                'requests_this_minute': self.rate_limit_state.requests_this_minute,
                'remaining_quota': self._calculate_remaining_quota()
            },
            'strategy_performance': strategy_stats,
            'cache_size': len(self.session_cache),
            'unique_records_seen': len(self.seen_records)
        }


# Convenience functions for easy usage
def create_optimized_extractor(api_key: str, max_daily_requests: int = 1000) -> OptimizedLicitaYaExtractor:
    """
    Create an optimized LicitaYa extractor with smart defaults.

    Args:
        api_key: LicitaYa API key
        max_daily_requests: Conservative estimate of daily request limit

    Returns:
        Configured optimized extractor
    """
    config = {
        'api_key': api_key,
        'max_daily_requests': max_daily_requests,
        'timeout': 20,
        'retry_attempts': 2
    }

    return OptimizedLicitaYaExtractor(config)