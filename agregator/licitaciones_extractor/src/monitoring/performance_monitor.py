"""
Performance Monitor for optimizing extraction processes
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
import multiprocessing as mp
from functools import wraps

from ..utils.logger import get_logger


@dataclass
class PerformanceMetrics:
    """Data class for performance metrics"""
    operation: str
    start_time: float
    end_time: float
    duration: float
    success: bool
    records_processed: int = 0
    error_message: Optional[str] = None

    @property
    def throughput(self) -> float:
        """Calculate throughput (records per second)"""
        if self.duration > 0 and self.records_processed > 0:
            return self.records_processed / self.duration
        return 0


class PerformanceMonitor:
    """Monitors and optimizes system performance"""

    def __init__(self, enable_parallel: bool = True, max_workers: int = None):
        self.logger = get_logger(self.__class__.__name__)
        self.enable_parallel = enable_parallel
        self.max_workers = max_workers or mp.cpu_count()
        self.metrics_history: List[PerformanceMetrics] = []

    def measure_time(self, operation_name: str = None):
        """Decorator to measure execution time of functions"""
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                op_name = operation_name or func.__name__
                start_time = time.time()
                success = True
                error_message = None
                result = None

                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    success = False
                    error_message = str(e)
                    self.logger.error(f"Error in {op_name}: {e}")
                    raise
                finally:
                    end_time = time.time()
                    duration = end_time - start_time

                    # Try to extract records processed from result
                    records_processed = 0
                    if isinstance(result, dict):
                        records_processed = result.get('records_processed', 0)
                    elif isinstance(result, list):
                        records_processed = len(result)

                    # Record metrics
                    metrics = PerformanceMetrics(
                        operation=op_name,
                        start_time=start_time,
                        end_time=end_time,
                        duration=duration,
                        success=success,
                        records_processed=records_processed,
                        error_message=error_message
                    )
                    self.metrics_history.append(metrics)

                    # Log performance
                    if success:
                        self.logger.info(
                            f"{op_name} completed in {duration:.2f}s "
                            f"(throughput: {metrics.throughput:.2f} records/s)"
                        )
                    else:
                        self.logger.error(f"{op_name} failed after {duration:.2f}s")

                return result
            return wrapper
        return decorator

    def parallel_execute(self, tasks: List[Callable], task_args: List[tuple] = None,
                        use_processes: bool = False) -> List[Any]:
        """Execute tasks in parallel"""
        if not self.enable_parallel:
            # Sequential execution
            results = []
            for i, task in enumerate(tasks):
                args = task_args[i] if task_args else ()
                results.append(task(*args))
            return results

        # Parallel execution
        executor_class = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
        results = []

        with executor_class(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = []
            for i, task in enumerate(tasks):
                args = task_args[i] if task_args else ()
                future = executor.submit(task, *args)
                futures.append((i, future))

            # Collect results maintaining order
            results = [None] * len(tasks)
            for i, future in futures:
                try:
                    results[i] = future.result()
                except Exception as e:
                    self.logger.error(f"Task {i} failed: {e}")
                    results[i] = None

        return results

    async def async_execute(self, coroutines: List) -> List[Any]:
        """Execute coroutines asynchronously"""
        try:
            results = await asyncio.gather(*coroutines, return_exceptions=True)

            # Log any exceptions
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.error(f"Async task {i} failed: {result}")

            return results
        except Exception as e:
            self.logger.error(f"Async execution failed: {e}")
            return []

    def optimize_batch_size(self, total_items: int, processing_time_per_item: float = 0.1) -> int:
        """Calculate optimal batch size based on system resources and processing time"""
        # Consider memory constraints
        available_memory = self._get_available_memory_mb()
        memory_per_item = 1  # Estimated MB per item

        # Calculate based on memory
        max_batch_by_memory = int(available_memory * 0.5 / memory_per_item)

        # Calculate based on processing time (target: process batch in 30 seconds)
        target_batch_time = 30
        max_batch_by_time = int(target_batch_time / processing_time_per_item)

        # Consider parallelization
        max_batch_by_workers = self.max_workers * 10

        # Choose minimum to be safe
        optimal_batch = min(
            max_batch_by_memory,
            max_batch_by_time,
            max_batch_by_workers,
            total_items
        )

        # Ensure minimum batch size
        optimal_batch = max(optimal_batch, 10)

        self.logger.info(f"Optimal batch size calculated: {optimal_batch}")
        return optimal_batch

    def _get_available_memory_mb(self) -> int:
        """Get available system memory in MB"""
        try:
            import psutil
            return int(psutil.virtual_memory().available / (1024 * 1024))
        except:
            # Default to 1GB if can't determine
            return 1024

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get summary of performance metrics"""
        if not self.metrics_history:
            return {}

        # Group metrics by operation
        operations = {}
        for metric in self.metrics_history:
            if metric.operation not in operations:
                operations[metric.operation] = []
            operations[metric.operation].append(metric)

        # Calculate statistics for each operation
        summary = {}
        for op_name, metrics in operations.items():
            durations = [m.duration for m in metrics]
            success_count = sum(1 for m in metrics if m.success)
            total_records = sum(m.records_processed for m in metrics)

            summary[op_name] = {
                "total_executions": len(metrics),
                "success_rate": success_count / len(metrics) * 100,
                "avg_duration": sum(durations) / len(durations),
                "min_duration": min(durations),
                "max_duration": max(durations),
                "total_records_processed": total_records,
                "avg_throughput": total_records / sum(durations) if sum(durations) > 0 else 0
            }

        return summary

    def clear_metrics(self):
        """Clear metrics history"""
        self.metrics_history.clear()
        self.logger.info("Performance metrics cleared")


class ParallelExtractor:
    """Manages parallel extraction from multiple sources"""

    def __init__(self, extractors: List[Any], performance_monitor: PerformanceMonitor):
        self.extractors = extractors
        self.performance_monitor = performance_monitor
        self.logger = get_logger(self.__class__.__name__)

    def extract_parallel(self, date: str) -> Dict[str, List[Dict]]:
        """Extract data from all sources in parallel"""
        self.logger.info(f"Starting parallel extraction for date: {date}")

        # Prepare tasks
        tasks = []
        task_args = []
        source_names = []

        for extractor in self.extractors:
            tasks.append(self._safe_extract)
            task_args.append((extractor, date))
            source_names.append(extractor.__class__.__name__)

        # Execute in parallel
        results = self.performance_monitor.parallel_execute(
            tasks, task_args, use_processes=False
        )

        # Combine results
        combined_results = {}
        for source_name, result in zip(source_names, results):
            if result is not None:
                combined_results[source_name] = result
                self.logger.info(f"{source_name}: Extracted {len(result)} records")
            else:
                self.logger.warning(f"{source_name}: No data extracted")

        return combined_results

    def _safe_extract(self, extractor: Any, date: str) -> Optional[List[Dict]]:
        """Safely extract data from a single source"""
        try:
            return extractor.extract_data(date)
        except Exception as e:
            self.logger.error(f"Extraction failed for {extractor.__class__.__name__}: {e}")
            return None


class BatchProcessor:
    """Optimized batch processing for large datasets"""

    def __init__(self, performance_monitor: PerformanceMonitor):
        self.performance_monitor = performance_monitor
        self.logger = get_logger(self.__class__.__name__)

    def process_in_batches(self, items: List[Any], processor: Callable,
                          batch_size: int = None) -> List[Any]:
        """Process items in optimized batches"""
        if not items:
            return []

        # Determine batch size
        if batch_size is None:
            batch_size = self.performance_monitor.optimize_batch_size(len(items))

        self.logger.info(f"Processing {len(items)} items in batches of {batch_size}")

        results = []
        total_batches = (len(items) + batch_size - 1) // batch_size

        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_num = i // batch_size + 1

            self.logger.info(f"Processing batch {batch_num}/{total_batches}")

            # Process batch with timing
            @self.performance_monitor.measure_time(f"batch_{batch_num}")
            def process_batch():
                return processor(batch)

            batch_results = process_batch()
            if batch_results:
                results.extend(batch_results)

        return results

    async def async_process_in_batches(self, items: List[Any], async_processor: Callable,
                                      batch_size: int = None) -> List[Any]:
        """Process items in batches asynchronously"""
        if not items:
            return []

        # Determine batch size
        if batch_size is None:
            batch_size = self.performance_monitor.optimize_batch_size(len(items))

        self.logger.info(f"Async processing {len(items)} items in batches of {batch_size}")

        results = []
        coroutines = []

        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            coroutines.append(async_processor(batch))

        # Execute all batches concurrently
        batch_results = await self.performance_monitor.async_execute(coroutines)

        # Flatten results
        for batch_result in batch_results:
            if batch_result and not isinstance(batch_result, Exception):
                results.extend(batch_result)

        return results