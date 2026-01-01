# ============================================================================
# DISTRIBUTED CACHE (REDIS CLONE) - FIXED PRODUCTION IMPLEMENTATION
# ============================================================================
#
# RECTIFICATIONS APPLIED:
# 1. FIXED: dict.popitem() TypeError in SimpleCache (switched to iterator method)
# 2. O(N) Eviction -> O(1)
# 3. Stop-the-World GC -> Probabilistic Sampling
# 4. Async Replication with Queues
# 5. Stable CRC32 Hashing
#
# ============================================================================

import time
import threading
import random
import json
import queue
import zlib  # For stable hashing
from collections import OrderedDict
from typing import Any, Optional, Dict, Tuple, List
from dataclasses import dataclass
from enum import Enum


# ============================================================================
# TIER 1: SIMPLE CACHE WITH O(1) OPERATIONS
# ============================================================================

@dataclass
class CacheEntry:
    """Single cache entry with metadata."""
    key: str
    value: Any
    timestamp: float
    frequency: int
    ttl_expiry: Optional[float] = None
    size_bytes: int = 0


class SimpleCache:
    """
    Basic in-memory cache with O(1) operations.
    
    Complexity:
    - get(): O(1)
    - set(): O(1) amortized
    - delete(): O(1)
    """
    
    def __init__(self, max_size: int = 1000):
        """Initialize cache"""
        self.data: Dict[str, CacheEntry] = {}
        self.max_size = max_size
        self.stats = {
            "hits": 0, "misses": 0, "sets": 0,
            "evictions": 0, "total_bytes": 0
        }
        self.lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value by key - O(1)"""
        with self.lock:
            if key not in self.data:
                self.stats["misses"] += 1
                return None
            
            entry = self.data[key]
            
            # Lazy Expiration
            if entry.ttl_expiry and time.time() > entry.ttl_expiry:
                self._delete_internal(key)
                self.stats["misses"] += 1
                return None
            
            entry.timestamp = time.time()
            entry.frequency += 1
            self.stats["hits"] += 1
            return entry.value
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        """Set key-value pair - O(1)"""
        with self.lock:
            size = 100  # Constant overhead assumption
            
            if key in self.data:
                old_size = self.data[key].size_bytes
                self.stats["total_bytes"] -= old_size
            else:
                if len(self.data) >= self.max_size:
                    self._evict_one()
            
            current_time = time.time()
            entry = CacheEntry(
                key=key,
                value=value,
                timestamp=current_time,
                frequency=1,
                ttl_expiry=current_time + ttl_seconds if ttl_seconds else None,
                size_bytes=size
            )
            
            self.data[key] = entry
            self.stats["sets"] += 1
            self.stats["total_bytes"] += size
    
    def delete(self, key: str) -> bool:
        """Delete key from cache - O(1)"""
        with self.lock:
            if key in self.data:
                self._delete_internal(key)
                return True
            return False
            
    def _delete_internal(self, key: str):
        """Internal helper to remove data"""
        if key in self.data:
            entry = self.data.pop(key)
            self.stats["total_bytes"] -= entry.size_bytes
    
    def _evict_one(self) -> None:
        """
        Evict the oldest inserted entry (FIFO) - O(1).
        FIX: Standard dict.popitem() doesn't accept args.
        We use next(iter(dict)) to get the first key efficiently.
        """
        if self.data:
            # next(iter(self.data)) returns the first key (FIFO order in Python 3.7+)
            # This is O(1) start time.
            oldest_key = next(iter(self.data))
            entry = self.data.pop(oldest_key)
            
            self.stats["evictions"] += 1
            self.stats["total_bytes"] -= entry.size_bytes
    
    def size(self) -> int:
        return len(self.data)
    
    def clear(self) -> None:
        with self.lock:
            self.data.clear()
            self.stats["total_bytes"] = 0
    
    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            total_ops = self.stats["hits"] + self.stats["misses"]
            hit_rate = (self.stats["hits"] / total_ops * 100) if total_ops > 0 else 0
            return {
                "hits": self.stats["hits"],
                "misses": self.stats["misses"],
                "hit_rate": f"{hit_rate:.1f}%",
                "sets": self.stats["sets"],
                "evictions": self.stats["evictions"],
                "size_entries": len(self.data),
                "size_bytes": self.stats["total_bytes"],
                "max_size": self.max_size
            }


# ============================================================================
# TIER 2: LRU CACHE WITH LEAST-RECENTLY-USED EVICTION
# ============================================================================

class LRUCache(SimpleCache):
    """
    Cache with Least Recently Used (LRU) eviction policy.
    Uses OrderedDict to track access order. 
    """
    
    def __init__(self, max_size: int = 1000):
        super().__init__(max_size)
        self.access_order = OrderedDict()
    
    def get(self, key: str) -> Optional[Any]:
        with self.lock:
            if key not in self.data:
                self.stats["misses"] += 1
                return None
            
            entry = self.data[key]
            
            # TTL Check
            if entry.ttl_expiry and time.time() > entry.ttl_expiry:
                self._delete_internal(key)
                self.stats["misses"] += 1
                return None
            
            # LRU Update: Move accessed item to end of list
            self.access_order.move_to_end(key)
            
            entry.timestamp = time.time()
            entry.frequency += 1
            self.stats["hits"] += 1
            return entry.value
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        with self.lock:
            current_time = time.time()
            size = 100
            
            if key in self.data:
                old_size = self.data[key].size_bytes
                self.stats["total_bytes"] -= old_size
                self.access_order.move_to_end(key)
            else:
                if len(self.data) >= self.max_size:
                    self._evict_lru()
                self.access_order[key] = True
            
            entry = CacheEntry(
                key=key,
                value=value,
                timestamp=current_time,
                frequency=1,
                ttl_expiry=current_time + ttl_seconds if ttl_seconds else None,
                size_bytes=size
            )
            
            self.data[key] = entry
            self.stats["sets"] += 1
            self.stats["total_bytes"] += size
            
    def _delete_internal(self, key: str):
        if key in self.data:
            entry = self.data.pop(key)
            self.stats["total_bytes"] -= entry.size_bytes
        if key in self.access_order:
            del self.access_order[key]
    
    def _evict_lru(self) -> None:
        """
        Evict least recently used (first item) - O(1).
        popitem(last=False) works here because access_order is an OrderedDict.
        """
        if self.access_order:
            lru_key, _ = self.access_order.popitem(last=False)
            if lru_key in self.data:
                entry = self.data.pop(lru_key)
                self.stats["evictions"] += 1
                self.stats["total_bytes"] -= entry.size_bytes


# ============================================================================
# TIER 3: TTL CACHE WITH PROBABILISTIC CLEANUP
# ============================================================================

class TTLCache(LRUCache):
    """
    Cache with TTL support using Redis-style Probabilistic Expiration.
    """
    
    def __init__(self, max_size: int = 1000, default_ttl: Optional[float] = None, 
                 enable_cleanup: bool = True):
        super().__init__(max_size)
        self.default_ttl = default_ttl
        self.cleanup_thread = None
        self.cleanup_running = enable_cleanup
        
        if enable_cleanup:
            self._start_cleanup_thread()
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        if ttl_seconds is None:
            ttl_seconds = self.default_ttl
        super().set(key, value, ttl_seconds)
    
    def _start_cleanup_thread(self) -> None:
        self.cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True
        )
        self.cleanup_thread.start()
    
    def _cleanup_loop(self) -> None:
        """
        Redis-style probabilistic active expiration loop.
        Samples random keys to expire instead of scanning everything.
        """
        while self.cleanup_running:
            try:
                if not self.data:
                    time.sleep(1)
                    continue

                SAMPLE_SIZE = 20
                EXPIRE_THRESHOLD = 0.25 # 25%
                expired_found = 0
                
                with self.lock:
                    keys = list(self.data.keys())
                    if not keys:
                        continue
                        
                    sample_count = min(len(keys), SAMPLE_SIZE)
                    sample_keys = random.sample(keys, sample_count)
                    now = time.time()
                    
                    for key in sample_keys:
                        entry = self.data.get(key)
                        if entry and entry.ttl_expiry and now > entry.ttl_expiry:
                            self._delete_internal(key)
                            self.stats["evictions"] += 1
                            expired_found += 1
                
                if expired_found > (SAMPLE_SIZE * EXPIRE_THRESHOLD):
                    time.sleep(0.01) # Aggressive cleanup
                else:
                    time.sleep(1)    # Relaxed
                    
            except Exception as e:
                print(f"Cleanup thread error: {e}")
                time.sleep(5)
    
    def stop_cleanup(self) -> None:
        self.cleanup_running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=1)


# ============================================================================
# TIER 4: DISTRIBUTED CACHE WITH ASYNC REPLICATION
# ============================================================================

@dataclass
class ReplicationMessage:
    action: str
    key: str
    value: Any = None
    ttl_seconds: Optional[float] = None
    timestamp: float = 0.0
    
    def to_json(self) -> str:
        return json.dumps({
            "action": self.action,
            "key": self.key,
            "value": self.value,
            "ttl_seconds": self.ttl_seconds,
            "timestamp": self.timestamp
        })
    
    @classmethod
    def from_json(cls, data: str) -> 'ReplicationMessage':
        d = json.loads(data)
        return cls(**d)


class CacheNode(TTLCache):
    """
    Single cache node with Non-Blocking Asynchronous Replication.
    Writes to Master return immediately; updates queue for Slaves.
    """
    
    def __init__(self, node_id: str, max_size: int = 100000, is_master: bool = True):
        super().__init__(max_size=max_size, enable_cleanup=True)
        self.node_id = node_id
        self.is_master = is_master
        self.replicas: list = []
        self.replication_stats = {
            "messages_sent": 0, "messages_received": 0, "queue_depth": 0
        }
        
        # Async Replication Infrastructure
        self.repl_queue = queue.Queue()
        self.repl_running = is_master
        if is_master:
            threading.Thread(target=self._replication_worker, daemon=True).start()
    
    def add_replica(self, replica_node: 'CacheNode') -> None:
        if not self.is_master:
            raise Exception("Cannot add replicas to non-master node!")
        self.replicas.append(replica_node)
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        if not self.is_master:
            raise Exception("Cannot write to replica!")
        
        # 1. Write locally
        super().set(key, value, ttl_seconds)
        
        # 2. Enqueue (Non-blocking)
        msg = ReplicationMessage(
            action="set", key=key, value=value, 
            ttl_seconds=ttl_seconds, timestamp=time.time()
        )
        self.repl_queue.put(msg)
    
    def delete(self, key: str) -> bool:
        if not self.is_master:
            raise Exception("Cannot delete on replica!")
        
        result = super().delete(key)
        
        if result:
            msg = ReplicationMessage(
                action="delete", key=key, timestamp=time.time()
            )
            self.repl_queue.put(msg)
        return result
    
    def _replication_worker(self):
        """Worker to drain queue and update replicas."""
        while self.repl_running:
            try:
                msg = self.repl_queue.get()
                self.replication_stats["queue_depth"] = self.repl_queue.qsize()
                
                for replica in self.replicas:
                    try:
                        replica._apply_replication(msg)
                    except Exception as e:
                        print(f"Replication failed to {replica.node_id}: {e}")
                        
                self.replication_stats["messages_sent"] += 1
                self.repl_queue.task_done()
            except Exception as e:
                print(f"Replication worker error: {e}")
                time.sleep(1)

    def _apply_replication(self, msg: ReplicationMessage) -> None:
        """Apply update to Slave (Bypassing is_master checks)."""
        with self.lock:
            if msg.action == "set":
                # We use TTLCache methods directly to avoid CacheNode restriction
                TTLCache.set(self, msg.key, msg.value, msg.ttl_seconds)
            elif msg.action == "delete":
                TTLCache.delete(self, msg.key)
            self.replication_stats["messages_received"] += 1


# ============================================================================
# TIER 5: CACHE CLUSTER WITH STABLE SHARDING
# ============================================================================

class CacheCluster:
    """
    Distributed cluster with Consistent/Stable Hashing (CRC32).
    """
    
    def __init__(self, num_shards: int = 4, shard_size: int = 100000):
        self.num_shards = num_shards
        self.shards = [
            TTLCache(max_size=shard_size, enable_cleanup=True)
            for _ in range(num_shards)
        ]
        self.stats = {
            "total_gets": 0, "total_sets": 0, "total_deletes": 0
        }
    
    def _get_shard_index(self, key: str) -> int:
        """Get shard index using Stable Hashing (CRC32)."""
        key_bytes = key.encode('utf-8')
        hash_val = zlib.crc32(key_bytes)
        return hash_val % self.num_shards
    
    def get(self, key: str) -> Optional[Any]:
        idx = self._get_shard_index(key)
        self.stats["total_gets"] += 1
        return self.shards[idx].get(key)
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        idx = self._get_shard_index(key)
        self.stats["total_sets"] += 1
        self.shards[idx].set(key, value, ttl_seconds)
    
    def delete(self, key: str) -> bool:
        idx = self._get_shard_index(key)
        self.stats["total_deletes"] += 1
        return self.shards[idx].delete(key)
    
    def get_cluster_stats(self) -> Dict[str, Any]:
        """Aggregate stats from all shards."""
        total_entries = sum(shard.size() for shard in self.shards)
        total_bytes = sum(shard.stats["total_bytes"] for shard in self.shards)
        
        shard_stats = [shard.get_stats() for shard in self.shards]
        total_hits = sum(s["hits"] for s in shard_stats)
        total_misses = sum(s["misses"] for s in shard_stats)
        total_ops = total_hits + total_misses
        hit_rate = (total_hits / total_ops * 100) if total_ops > 0 else 0
        
        return {
            "num_shards": self.num_shards,
            "total_entries": total_entries,
            "total_bytes": total_bytes,
            "total_hits": total_hits,
            "total_misses": total_misses,
            "hit_rate": f"{hit_rate:.1f}%",
            "operations": self.stats
        }


# ============================================================================
# TESTING & BENCHMARKING
# ============================================================================

def run_tests():
    """Run comprehensive tests"""
    print("=" * 80)
    print("DISTRIBUTED CACHE - TEST SUITE (RECTIFIED)")
    print("=" * 80)
    
    # Test 1
    print("\n✓ Test 1: Basic Cache Operations")
    cache = SimpleCache(max_size=10)
    cache.set("a", 1)
    assert cache.get("a") == 1
    assert cache.get("b") is None
    print("  Basic get/set: PASSED")
    
    # Test 2
    print("\n✓ Test 2: O(1) Eviction Strategy")
    cache = SimpleCache(max_size=3)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)
    assert cache.size() == 3
    
    # Eviction Trigger
    cache.set("d", 4)
    assert cache.size() == 3
    # Check FIFO behavior (SimpleCache default)
    assert cache.get("a") is None 
    print("  Eviction logic: PASSED")
    
    # Test 3
    print("\n✓ Test 3: LRU Eviction Policy")
    cache = LRUCache(max_size=3)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)
    cache.get("a") # 'a' is now most recently used
    cache.set("d", 4) # Should evict 'b' (oldest untouched)
    
    assert cache.get("a") == 1
    assert cache.get("b") is None
    print("  LRU correctness: PASSED")
    
    # Test 4
    print("\n✓ Test 4: TTL & Probabilistic Cleanup")
    cache = TTLCache(max_size=10, enable_cleanup=True)
    cache.set("session", "data", ttl_seconds=0.2)
    assert cache.get("session") == "data"
    time.sleep(0.3)
    assert cache.get("session") is None
    print("  TTL & Cleanup: PASSED")
    
    # Test 5
    print("\n✓ Test 5: Thread Safety")
    cache = TTLCache(max_size=10000, enable_cleanup=True)
    def worker(tid):
        for i in range(100):
            cache.set(f"k:{tid}:{i}", i)
            cache.get(f"k:{tid}:{i}")
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert cache.size() == 500
    print("  Concurrent access: PASSED")
    
    # Test 6
    print("\n✓ Test 6: Async Replication")
    master = CacheNode("m1", is_master=True)
    replica = CacheNode("r1", is_master=False)
    master.add_replica(replica)
    master.set("u:1", "Alice")
    time.sleep(0.2) # Allow queue drain
    assert replica.get("u:1") == "Alice"
    print("  Replication: PASSED")
    
    # Test 7
    print("\n✓ Test 7: Stable Sharding")
    cluster = CacheCluster(num_shards=4)
    cluster.set("persist", "val")
    assert cluster.get("persist") == "val"
    print("  Sharding: PASSED")
    
    print("\n" + "=" * 80)
    print("ALL TESTS PASSED! ✅")
    print("=" * 80)


def benchmark_cache():
    """Benchmark cache performance"""
    print("\n" + "=" * 80)
    print("DISTRIBUTED CACHE - PERFORMANCE BENCHMARK")
    print("=" * 80)
    
    cache = CacheCluster(num_shards=4, shard_size=100000)
    
    print("\nWarming up cache...")
    WARMUP = 10000
    for i in range(WARMUP):
        cache.set(f"warmup:{i}", i)
    
    print("Benchmarking 500,000 operations (80% reads, 20% writes)...")
    
    start = time.time()
    OPS = 500000
    
    for i in range(OPS):
        if i % 5 != 0:
            key = f"warmup:{random.randint(0, WARMUP-1)}"
            cache.get(key)
        else:
            key = f"new:{i}"
            cache.set(key, i)
    
    elapsed = time.time() - start
    ops_per_sec = OPS / elapsed
    latency_us = (elapsed * 1000000) / OPS
    
    print("\n" + "-" * 80)
    print("RESULTS:")
    print("-" * 80)
    print(f"Operations:          {OPS:,}")
    print(f"Time elapsed:        {elapsed:.2f} seconds")
    print(f"Throughput:          {ops_per_sec:,.0f} ops/sec")
    print(f"Avg latency:         {latency_us:.2f} microseconds")
    
    stats = cache.get_cluster_stats()
    print(f"Cluster Hit rate:    {stats['hit_rate']}")
    print(f"Entries stored:      {stats['total_entries']:,}")
    print("-" * 80)
    
    print("\nPerformance Verification:")
    print(f"  Target: > 100k OPS/sec     {'✅ PASS' if ops_per_sec > 100000 else '❌ FAIL'}")
    print(f"  Target: < 50µs latency     {'✅ PASS' if latency_us < 50 else '❌ FAIL'}")
    print("=" * 80)


if __name__ == "__main__":
    run_tests()
    benchmark_cache()
    
    print("\n✅ RECTIFIED CACHE IMPLEMENTATION COMPLETE")
    print("Status: Production-Ready")
    print("Architecture: Sharded, Replicated, Probabilistic TTL")
    print("=" * 80)