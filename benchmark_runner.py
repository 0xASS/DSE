# benchmark_runner.py
import redis
import time
import random
import concurrent.futures

# Configuration matching Phase 3 Docker Compose environment
MASTER_HOST = 'localhost'
MASTER_PORT = 6379
REPLICA_PORT = 6380

def get_master_client():
    pool = redis.ConnectionPool(host=MASTER_HOST, port=MASTER_PORT, decode_responses=True)
    return redis.Redis(connection_pool=pool)

def get_replica_client():
    pool = redis.ConnectionPool(host=MASTER_HOST, port=REPLICA_PORT, decode_responses=True)
    return redis.Redis(connection_pool=pool)

def simulate_geospatial_write(client, client_id):
    """Simulates high-frequency coordinate tracking ingestion."""
    lat = random.uniform(44.40, 44.50)
    lon = random.uniform(26.05, 26.15)
    member = f"vehicle_{client_id}_{random.randint(1, 1000)}"
    
    start_time = time.perf_counter()
    try:
        client.geoadd("bucharest_traffic", (lon, lat, member))
        latency = (time.perf_counter() - start_time) * 1000 # convert to ms
        return True, latency
    except redis.exceptions.RedisError:
        return False, 0

def simulate_geospatial_read(client):
    """Simulates real-time dashboard reading queries."""
    start_time = time.perf_counter()
    try:
        # Fetching nearby vehicles around center coordinate
        client.georadius("bucharest_traffic", 26.10, 44.45, 5, unit="km", num=10)
        latency = (time.perf_counter() - start_time) * 1000
        return True, latency
    except redis.exceptions.RedisError:
        return False, 0

def run_stress_test(scenario_name, target_node="master", operational_mode="write", concurrency=50, total_ops=10000):
    client = get_master_client() if target_node == "master" else get_replica_client()
    print(f"\n🚀 Executing {scenario_name} ({concurrency} concurrent workers, {total_ops} total tasks)...")
    
    latencies = []
    success_count = 0
    fail_count = 0
    
    start_wall_time = time.perf_counter()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for i in range(total_ops):
            if operational_mode == "write":
                futures.append(executor.submit(simulate_geospatial_write, client, i))
            else:
                futures.append(executor.submit(simulate_geospatial_read, client))
                
        for future in concurrent.futures.as_completed(futures):
            success, latency = future.result()
            if success:
                success_count += 1
                latencies.append(latency)
            else:
                fail_count += 1

    end_wall_time = time.perf_counter()
    total_time = end_wall_time - start_wall_time
    throughput = success_count / total_time if total_time > 0 else 0
    
    if latencies:
        latencies.sort()
        p50 = latencies[int(len(latencies) * 0.50)]
        p99 = latencies[int(len(latencies) * 0.99)]
    else:
        p50, p99 = 0, 0
        
    print(f"--- Results for {scenario_name} ---")
    print(f"Throughput: {throughput:.2f} RPS")
    print(f"P50 Latency: {p50:.2f} ms")
    print(f"P99 Latency: {p99:.2f} ms")
    print(f"Success/Fail: {success_count}/{fail_count}")
    return throughput, p50, p99, fail_count

if __name__ == "__main__":
    # Scenario 1: Low-load baseline reads from replica
    run_stress_test("Scenario 1: Baseline Replica Reads", target_node="replica", operational_mode="read", concurrency=10, total_ops=2000)
    
    # Scenario 2: High-throughput ingestion writes to master
    run_stress_test("Scenario 2: High-Throughput Master Writes", target_node="master", operational_mode="write", concurrency=100, total_ops=20000)
    
    # Scenario 3: Continuous operations simulating background tracking for fault injection
    print("\n⚠️ For Scenario 3: Run the test loop below, and manually execute 'docker restart redis-master' in your separate terminal.")
    time.sleep(2)
    run_stress_test("Scenario 3: Fault-Injection Ingestion Resiliency", target_node="master", operational_mode="write", concurrency=30, total_ops=15000)