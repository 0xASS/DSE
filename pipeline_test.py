import redis
import time
import random

def execute_optimized_batch_ingestion(vehicle_count=1000):
    """
    PURPOSE: Packs multiple GEOADD telemetry updates into a single network 
             frame to drastically eliminate Round-Trip Time (RTT) overhead.
    """
    # 1. Establish a connection reuse pool to completely avoid TCP handshake overhead
    pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
    client = redis.Redis(connection_pool=pool)
    
    # Generate mock vehicle tracking telemetry coordinates (Bucharest bounding box)
    mock_telemetry_data = [
        (f"vehicle_{i}", random.uniform(26.05, 26.15), random.uniform(44.40, 44.50))
        for i in range(vehicle_count)
    ]
    
    # 2. Instantiate a pipelined execution block.
    # Setting transaction=False improves speed when absolute multi-command isolation isn't mandatory.
    pipeline = client.pipeline(transaction=False)
    
    print(f"📦 Packing {vehicle_count} telemetry writes into pipeline buffer...")
    start_time = time.perf_counter()
    
    # 3. Buffer commands sequentially into local memory (no network calls are made yet)
    for vehicle_id, lon, lat in mock_telemetry_data:
        pipeline.geoadd("fleet_coordinates", (lon, lat, vehicle_id))
        
    # 4. Flush/Execute the entire buffer across the docker virtual bridge net in a single I/O burst
    results = pipeline.execute()
    
    end_time = time.perf_counter()
    duration = (end_time - start_time) * 1000 # convert to ms
    throughput = vehicle_count / (end_time - start_time)
    
    print(f"🚀 Batch execution completed successfully!")
    print(f"⏱️ Total Time taken: {duration:.2f} ms")
    print(f"📊 Achieved Throughput: {throughput:.2f} operations/sec")
    
    return sum(1 for res in results if res)

if __name__ == "__main__":
    execute_optimized_batch_ingestion(5000)