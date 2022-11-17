package caching;

import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class HazelcastCachingClient {
	
	private static final HazelcastCachingClient hzClient = new HazelcastCachingClient();
	private static final String MAP_NAME = "sandeep-distributed-map";
	private static HazelcastInstance hz;

	public static void main(String[] args) {
		
		
//		Config embeddedServerConfig = new Config();
//		embeddedServerConfig.setClusterName("hazelcast-embedded-server");
//		HazelcastInstance hz = Hazelcast.newHazelcastInstance(embeddedServerConfig);

		// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
		//HazelcastInstance hz = HazelcastClient.newHazelcastClient();
		// Get the Distributed Map from Cluster.
		
//		hzClient.putDataToCache(hzClient.getInstance(mapName),mapName,"name_1","sandeep");
		// Standard Put and Get.
//		String data = hzClient.getDataFromCacheByKey(hz, mapName, "name_1");
//		System.out.println("Key : name_1, Value : "+data );
		
		// Concurrent Map methods, optimistic updating
//		map.putIfAbsent("somekey", "somevalue");
//		map.replace("key", "value", "newvalue");
		// Shutdown this Hazelcast client
//		hz.shutdown();
		
		System.out.println("Cache Testing...");
		hz = hzClient.getInstance(MAP_NAME);
		Set<String> keys = new HashSet<String>();
		cacheNext(0,keys);

	}

	private static void cacheNext(int nextCount, Set<String> keys) {
		if(nextCount==10) {
			return;
		}
		System.out.println("Enter key and value to update");
		Scanner sc = new Scanner(System.in);
		String key = sc.nextLine();
		String value = sc.nextLine();
		hzClient.updateCache(hz, key, value);
		
		String data = hzClient.getDataFromCacheByKey(hz, key);
		System.out.println("updated cache with Key : "+key+", Value : "+data );
		keys.add(key);
		System.out.println("updated cache : "+hzClient.getAllDataFromCache(hz,keys) );
		
		
		nextCount = nextCount+1;
		cacheNext(nextCount,keys);
		nextCount = 0;
		hz.shutdown();
	}
	
	private HazelcastInstance getInstance(String clusterName) {
		if(null == hz) {
			Config embeddedServerConfig = new Config();
			embeddedServerConfig.setClusterName(clusterName);
			hz = Hazelcast.newHazelcastInstance(embeddedServerConfig);
		}
		return hz;
	}
	
	private void putDataToCache(HazelcastInstance hz, String key, String value) {
		IMap<String,String> map = hz.getMap(MAP_NAME);
		map.put(key, value);
	}
	
	private String getDataFromCacheByKey(HazelcastInstance hz, String key) {
		IMap<String,String> map = hz.getMap(MAP_NAME);
		return map.get(key);
	}
	
	private Map<String,String> getAllDataFromCache(HazelcastInstance hz,Set<String> keys) {
		IMap<String, String> map = hz.getMap(MAP_NAME);
		return map.getAll(keys);
	}
	
	private void updateCache(HazelcastInstance hz, String key, String value) {
		System.out.println("Updating cache " + MAP_NAME +" with key : "+key + ", value : "+value);
		IMap<String,String> map = hz.getMap(MAP_NAME);
		hzClient.putDataToCache(hz, key,value);
	}

}
