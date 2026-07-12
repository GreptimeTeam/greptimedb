---
Feature Name: "Fault Tolerance for Region"
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/1126
Date: 2023-03-08
Author: "Luo Fucong <luofucong@greptime.com>"
---

Fault Tolerance for Region
----------------------

# Summary

This RFC proposes a method to achieve fault tolerance for regions in GreptimeDB's distributed mode. Or, put it in another way, achieving region high availability("HA") for GreptimeDB cluster. 

In this RFC, we mainly describe two aspects of region HA: how region availability is detected, and what recovery process is need to be taken. We also discuss some alternatives and future work.

When this feature is done, our users could expect a GreptimeDB cluster that can always handle their requests to regions, despite some requests may failed during the region failover. The optimization to reduce the MTTR(Mean Time To Recovery) is not a concern of this RPC, and is left for future work.

# Motivation

Fault tolerance for regions is a critical feature for our clients to use the GreptimeDB cluster confidently. High availability for users to interact with their stored data is a "must have" for any TSDB products, that include our GreptimeDB cluster.

# Details

## Background

Some backgrounds about region in distributed mode:

- A table is logically split into multiple regions. Each region stores a part of non-overlapping table data.
- Regions are distributed in Datanodes, the mappings are not static, are assigned and governed by Metasrv.
- In distributed mode, client requests are scoped in regions. To be more specific, when a request that needs to scan multiple regions arrived in Frontend, Frontend splits the request into multiple sub-requests, each of which scans one region only, and submits them to Datanodes that hold corresponding regions. 

In conclusion, as long as regions remain available, and regions could regain availability when failures do occur, the overall region HA could be achieved. With this in mind, let's see how region failures are detected first. 

## Failure Detection

We detect region failures in Metasrv, and do it both passively and actively. Passively means that Metasrv do not fire some "are you healthy" requests to regions. Instead, we carry region healthy information in the heartbeat requests that are submit to Metasrv by Datanodes. 

Datanode already carries its regions stats in the heartbeat request (the non-relevant fields are omitted):

```protobuf
message HeartbeatRequest {
  ...
  // Region stats on this node
  repeated RegionStat region_stats = 6;
  ...
}

message RegionStat {
  uint64 region_id = 1;
  TableName table_name = 2;
  ...
}
```

For the sake of simplicity, we don't add another field `bool available = 3` to the `RegionStat` message; instead, if the region were unavailable in the view of the Datanode that contains it, the Datanode just not includes the `RegionStat` of it in the heartbeat request. Or, if the Datanode itself is not unavailable, the heartbeat request is not submitted, effectively the same with not carrying the `RegionStat`.

> The heartbeat interval is now hardcoded to five seconds.

Metasrv gathers the heartbeat requests, extracts the `RegionStat`s, and treat them as region heartbeat. In this way, Metasrv maintains all regions healthy information. If some region's heartbeats were not received in a period of time, Metasrv speculates the region might be unavailable. To make the decision whether a region is failed or not, Metasrv uses a failure detection algorithm called the "[Phi φ Accrual Failure Detection](https://medium.com/@arpitbhayani/phi-%CF%86-accrual-failure-detection-79c21ce53a7a)". Basically, the algorithm calculates a value called "phi" to represent the possibility of a region's unavailability, based on the historical heartbeats' arrived rate. Once the "phi" is above some pre-defined threshold, Metasrv knows the region is failed.

> This algorithm has been widely adopted in some well known products, like Akka and Cassandra.

When Metasrv decides some region is failed from heartbeats, it's not the final decision. Here comes the "actively" detection. Before Metasrv decides to do region failover, it actively invokes the healthy check interface of the Datanode that the failure region resides. Only this healthy check is failed does Metasrv actually start doing failover upon the region.

To conclude, the failure detection pseudo-codes are like this:

```rust
// in Metasrv:
fn failure_detection() {
    loop {
        // passive detection
        let failed_regions = all_regions.iter().filter(|r| r.estimated_failure_possibility() > config.phi).collect();

        // find the datanodes that contains the failed regions
        let datanodes_and_regions = find_region_resides_datanodes(failed_regions);

        // active detection  
        for (datanode, regions) in datanodes_and_regions {
            if !datanode.is_healthy(regions) {
                do_failover(datanode, regions);
            }
        }

        sleep(config.detect_interval);
    }
}
```

Some design considerations:

- Why active detecting while we have passively detection? Because it could be happened that the network is singly connectable sometimes (especially in the complex Cloud environment), then the Datanode's heartbeats cannot reach Metasrv, while Metasrv could request Datanode. Active detecting avoid this false positive situation.
- Why the detection works on region instead of Datanode? Because we might face the possibility that only part of the regions in the Datanode are not available, not ALL regions. Especially the situation that Datanodes are used by multiple tenants. If this is the case, it's better to do failover upon the designated regions instead of the whole regions that reside on the Datanode. All in all, we want a more subtle control over region failover. 

So we detect some regions are not available. How to regain the availability back?

## Region Failover

Region Failover largely relies on remote WAL, aka "[Bunshin](https://github.com/GreptimeTeam/bunshin)". I'm not including any of the details of it in this RFC, let's just assume we already have it.

In general, region failover is fairly simple. Once Metasrv decides to do failover upon some regions, it first chooses one or more Datanodes to hold the failed region. This can be done easily, as the Metasrv already has the whole picture of Datanodes: it knows which Datanode has the minimum regions, what Datanode historically had the lowest CPU usage and IO rate, and how the Datanodes are assigned to tenants, among other information that can all help the Metasrv choose the most suitable Datanodes. Let's call these chosen Datanodes as "candidates".

> The strategy to choose the most suitable candidates required careful design, but it's another RFC.

Then, Metasrv sets the states of these failed regions as "passive". We should add a field to `Region`:

```protobuf
message Region {
  uint64 id = 1;
  string name = 2;
  Partition partition = 3;
  
  message State {
    Active,
    Passive,
  }
  State state = 4;
  
  map<string, string> attrs = 100;
}
```

Here `Region` is used in message `RegionRoute`, which indicates how the write request is split among regions. When a region is set as "passive", Frontend knows the write to it should be rejected at the moment (the region read is not blocked, however).

> Making a region "passive" here is effectively blocking the write to it. It's ok in the failover situation, the region is failed anyway. However, when dealing with active maintenance operations, region state requires more refined design. But that's another story. 

Third, Metasrv fires the "close region" requests to the failed Datanodes, and fires the "open region" requests to those candidates. "Close region" requests might be failed due to the unavailability of Datanodes, but that's fine, it's just a best-effort attempt to reduce the chance of any in-flight writes got handled unintentionally after the region is set as "passive". The "open region" requests must have succeeded though. Datanodes open regions from remote WAL.

> Currently the "close region" is undefined in Datanode. It could be a local cache clean up of region data or other resources tidy up.

Finally, when a candidate successfully opens its region, it calls back to Metasrv, indicating it is ready to handle region. "call back" here is backed by its heartbeat to Metasrv. Metasrv updates the region's state to "active", so as to let Frontend lifts the restrictions of region writes (again, the read part of region is untouched).

All the above steps should be managed by remote procedure framework. It's another implementation challenge in the region failover feature. (One is the remote WAL of course.)

A picture is worth a 1000 words:

```text
                                    +-------------------------+                                        
                                    | Metasrv detects region  |                                        
                                    | failure                 |                                        
                                    +-------------------------+                                        
                                                 |                                                     
                                                 v                                                     
                                    +----------------------------+                                        
                                    | Metasrv chooses candidates |                                        
                                    | to hold failed regions     |                                        
                                    +----------------------------+                                        
                                                 |                                                     
                                                 v                                                     
                                    +-------------------------+       +-------------------------+      
                                    | Metasrv "passive" the   |------>| Frontend rejects writes |      
                                    | failed regions          |       | to "passive" regions    |      
                                    +-------------------------+       +-------------------------+      
                                                 |                                                     
                                                 v                                                     
+--------------------------+        +---------------------------+                                        
| Candidate Datanodes open |<-------| Metasrv fires "close" and |                                        
| regions from remote WAL  |        | "open" region requests    |                                        
+--------------------------+        +---------------------------+                                        
             |                                                                                         
             |                                                                                         
             |                      +-------------------------+       +-------------------------+      
             +--------------------->| Metasrv "active" the    |------>| Frontend lifts write    |      
                                    | failed regions          |       | restriction to regions  |      
                                    +-------------------------+       +-------------------------+      
                                                 |                                                     
                                                 v                                                     
                                    +-------------------------+                                        
                                    | Region failover done,   |                                        
                                    | HA regain               |                                        
                                    +-------------------------+                                        
```

# Alternatives

## The "Neon" Way

Remote WAL raises a problem that could harm the write throughput of GreptimeDB cluster: each write request has to do at least two remote call, one is from Frontend to Datanode, and one is from Datanode to remote WAL. What if we do it the "[Neon](https://github.com/neondatabase/neon)" way, making remote WAL sits in between the Frontend and Datanode, couldn't that improve our write throughput? It could, though there're some consistency issues like "read-your-writes" to solve.

However, the main concerns we don't adopt this method are two-fold:

1. Remote WAL is planned to be quorum based, it can be efficiently written;
2. More importantly, we are planning to make the remote WAL an option that users could choose not to enable it (at the cost of some reliability reduction).

## No WAL, Replication instead

This method replicates region across Datanodes directly, like the common way in shared-nothing database. Were the main region failed, a standby region in the replicate group is elected as new "main" and take the read/write requests. The main concern to this method is the incompatibility to our current architecture and code structure. It requires a major redesign, but gains no significant advantage over the remote WAL method. 

However, the replication does have its own advantage that we can learn from to optimize this failover procedure.

# Future Work

Some optimizations we could take:

- To reduce the MTTR, we could make Metasrv chooses the candidate to each region at normal time. The candidate does some preparation works to reduce the open region time, effectively accelerate the failover procedure.
- We can adopt the replication method, to the degree that region replicas are used as the fast catch-up candidates. The data difference among replicas is minor, region failover does not need to load or exchange too much data, greatly reduced the region failover time.
