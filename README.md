# True time protocol implementation

True time protocol implementation is based on paper "Logical Physical Clocks
and consistent snapshots in globally distributed databases" by Marcelo Leone.
We maintain a logical clock to be always close to NTP clock and maintain
causality property e.g. e hb f => tt(e) < tt(f). Therefore we can further
reuse this time to return consistent global snapshots with no need to wait out
clock sync uncertainties like TrueTime did.

From this paper the main observation is that for any event f
|l.f - pt.f| <= eps, where eps is maximum offset maintained by NTP. At the same
time another observation is that: c.f <= N * (eps + 1), where N is number of nodes in the system for that last eps.

To maintain clock offset among set of nodes I used intersection algorithm that is also well-known as Marzullo algorithm. Synchronization of clocks in a network requires comparing them directly or indirectly with respect to differences in time and (possibly) frequency. The approach adopted here is based on variant of Christian's algorithm (Probabilistic clock synchronization).

Algorithm workflow:

    1. Each node asks other nodes in the system with heartbeat interval.
    2. Each node checks its offset with remote offset sources available each monitor interval (multiple of heartbeat interval)
        2.1 During this check, if offset couldn't be calculated, cause node to suicide
        2.2 During this check, if offset is out of "healthy" bounds, cause node to suicide. (false)
        2.3 If new offset interval is "healthier" than previous, cause update current ofset

Offset interval of node N is calculated as:

```[N.offset - N.error - N.maxoffset, N.offset + N.error + N.maxoffset]```

Base case:
- if the only source of time is node itself then we rely on its time with offset and error <0, 0>.



Further Work:
-------------

- maintain offset from ntp
- maintain maxoffset (According to current implementation, provide this arg emperically)
- simulation in multi node env
- simulation in WAN


memo:
1. check number of byte from both sites
2. why it's truncated
