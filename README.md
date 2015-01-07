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
