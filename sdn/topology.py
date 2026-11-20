#!/usr/bin/env python3
"""Mininet 4-Host, 4-Switch Diamond Topology for Demo 1 & Demo 2.

Hosts:
  - Clients (Senders)  : h1 (10.0.0.1/24, s1 port 1), h3 (10.0.0.3/24, s1 port 4)
  - Servers (Receivers): h2 (10.0.0.2/24, s4 port 3), h4 (10.0.0.4/24, s4 port 4)

Switches:
  - s1, s2 (Primary upper branch), s3 (Backup lower branch), s4

Run with:
  sudo python3 topology.py
  sudo python3 topology.py --loss 5.0
  sudo python3 topology.py --no-delay
"""

import argparse
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.topo import Topo


class DiamondTopology(Topo):
    def build(self, loss_pct=0.0, use_delays=True):
        # Clients connected to ingress switch s1
        h1 = self.addHost("h1", ip="10.0.0.1/24", mac="00:00:00:00:00:01")
        h3 = self.addHost("h3", ip="10.0.0.3/24", mac="00:00:00:00:00:03")

        # Servers connected to egress switch s4
        h2 = self.addHost("h2", ip="10.0.0.2/24", mac="00:00:00:00:00:02")
        h4 = self.addHost("h4", ip="10.0.0.4/24", mac="00:00:00:00:00:04")

        for switch in ("s1", "s2", "s3", "s4"):
            self.addSwitch(switch, dpid=f"{int(switch[1:]):016x}", protocols="OpenFlow13")

        primary_delay = "5ms" if use_delays else "0ms"
        backup_delay = "10ms" if use_delays else "0ms"

        # Fixed host interface numbers
        self.addLink(h1, "s1", port2=1)
        self.addLink(h3, "s1", port2=4)
        self.addLink("s4", h2, port1=3)
        self.addLink("s4", h4, port1=4)

        # Primary Path: s1 -> s2 -> s4
        self.addLink("s1", "s2", port1=2, port2=1, cls=TCLink, delay=primary_delay, loss=loss_pct)
        self.addLink("s2", "s4", port1=2, port2=1, cls=TCLink, delay=primary_delay, loss=loss_pct)

        # Backup Path: s1 -> s3 -> s4
        self.addLink("s1", "s3", port1=3, port2=1, cls=TCLink, delay=backup_delay, loss=loss_pct)
        self.addLink("s3", "s4", port1=2, port2=2, cls=TCLink, delay=backup_delay, loss=loss_pct)


def main():
    parser = argparse.ArgumentParser(description="Mininet Diamond Topology (Multi-Client / Multi-Server)")
    parser.add_argument("--loss", type=float, default=0.0, help="Packet loss percentage on switch links (default: 0.0)")
    parser.add_argument("--no-delay", action="store_true", help="Disable the 5ms/10ms link delays")
    args = parser.parse_args()

    topo = DiamondTopology(loss_pct=args.loss, use_delays=not args.no_delay)
    net = Mininet(topo=topo, controller=None, switch=OVSSwitch,
                  link=TCLink, autoSetMacs=False)
    net.addController("c0", controller=RemoteController, ip="127.0.0.1", port=6653)
    try:
        net.start()
        info("\n*** Controller: 127.0.0.1:6653 (OpenFlow 1.3)\n")
        info("*** Clients: h1 (10.0.0.1), h3 (10.0.0.3) | Servers: h2 (10.0.0.2), h4 (10.0.0.4)\n")
        info(f"*** Link Parameters: loss={args.loss}%, delays={'disabled' if args.no_delay else '5ms primary / 10ms backup'}\n")
        info("*** First test: pingall\n")
        info("*** Fail primary path: link s1 s2 down\n")
        info("*** Restore path: link s1 s2 up\n")
        info("*** Inspect flows: sh ovs-ofctl -O OpenFlow13 dump-flows s1\n\n")
        CLI(net)
    finally:
        net.stop()


if __name__ == "__main__":
    setLogLevel("info")
    main()
