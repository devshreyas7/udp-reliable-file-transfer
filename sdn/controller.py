"""Ryu OpenFlow 1.3 controller with deterministic primary/backup rerouting,
multi-client/multi-server MAC routing, Layer-4 UDP prioritization, and PortStats monitoring.

Run in an Ubuntu Python environment with Ryu installed:
    ryu-manager --ofp-tcp-listen-port 6653 controller.py

To run in Static Baseline Mode (no automatic failover on link failure):
    SDN_MODE=static ryu-manager --ofp-tcp-listen-port 6653 controller.py
"""

import os
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, DEAD_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub


H1_MAC = "00:00:00:00:00:01"
H2_MAC = "00:00:00:00:00:02"
H3_MAC = "00:00:00:00:00:03"
H4_MAC = "00:00:00:00:00:04"
BROADCAST_MAC = "ff:ff:ff:ff:ff:ff"

# Undirected physical switch-to-switch links: (switch DPID, switch port) for each end.
LINKS = {
    "s1-s2": ((1, 2), (2, 1)),
    "s2-s4": ((2, 2), (4, 1)),
    "s1-s3": ((1, 3), (3, 1)),
    "s3-s4": ((3, 2), (4, 2)),
}
PRIMARY = ("s1-s2", "s2-s4")
BACKUP = ("s1-s3", "s3-s4")
FLOW_COOKIE = 0xC0DE
UDP_APP_PORTS = (5005, 5006)


def chosen_path(down_links, static_mode=False):
    """Pure logic function so route selection can be unit-tested without Ryu."""
    if static_mode:
        # Static baseline mode never reroutes to the backup path.
        if not any(edge in down_links for edge in PRIMARY):
            return (1, 2, 4)
        return None
    if not any(edge in down_links for edge in PRIMARY):
        return (1, 2, 4)
    if not any(edge in down_links for edge in BACKUP):
        return (1, 3, 4)
    return None


def path_ports(path):
    """Return [(dpid, ingress, egress)] for inter-switch traffic from s1 to s4 (h1 -> h2)."""
    if path is None:
        return []
    ports = [(1, 1, 2 if path[1] == 2 else 3)]
    ports.append((path[1], 1, 2))
    ports.append((4, 1 if path[1] == 2 else 2, 3))
    return ports


class FailoverController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datapaths = {}
        self.down_ports = set()
        self.current_path = None
        self.static_mode = os.environ.get("SDN_MODE", "dynamic").lower() == "static"
        self.monitor_thread = hub.spawn(self._monitor_port_stats)

    def down_links(self):
        return {name for name, ends in LINKS.items()
                if any(endpoint in self.down_ports for endpoint in ends)}

    def _install_flow(self, dp, priority, match, out_ports):
        parser, ofp = dp.ofproto_parser, dp.ofproto
        actions = [parser.OFPActionOutput(p) for p in out_ports]
        dp.send_msg(parser.OFPFlowMod(
            datapath=dp,
            cookie=FLOW_COOKIE,
            priority=priority,
            match=match,
            instructions=[parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)],
        ))

    def replace_flows(self):
        # Delete only our own flows (not the switch's priority-0 table-miss rule).
        for dp in self.datapaths.values():
            ofp, parser = dp.ofproto, dp.ofproto_parser
            dp.send_msg(parser.OFPFlowMod(
                datapath=dp, command=ofp.OFPFC_DELETE,
                cookie=FLOW_COOKIE, cookie_mask=0xffffffffffffffff,
                out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                table_id=ofp.OFPTT_ALL))

        if self.current_path is None:
            self.logger.warning("No available route: active path is broken (static_mode=%s)", self.static_mode)
            return

        mid_dpid = self.current_path[1]
        s1_wan_port = 2 if mid_dpid == 2 else 3
        s4_wan_port = 1 if mid_dpid == 2 else 2

        # 1. Switch s1 (Clients h1 on port 1, h3 on port 4; WAN on s1_wan_port)
        dp1 = self.datapaths.get(1)
        if dp1:
            p1 = dp1.ofproto_parser
            # Local client-to-client & client-to-server unicast on s1
            for in_p in (1, 4, s1_wan_port):
                for dst_mac, out_p in (
                    (H1_MAC, 1),
                    (H3_MAC, 4),
                    (H2_MAC, s1_wan_port),
                    (H4_MAC, s1_wan_port),
                ):
                    if in_p == out_p:
                        continue
                    self._install_flow(dp1, 100, p1.OFPMatch(in_port=in_p, eth_dst=dst_mac), [out_p])
                    # Priority 200 Application-Aware UDP rules for ports 5005 & 5006
                    for uport in UDP_APP_PORTS:
                        self._install_flow(
                            dp1, 200,
                            p1.OFPMatch(in_port=in_p, eth_dst=dst_mac, eth_type=0x0800, ip_proto=17, udp_dst=uport),
                            [out_p],
                        )
                        self._install_flow(
                            dp1, 200,
                            p1.OFPMatch(in_port=in_p, eth_dst=dst_mac, eth_type=0x0800, ip_proto=17, udp_src=uport),
                            [out_p],
                        )
            # Loop-free ARP broadcast handling on s1
            self._install_flow(dp1, 100, p1.OFPMatch(in_port=1, eth_dst=BROADCAST_MAC), [4, s1_wan_port])
            self._install_flow(dp1, 100, p1.OFPMatch(in_port=4, eth_dst=BROADCAST_MAC), [1, s1_wan_port])
            self._install_flow(dp1, 100, p1.OFPMatch(in_port=s1_wan_port, eth_dst=BROADCAST_MAC), [1, 4])

        # 2. Transit Switch s2 or s3 (port 1 <-> port 2)
        dp_mid = self.datapaths.get(mid_dpid)
        if dp_mid:
            pm = dp_mid.ofproto_parser
            for in_p, out_p, dst_macs in (
                (1, 2, (H2_MAC, H4_MAC, BROADCAST_MAC)),
                (2, 1, (H1_MAC, H3_MAC, BROADCAST_MAC)),
            ):
                for dst_mac in dst_macs:
                    self._install_flow(dp_mid, 100, pm.OFPMatch(in_port=in_p, eth_dst=dst_mac), [out_p])
            for uport in UDP_APP_PORTS:
                self._install_flow(dp_mid, 200, pm.OFPMatch(in_port=1, eth_type=0x0800, ip_proto=17, udp_dst=uport), [2])
                self._install_flow(dp_mid, 200, pm.OFPMatch(in_port=2, eth_type=0x0800, ip_proto=17, udp_src=uport), [1])

        # 3. Switch s4 (Servers h2 on port 3, h4 on port 4; WAN on s4_wan_port)
        dp4 = self.datapaths.get(4)
        if dp4:
            p4 = dp4.ofproto_parser
            for in_p in (3, 4, s4_wan_port):
                for dst_mac, out_p in (
                    (H2_MAC, 3),
                    (H4_MAC, 4),
                    (H1_MAC, s4_wan_port),
                    (H3_MAC, s4_wan_port),
                ):
                    if in_p == out_p:
                        continue
                    self._install_flow(dp4, 100, p4.OFPMatch(in_port=in_p, eth_dst=dst_mac), [out_p])
                    for uport in UDP_APP_PORTS:
                        self._install_flow(
                            dp4, 200,
                            p4.OFPMatch(in_port=in_p, eth_dst=dst_mac, eth_type=0x0800, ip_proto=17, udp_dst=uport),
                            [out_p],
                        )
                        self._install_flow(
                            dp4, 200,
                            p4.OFPMatch(in_port=in_p, eth_dst=dst_mac, eth_type=0x0800, ip_proto=17, udp_src=uport),
                            [out_p],
                        )
            # Loop-free ARP broadcast handling on s4
            self._install_flow(dp4, 100, p4.OFPMatch(in_port=3, eth_dst=BROADCAST_MAC), [4, s4_wan_port])
            self._install_flow(dp4, 100, p4.OFPMatch(in_port=4, eth_dst=BROADCAST_MAC), [3, s4_wan_port])
            self._install_flow(dp4, 100, p4.OFPMatch(in_port=s4_wan_port, eth_dst=BROADCAST_MAC), [3, 4])

        self.logger.info(
            "ACTIVE ROUTE: %s (mode=%s)",
            " -> ".join("s%d" % dpid for dpid in self.current_path),
            "STATIC" if self.static_mode else "DYNAMIC",
        )

    def recalculate(self, force=False):
        selected = chosen_path(self.down_links(), static_mode=self.static_mode)
        if selected != self.current_path or force:
            self.current_path = selected
            self.replace_flows()

    def _monitor_port_stats(self):
        """Periodically polls OpenFlow switch port counters to detect drops/errors."""
        while True:
            for dp in list(self.datapaths.values()):
                parser = dp.ofproto_parser
                req = parser.OFPPortStatsRequest(dp, 0, dp.ofproto.OFPP_ANY)
                dp.send_msg(req)
            hub.sleep(5)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev):
        dpid = ev.msg.datapath.id
        for stat in ev.msg.body:
            if stat.port_no <= 4 and (stat.tx_dropped > 0 or stat.rx_dropped > 0 or stat.rx_errors > 0):
                self.logger.info(
                    "[SDN TELEMETRY] s%d port %d: tx_pkts=%d rx_pkts=%d tx_dropped=%d rx_dropped=%d",
                    dpid, stat.port_no, stat.tx_packets, stat.rx_packets, stat.tx_dropped, stat.rx_dropped
                )

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features(self, ev):
        dp = ev.msg.datapath
        self.datapaths[dp.id] = dp
        parser = dp.ofproto_parser
        dp.send_msg(parser.OFPFlowMod(
            datapath=dp, priority=0, match=parser.OFPMatch(),
            instructions=[]))
        self.logger.info("Switch s%d connected", dp.id)
        self.recalculate(force=True)

    @set_ev_cls(ofp_event.EventOFPStateChange, DEAD_DISPATCHER)
    def switch_disconnected(self, ev):
        dp = ev.datapath
        if self.datapaths.get(dp.id) is dp:
            del self.datapaths[dp.id]
            self.logger.warning("Switch s%d disconnected", dp.id)

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status(self, ev):
        msg = ev.msg
        dp = msg.datapath
        ofp = dp.ofproto
        endpoint = (dp.id, msg.desc.port_no)
        if endpoint not in {end for ends in LINKS.values() for end in ends}:
            return
        is_down = (msg.reason == ofp.OFPPR_DELETE or
                   bool(msg.desc.state & ofp.OFPPS_LINK_DOWN) or
                   bool(msg.desc.config & ofp.OFPPC_PORT_DOWN))
        if is_down:
            self.down_ports.add(endpoint)
        else:
            self.down_ports.discard(endpoint)
        self.logger.warning("s%d port %d %s", dp.id, msg.desc.port_no,
                            "DOWN" if is_down else "UP")
        self.recalculate()
