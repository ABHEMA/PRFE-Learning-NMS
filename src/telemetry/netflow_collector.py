#!/usr/bin/env python3
import socket
import time
import csv
import netflow

TEMPLATES = {}

def decode_ipv4(addr_int):
    if addr_int is None:
        return None
    return "{}.{}.{}.{}".format(
        (addr_int >> 24) & 0xFF,
        (addr_int >> 16) & 0xFF,
        (addr_int >> 8) & 0xFF,
        addr_int & 0xFF
    )

def start_collector(listen_ip="0.0.0.0", listen_port=9999):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((listen_ip, listen_port))
    print(f"Listening on {listen_ip}:{listen_port} for NetFlow/IPFIX packets...")

    fieldnames = ["timestamp", "source", "record_type", "metric", "value",
                  "label", "message"]

    with open("netflow_flows.csv", "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csvfile.seek(0, 2)
        if csvfile.tell() == 0:
            writer.writeheader()

        while True:
            data, addr = sock.recvfrom(8192)
            exporter_ip = addr[0]
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

            try:
                packet = netflow.parse_packet(data, templates=TEMPLATES)
                if hasattr(packet, "templates") and packet.templates:
                    TEMPLATES.update(packet.templates)

                flows = getattr(packet, "flows", [])
                version = getattr(packet.header, "version", "?")
                flow_count = len(flows)

                record_type = "NETFLOW"

                for idx, flow in enumerate(flows, 1):
                    src_ip_int = getattr(flow, "IPV4_SRC_ADDR", None)
                    dst_ip_int = getattr(flow, "IPV4_DST_ADDR", None)
                    src_ip = decode_ipv4(src_ip_int)
                    dst_ip = decode_ipv4(dst_ip_int)

                    src_port = getattr(flow, "SRC_PORT", None)
                    dst_port = getattr(flow, "DST_PORT", None)
                    pkts     = getattr(flow, "IN_PACKETS", None)
                    octets   = getattr(flow, "IN_OCTETS", None)
                    proto    = getattr(flow, "PROTO", None)

                    label = ""
                    message = (f"Flow {idx}/{flow_count}, Ver:{version}, "
                               f"Src:{src_ip}:{src_port} -> {dst_ip}:{dst_port}, "
                               f"Pkts:{pkts}, Bytes:{octets}, Proto:{proto}")

                    if octets is not None and octets > 5e6:
                        label = "HIGH_FLOW"
                        message += " | Large flow detected"

                    if (dst_port == 23 or dst_port == 445):
                        label = "SUSPICIOUS_PORT"
                        message += f" | Suspicious DST port {dst_port}"

                    if proto == 1:
                        label = "ICMP_FLOW"
                        message += " | ICMP flow flagged"

                    row = {
                        "timestamp": timestamp,
                        "source": exporter_ip,
                        "record_type": record_type,
                        "metric": "flow",
                        "value": pkts,
                        "label": label,
                        "message": message
                    }
                    writer.writerow(row)
                    csvfile.flush()

            except Exception as e:
                print(f"{timestamp} - Failed {exporter_ip}: {e}")

def main():
    start_collector("0.0.0.0", 9999)

if __name__ == "__main__":
    main()

