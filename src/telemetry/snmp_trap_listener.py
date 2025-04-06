#!/usr/bin/env python3
from pysnmp.entity import engine, config
from pysnmp.carrier.asyncore.dgram import udp
from pysnmp.entity.rfc3413 import ntfrcv
import csv
import time
import logging

snmpEngine = engine.SnmpEngine()

TRAP_ADDRESS = '0.0.0.0'
TRAP_PORT = 162

fieldnames = ["timestamp", "source", "record_type", "metric", "value", "label", "message"]

csvfile = open("snmp_traps.csv", "a", newline="")
writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
csvfile.seek(0, 2)
if csvfile.tell() == 0:
    writer.writeheader()

logging.basicConfig(filename='received_traps.log',
                    filemode='a',
                    format='%(asctime)s - %(message)s',
                    level=logging.INFO)

logging.info(f"Agent is listening for SNMP traps on {TRAP_ADDRESS}:{TRAP_PORT}")
print(f"Agent is listening for SNMP traps on {TRAP_ADDRESS}:{TRAP_PORT}")

config.addTransport(
    snmpEngine,
    udp.domainName + (1,),
    udp.UdpTransport().openServerMode((TRAP_ADDRESS, TRAP_PORT))
)
config.addV1System(snmpEngine, 'my-area', 'public')

def trap_callback(snmpEngine, stateReference, contextEngineId, contextName, varBinds, cbCtx):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    source = "UNKNOWN"  
    record_type = "SNMP_TRAP"
    lines = []
    for name, val in varBinds:
        lines.append(f"{name.prettyPrint()} = {val.prettyPrint()}")
    message_str = " | ".join(lines)

    row = {
        "timestamp": timestamp,
        "source": source,
        "record_type": record_type,
        "metric": "trap",
        "value": "",
        "label": "",
        "message": message_str
    }
    writer.writerow(row)
    csvfile.flush()

    logging.info("Received new Trap message")
    for name, val in varBinds:
        logging.info('%s = %s' % (name.prettyPrint(), val.prettyPrint()))
    logging.info("==== End of Trap ====")

    print("Received new Trap message")
    for name, val in varBinds:
        print(f"{name.prettyPrint()} = {val.prettyPrint()}")
    print("---- End of Trap ----")

ntfrcv.NotificationReceiver(snmpEngine, trap_callback)

snmpEngine.transportDispatcher.jobStarted(1)

try:
    snmpEngine.transportDispatcher.runDispatcher()
except KeyboardInterrupt:
    print("Exiting trap listener...")
    logging.info("Exiting trap listener due to KeyboardInterrupt")
    snmpEngine.transportDispatcher.closeDispatcher()
    csvfile.close()
except Exception as e:
    print(f"Error in trap listener: {e}")
    logging.info(f"Error in trap listener: {e}")
    snmpEngine.transportDispatcher.closeDispatcher()
    csvfile.close()
    raise

