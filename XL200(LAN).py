# import socket
# import threading
# import mysql.connector
# from datetime import datetime

# # MySQL Config
# DB_CONFIG = {
#     "host": "192.168.20.160",
#     "user": "remoteapi",
#     "password": "kmc@123",
#     "database": "kmc_05_06_2025_server"
# }

# HOST = "0.0.0.0"   # Listen on all interfaces
# PORT = 5001        # Check XL-200 manual for correct port

# def get_order_from_db(samplebarcode):
#     """Fetch assay codes + patient details from LIS order table"""
#     conn = mysql.connector.connect(**DB_CONFIG)
#     cursor = conn.cursor(dictionary=True)

#     cursor.execute("""
#         SELECT orderid, samplebarcode, patientid, patientname, dateofbirth,
#                age, gender, Assay_Code, testname, testcode, department, sampletype
#         FROM Patient_Lab_Testorder
#         WHERE samplebarcode = %s
#     """, (samplebarcode,))
#     rows = cursor.fetchall()

#     conn.close()
#     return rows

# def insert_results(sample_id, test_code, test_name, result, unit, reference, flag, dept):
#     """Insert analyzer result into LIS_MACHINERESULT"""
#     conn = mysql.connector.connect(**DB_CONFIG)
#     cursor = conn.cursor()

#     query = """
#         INSERT INTO LIS_MACHINERESULT
#         (LIS_MACHNAME, LIS_MACHID, LIS_LABID, LIS_SAMPLENO, LIS_MACHTESTID,
#          LIS_MACHRESULTS, LIS_ALARAM, LIS_UNITS, LIS_REFERENCE, LIS_RESFLAG,
#          LIS_TESTDEPT, LIS_RPREVIEW, LIS_CREATEDDTTM, IsProcessed, LIS_Test_Code)
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     values = (
#         "Biochemistry Analyzer",      # LIS_MACHNAME
#         "Erba XL-200",                # LIS_MACHID
#         "",                           # LIS_LABID (if any external lab id)
#         sample_id,                    # LIS_SAMPLENO
#         test_name,                    # LIS_MACHTESTID
#         result,                       # LIS_MACHRESULTS
#         "",                           # LIS_ALARAM (empty unless machine sends)
#         unit,                         # LIS_UNITS
#         reference,                    # LIS_REFERENCE
#         flag,                         # LIS_RESFLAG
#         dept,                         # LIS_TESTDEPT
#         0,                            # LIS_RPREVIEW
#         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # LIS_CREATEDDTTM
#         0,                            # IsProcessed
#         test_code                     # LIS_Test_Code
#     )

#     cursor.execute(query, values)
#     conn.commit()
#     conn.close()

# def build_order_message(sample_id, patientname, dob, gender, assays):
#     """Build ASTM O| message with assays"""
#     assay_codes = "^".join([row["Assay_Code"] for row in assays if row["Assay_Code"]])
#     return (
#         f"H|\\^&|||LIS|||||P|1\r"
#         f"P|1||{sample_id}||{patientname}||{dob}|{gender}\r"
#         f"O|1|{sample_id}||{assay_codes}|||N|||||||||A\r"
#         f"L|1|N\r"
#     )

# def parse_results(message):
#     """Extract results from R| lines"""
#     results = []
#     for line in message.splitlines():
#         if line.startswith("R|"):
#             parts = line.split("|")
#             if len(parts) >= 6:
#                 testname = parts[2].replace("^^^", "")   # e.g. GLU
#                 value = parts[3]                        # test result
#                 unit = parts[4]
#                 reference = parts[5] if len(parts) > 5 else ""
#                 flag = parts[6] if len(parts) > 6 else ""
#                 results.append((testname, value, unit, reference, flag))
#     return results

# def handle_client(conn, addr):
#     print(f"Connected by {addr}")
#     buffer = ""

#     while True:
#         data = conn.recv(1024)
#         if not data:
#             break
#         buffer += data.decode(errors="ignore")

#         # Check if ASTM message complete
#         if "L|1|N" in buffer:
#             print("Received:\n", buffer)

#             # Check if this is a sample order request (contains P|)
#             if "P|" in buffer and "O|" in buffer and not "R|" in buffer:
#                 # Extract sample barcode
#                 sample_id = None
#                 for line in buffer.splitlines():
#                     if line.startswith("P|"):
#                         parts = line.split("|")
#                         if len(parts) > 2:
#                             sample_id = parts[2]

#                 if sample_id:
#                     orders = get_order_from_db(sample_id)
#                     if orders:
#                         order_msg = build_order_message(
#                             sample_id,
#                             orders[0]["patientname"],
#                             orders[0]["dateofbirth"],
#                             orders[0]["gender"],
#                             orders
#                         )
#                         print("Sending Worklist:\n", order_msg)
#                         conn.send(order_msg.encode())
            
#             # Check if this is result message
#             if "R|" in buffer:
#                 sample_id = None
#                 for line in buffer.splitlines():
#                     if line.startswith("O|"):
#                         parts = line.split("|")
#                         if len(parts) > 2:
#                             sample_id = parts[2]

#                 results = parse_results(buffer)
#                 for testname, value, unit, reference, flag in results:
#                     insert_results(sample_id, testname, testname, value, unit, reference, flag, "BIOCHEMISTRY")

#             buffer = ""  # reset

#     conn.close()

# def start_server():
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.bind((HOST, PORT))
#         s.listen()
#         print(f"Listening on {HOST}:{PORT}...")
#         while True:
#             conn, addr = s.accept()
#             threading.Thread(target=handle_client, args=(conn, addr)).start()

# if __name__ == "__main__":
#     start_server()




import socket
import threading
import mysql.connector
from datetime import datetime

# ASTM control characters
STX = chr(2)
ETX = chr(3)
EOT = chr(4)
ENQ = chr(5)
ACK = chr(6)
NAK = chr(21)
CR = chr(13)

# MySQL Config
DB_CONFIG = {
    "host": "192.168.20.160",
    "user": "remoteapi",
    "password": "kmc@123",
    "database": "kmc_05_06_2025_server"
}

HOST = "0.0.0.0"
PORT = 5001

seq_counter = 1
seq_lock = threading.Lock()

# --- Logging function ---
def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# --- DATABASE FUNCTIONS ---
def get_order_from_db(samplebarcode):
    log(f"Fetching order for SampleID: {samplebarcode}")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT orderid, samplebarcode, patientid, patientname, dateofbirth,
               age, gender, Assay_Code, testname, testcode, department, sampletype
        FROM Patient_Lab_Testorder
        WHERE samplebarcode = %s
    """, (samplebarcode,))
    rows = cursor.fetchall()
    conn.close()
    log(f"Found {len(rows)} orders for SampleID: {samplebarcode}")
    return rows

def insert_results(sample_id, test_code, test_name, result, unit, reference, flag, dept):
    log(f"Inserting result for SampleID: {sample_id}, Test: {test_name}, Result: {result}")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = """
        INSERT INTO LIS_MACHINERESULT
        (LIS_MACHNAME, LIS_MACHID, LIS_LABID, LIS_SAMPLENO, LIS_MACHTESTID,
         LIS_MACHRESULTS, LIS_ALARAM, LIS_UNITS, LIS_REFERENCE, LIS_RESFLAG,
         LIS_TESTDEPT, LIS_RPREVIEW, LIS_CREATEDDTTM, IsProcessed, LIS_Test_Code)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    values = (
        "Biochemistry Analyzer",
        "Erba XL-200",
        "",
        sample_id,
        test_name,
        result,
        "",
        unit,
        reference,
        flag,
        dept,
        0,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        0,
        test_code
    )
    cursor.execute(query, values)
    conn.commit()
    conn.close()
    log(f"Result inserted for SampleID: {sample_id}, Test: {test_name}")

# --- ASTM MESSAGE FUNCTIONS ---
def calc_checksum(data):
    return f"{sum(bytearray(data, 'ascii')) % 256:02X}"

def wrap_astm_message(message):
    message_no_cr = message.replace("\r", "")
    checksum = calc_checksum(message_no_cr + ETX)
    return f"{STX}{message}{ETX}{checksum}{CR}"

def next_seq():
    global seq_counter
    with seq_lock:
        seq = seq_counter
        seq_counter += 1
        if seq_counter > 7:
            seq_counter = 1
        return seq

def build_order_message(sample_id, patientname, dob, gender, assays):
    seq = next_seq()
    assay_codes = "^".join([row["Assay_Code"] for row in assays if row["Assay_Code"]])
    msg = (
        f"H|^&|||LIS|||||P|1\r"
        f"P|1||{sample_id}||{patientname}||{dob}|{gender}\r"
        f"O|1|{sample_id}||{assay_codes}|||N|||||||||A\r"
        f"L|1|N\r"
    )
    log(f"Order message built for SampleID: {sample_id}")
    return wrap_astm_message(msg)

def parse_results(message):
    results = []
    for line in message.splitlines():
        if line.startswith("R|"):
            parts = line.split("|")
            if len(parts) >= 6:
                testname = parts[2].replace("^^^", "")
                value = parts[3]
                unit = parts[4]
                reference = parts[5] if len(parts) > 5 else ""
                flag = parts[6] if len(parts) > 6 else ""
                results.append((testname, value, unit, reference, flag))
                log(f"Parsed result: {testname}={value} {unit}, Ref={reference}, Flag={flag}")
    return results

# --- CLIENT HANDLER ---
def handle_client(conn, addr):
    log(f"Connected by {addr}")
    buffer = ""

    while True:
        try:
            data = conn.recv(1024)
            if not data:
                break
            data = data.decode(errors="ignore")
            buffer += data

            # ENQ/ACK handling
            if ENQ in buffer:
                log("ENQ received, sending ACK")
                conn.send(ACK.encode())
                buffer = buffer.replace(ENQ, "")

            # Complete ASTM message
            if ETX in buffer:
                log("Full ASTM message received")
                message_start = buffer.find(STX)
                message_end = buffer.find(ETX) + 1
                if message_start != -1 and message_end != -1:
                    astm_message = buffer[message_start:message_end]
                    log(f"ASTM message:\n{astm_message}")
                    conn.send(ACK.encode())
                    payload = astm_message.strip(STX + ETX + CR)

                    # Sample order request
                    if "P|" in payload and "O|" in payload and "R|" not in payload:
                        sample_id = None
                        for line in payload.splitlines():
                            if line.startswith("P|"):
                                parts = line.split("|")
                                if len(parts) > 2:
                                    sample_id = parts[2]
                        if sample_id:
                            orders = get_order_from_db(sample_id)
                            if orders:
                                order_msg = build_order_message(
                                    sample_id,
                                    orders[0]["patientname"],
                                    orders[0]["dateofbirth"],
                                    orders[0]["gender"],
                                    orders
                                )
                                log(f"Sending Worklist for SampleID: {sample_id}")
                                conn.send(order_msg.encode())

                    # Result message
                    if "R|" in payload:
                        sample_id = None
                        for line in payload.splitlines():
                            if line.startswith("O|"):
                                parts = line.split("|")
                                if len(parts) > 2:
                                    sample_id = parts[2]
                        results = parse_results(payload)
                        for testname, value, unit, reference, flag in results:
                            insert_results(sample_id, testname, testname, value, unit, reference, flag, "BIOCHEMISTRY")

                buffer = ""  # reset buffer
        except Exception as e:
            log(f"Error: {e}")
            break
    conn.close()
    log(f"Disconnected {addr}")

# --- SERVER ---
def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        log(f"Listening on {HOST}:{PORT}...")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_server()

