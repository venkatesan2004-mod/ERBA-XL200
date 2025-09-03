import socket
import threading
import mysql.connector
from datetime import datetime

# MySQL Config
DB_CONFIG = {
    "host": "192.168.20.160",
    "user": "remoteapi",
    "password": "kmc@123",
    "database": "kmc_05_06_2025_server"
}

HOST = "0.0.0.0"   # Listen on all interfaces
PORT = 5001        # Check XL-200 manual for correct port

def get_order_from_db(samplebarcode):
    """Fetch assay codes + patient details from LIS order table"""
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
    return rows

def insert_results(sample_id, test_code, test_name, result, unit, reference, flag, dept):
    """Insert analyzer result into LIS_MACHINERESULT"""
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
        "Biochemistry Analyzer",      # LIS_MACHNAME
        "Erba XL-200",                # LIS_MACHID
        "",                           # LIS_LABID (if any external lab id)
        sample_id,                    # LIS_SAMPLENO
        test_name,                    # LIS_MACHTESTID
        result,                       # LIS_MACHRESULTS
        "",                           # LIS_ALARAM (empty unless machine sends)
        unit,                         # LIS_UNITS
        reference,                    # LIS_REFERENCE
        flag,                         # LIS_RESFLAG
        dept,                         # LIS_TESTDEPT
        0,                            # LIS_RPREVIEW
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # LIS_CREATEDDTTM
        0,                            # IsProcessed
        test_code                     # LIS_Test_Code
    )

    cursor.execute(query, values)
    conn.commit()
    conn.close()

def build_order_message(sample_id, patientname, dob, gender, assays):
    """Build ASTM O| message with assays"""
    assay_codes = "^".join([row["Assay_Code"] for row in assays if row["Assay_Code"]])
    return (
        f"H|\\^&|||LIS|||||P|1\r"
        f"P|1||{sample_id}||{patientname}||{dob}|{gender}\r"
        f"O|1|{sample_id}||{assay_codes}|||N|||||||||A\r"
        f"L|1|N\r"
    )

def parse_results(message):
    """Extract results from R| lines"""
    results = []
    for line in message.splitlines():
        if line.startswith("R|"):
            parts = line.split("|")
            if len(parts) >= 6:
                testname = parts[2].replace("^^^", "")   # e.g. GLU
                value = parts[3]                        # test result
                unit = parts[4]
                reference = parts[5] if len(parts) > 5 else ""
                flag = parts[6] if len(parts) > 6 else ""
                results.append((testname, value, unit, reference, flag))
    return results

def handle_client(conn, addr):
    print(f"Connected by {addr}")
    buffer = ""

    while True:
        data = conn.recv(1024)
        if not data:
            break
        buffer += data.decode(errors="ignore")

        # Check if ASTM message complete
        if "L|1|N" in buffer:
            print("Received:\n", buffer)

            # Check if this is a sample order request (contains P|)
            if "P|" in buffer and "O|" in buffer and not "R|" in buffer:
                # Extract sample barcode
                sample_id = None
                for line in buffer.splitlines():
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
                        print("Sending Worklist:\n", order_msg)
                        conn.send(order_msg.encode())
            
            # Check if this is result message
            if "R|" in buffer:
                sample_id = None
                for line in buffer.splitlines():
                    if line.startswith("O|"):
                        parts = line.split("|")
                        if len(parts) > 2:
                            sample_id = parts[2]

                results = parse_results(buffer)
                for testname, value, unit, reference, flag in results:
                    insert_results(sample_id, testname, testname, value, unit, reference, flag, "BIOCHEMISTRY")

            buffer = ""  # reset

    conn.close()

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"Listening on {HOST}:{PORT}...")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr)).start()

if __name__ == "__main__":
    start_server()
