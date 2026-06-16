import threading
import random
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.secret_key = 'sync_web_secret_key_for_session_security'

# =========================================================================
# FIXED: Using async_mode='threading' to avoid unstable eventlet/gevent 
# dependencies on newer Python installations while ensuring native thread safety.
# =========================================================================
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# --- CONSOLE LOGGER ---
def log_to_console(department, message):
    """Prints a log in the backend and emits it via WebSockets to the scrolling terminal."""
    formatted_msg = f"[{department}] {message}"
    print(formatted_msg)
    socketio.emit('console_log', {'department': department, 'msg': formatted_msg})

# --- ROUTING LOGIC ---
@app.route('/')
def login_page():
    return render_template('login.html')

@app.route('/dashboard')
def dashboard_page():
    return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.json or {}
    username = data.get('username')
    password = data.get('password')

    if username == "admin" and password == "ssuet123":
        return jsonify({"success": True, "message": "Authentication successful!"})
    else:
        return jsonify({"success": False, "message": "Invalid credentials"})


# =========================================================================
# SECTION 1: IT SUPPORT DEPARTMENT — Shortest Job First (SJF) Scheduling
# =========================================================================
sjf_queue = []
sjf_active_job = None
sjf_lock = threading.Lock()
sjf_event = threading.Event()

def sjf_worker_loop():
    global sjf_queue, sjf_active_job
    log_to_console("IT-Support", "SJF background scheduler daemon started.")

    while True:
        with sjf_lock:
            has_jobs = len(sjf_queue) > 0

        if not has_jobs:
            sjf_event.clear()
            socketio.sleep(1.0)
            continue

        with sjf_lock:
            if sjf_queue:
                sjf_queue.sort(key=lambda x: x['burst_time'])
                sjf_active_job = sjf_queue.pop(0)
                log_to_console("IT-Support", f"Queue sorted by burst time. Shortest ticket selected: '{sjf_active_job['name']}' (burst: {sjf_active_job['burst_time']}s)")
            else:
                sjf_active_job = None

        if sjf_active_job:
            sjf_active_job['status'] = 'Active'
            socketio.emit('sjf_status', {'queue': sjf_queue, 'active': sjf_active_job})

            burst_time = sjf_active_job['burst_time']
            ticks = int(burst_time * 10)
            log_to_console("IT-Support", f"Executing ticket '{sjf_active_job['name']}' for {burst_time}s...")

            for t in range(ticks, 0, -1):
                sjf_active_job['remaining_time'] = round(t / 10.0, 1)
                socketio.emit('sjf_status', {'queue': sjf_queue, 'active': sjf_active_job})
                socketio.sleep(0.1)

            sjf_active_job['status'] = 'Completed'
            sjf_active_job['remaining_time'] = 0.0
            log_to_console("IT-Support", f"Ticket '{sjf_active_job['name']}' resolved successfully.")
            socketio.emit('sjf_status', {'queue': sjf_queue, 'active': sjf_active_job})

            socketio.sleep(0.5)
            sjf_active_job = None
            socketio.emit('sjf_status', {'queue': sjf_queue, 'active': None})


# =========================================================================
# SECTION 2: ADMIN DEPARTMENT — Multi-Level Feedback Queue (MLFQ)
# =========================================================================
mlfq_queues = {"Q0": [], "Q1": [], "Q2": []}
mlfq_active_task = None
mlfq_lock = threading.Lock()
mlfq_event = threading.Event()

def mlfq_worker_loop():
    global mlfq_queues, mlfq_active_task
    log_to_console("Admin-Dept", "MLFQ background scheduler daemon started.")

    quantums = {"Q0": 1.0, "Q1": 2.0, "Q2": 9999.0}

    while True:
        task = None
        source_queue = None

        with mlfq_lock:
            if mlfq_queues["Q0"]:
                task = mlfq_queues["Q0"].pop(0)
                source_queue = "Q0"
            elif mlfq_queues["Q1"]:
                task = mlfq_queues["Q1"].pop(0)
                source_queue = "Q1"
            elif mlfq_queues["Q2"]:
                task = mlfq_queues["Q2"].pop(0)
                source_queue = "Q2"

        if not task:
            mlfq_event.clear()
            socketio.sleep(1.0)
            continue

        mlfq_active_task = task
        task['status'] = 'Active'
        task['current_queue'] = source_queue

        quantum = quantums[source_queue]
        run_time = min(task['remaining_time'], quantum)

        log_to_console("Admin-Dept", f"Dispatching '{task['name']}' from {source_queue} (quantum: {quantum}s, remaining: {task['remaining_time']}s)")
        socketio.emit('mlfq_status', {'queues': mlfq_queues, 'active': mlfq_active_task})

        ticks = int(run_time * 10)
        for _ in range(ticks):
            socketio.sleep(0.1)
            task['remaining_time'] = round(task['remaining_time'] - 0.1, 1)
            if task['remaining_time'] < 0:
                task['remaining_time'] = 0.0
            socketio.emit('mlfq_status', {'queues': mlfq_queues, 'active': mlfq_active_task})

        mlfq_active_task = None

        if task['remaining_time'] <= 0:
            task['status'] = 'Completed'
            log_to_console("Admin-Dept", f"Task '{task['name']}' finished execution completely.")
            socketio.emit('mlfq_status', {'queues': mlfq_queues, 'active': None})
        else:
            if source_queue == "Q0":
                next_queue = "Q1"
                log_to_console("Admin-Dept", f"Task '{task['name']}' quantum expired in Q0. DEMOTING to Q1.")
            elif source_queue == "Q1":
                next_queue = "Q2"
                log_to_console("Admin-Dept", f"Task '{task['name']}' quantum expired in Q1. DEMOTING to Q2.")
            else:
                next_queue = "Q2"
                log_to_console("Admin-Dept", f"Task '{task['name']}' context-switched back into Q2.")

            task['status'] = 'Waiting'
            with mlfq_lock:
                mlfq_queues[next_queue].append(task)
            socketio.emit('mlfq_status', {'queues': mlfq_queues, 'active': None})

        socketio.sleep(0.5)


# =========================================================================
# SECTION 3: HR DEPARTMENT — Paging & Contiguous Memory Allocation
# =========================================================================
memory_blocks = [None] * 32
allocated_processes = {}
memory_lock = threading.Lock()

def get_memory_state():
    return {
        "blocks": memory_blocks,
        "processes": allocated_processes
    }


# =========================================================================
# SECTION 4: FINANCE DEPARTMENT — Thread Synchronization & Mutex Locks
# =========================================================================
shared_balance = 1000
finance_mutex = threading.Lock()
finance_simulation_active = False

transactions = [
    {"thread_id": "Payroll-A", "action": "Deposit",  "amount": 200, "status": "Idle"},
    {"thread_id": "Payroll-B", "action": "Withdraw", "amount": 150, "status": "Idle"},
    {"thread_id": "Payroll-C", "action": "Withdraw", "amount": 300, "status": "Idle"},
    {"thread_id": "Payroll-D", "action": "Deposit",  "amount": 400, "status": "Idle"},
    {"thread_id": "Payroll-E", "action": "Withdraw", "amount": 100, "status": "Idle"},
]

def run_unmanaged_thread(tx):
    global shared_balance
    tx['status'] = 'Waiting'
    socketio.emit('finance_tx_status', {'transactions': transactions, 'balance': shared_balance})
    socketio.sleep(random.uniform(0.1, 0.3))

    tx['status'] = 'Active'
    socketio.emit('finance_tx_status', {'transactions': transactions, 'balance': shared_balance})
    log_to_console("Finance-Race", f"{tx['thread_id']} reading shared budget. Value read: ${shared_balance}")

    local_val = shared_balance
    log_to_console("Finance-Race", f"{tx['thread_id']} entering critical section (NO LOCK)...")
    socketio.sleep(0.8)

    if tx['action'] == "Deposit":
        local_val += tx['amount']
    else:
        local_val -= tx['amount']

    shared_balance = local_val
    log_to_console("Finance-Race", f"{tx['thread_id']} writing back budget: ${shared_balance}")

    tx['status'] = 'Completed'
    socketio.emit('finance_tx_status', {'transactions': transactions, 'balance': shared_balance})

def run_managed_thread(tx):
    global shared_balance
    tx['status'] = 'Waiting'
    socketio.emit('finance_tx_status', {'transactions': transactions, 'balance': shared_balance})
    socketio.sleep(random.uniform(0.1, 0.3))

    log_to_console("Finance-Mutex", f"{tx['thread_id']} requesting Mutex lock...")

    with finance_mutex:
        tx['status'] = 'Active'
        socketio.emit('finance_tx_status', {'transactions': transactions, 'balance': shared_balance})
        log_to_console("Finance-Mutex", f"Mutex lock ACQUIRED by {tx['thread_id']}. Critical Section Safe.")

        local_val = shared_balance
        log_to_console("Finance-Mutex", f"{tx['thread_id']} safely read budget: ${local_val}")
        socketio.sleep(0.8)

        if tx['action'] == "Deposit":
            local_val += tx['amount']
        else:
            local_val -= tx['amount']

        shared_balance = local_val
        log_to_console("Finance-Mutex", f"{tx['thread_id']} writing back budget: ${shared_balance}")
        log_to_console("Finance-Mutex", f"Mutex lock RELEASED by {tx['thread_id']}.")

    tx['status'] = 'Completed'
    socketio.emit('finance_tx_status', {'transactions': transactions, 'balance': shared_balance})


def finance_simulation_thread(mode):
    global shared_balance, finance_simulation_active, transactions
    finance_simulation_active = True

    log_to_console("Finance-Dept", f"Starting Finance Simulation in '{mode.upper()}' mode...")

    for tx in transactions:
        tx['status'] = 'Idle'
    socketio.emit('finance_tx_status', {'transactions': transactions, 'balance': shared_balance})

    threads = []
    for tx in transactions:
        target_fn = run_unmanaged_thread if mode == 'unmanaged' else run_managed_thread
        t = socketio.start_background_task(target=target_fn, tx=tx)
        threads.append(t)

    socketio.sleep(5.0)
    finance_simulation_active = False
    log_to_console("Finance-Dept", f"Finance Simulation complete. Final Budget: ${shared_balance}")
    socketio.emit('finance_simulation_done', {'balance': shared_balance, 'success': (shared_balance == 1050)})


# =========================================================================
# WEBSOCKET EVENT HANDLERS
# =========================================================================
@socketio.on('add_sjf_job')
def handle_add_sjf_job(data):
    name = data.get('name', 'Ticket-' + str(random.randint(100, 999)))
    try:
        burst_time = float(data.get('burst_time', 2.0))
    except ValueError:
        burst_time = 2.0

    job = {
        "id": str(random.randint(10000, 99999)),
        "name": name,
        "burst_time": burst_time,
        "remaining_time": burst_time,
        "status": "Waiting"
    }

    with sjf_lock:
        sjf_queue.append(job)

    log_to_console("IT-Support", f"New ticket '{name}' submitted with burst time {burst_time}s.")
    socketio.emit('sjf_status', {'queue': sjf_queue, 'active': sjf_active_job})
    sjf_event.set()

@socketio.on('clear_sjf')
def handle_clear_sjf():
    global sjf_queue
    with sjf_lock:
        sjf_queue = []
    log_to_console("IT-Support", "Cleared all pending tickets from SJF queue.")
    socketio.emit('sjf_status', {'queue': sjf_queue, 'active': sjf_active_job})

@socketio.on('add_mlfq_task')
def handle_add_mlfq_task(data):
    name = data.get('name', 'AdminTask-' + str(random.randint(100, 999)))
    try:
        burst_time = float(data.get('burst_time', 4.0))
    except ValueError:
        burst_time = 4.0

    task = {
        "id": str(random.randint(10000, 99999)),
        "name": name,
        "burst_time": burst_time,
        "remaining_time": burst_time,
        "status": "Waiting",
        "current_queue": "Q0"
    }

    with mlfq_lock:
        mlfq_queues["Q0"].append(task)

    log_to_console("Admin-Dept", f"Admin task '{name}' submitted (burst: {burst_time}s). Entering High Priority Queue Q0.")
    socketio.emit('mlfq_status', {'queues': mlfq_queues, 'active': mlfq_active_task})
    mlfq_event.set()

@socketio.on('clear_mlfq')
def handle_clear_mlfq():
    global mlfq_queues
    with mlfq_lock:
        mlfq_queues = {"Q0": [], "Q1": [], "Q2": []}
    log_to_console("Admin-Dept", "Cleared all MLFQ queues.")
    socketio.emit('mlfq_status', {'queues': mlfq_queues, 'active': mlfq_active_task})

@socketio.on('allocate_memory')
def handle_allocate_memory(data):
    global memory_blocks, allocated_processes
    pname = data.get('process_name', '').strip()
    try:
        size = int(data.get('size', 4))
    except ValueError:
        size = 4
    method = data.get('method', 'contiguous')

    if not pname:
        log_to_console("HR-Dept", "Allocation failed: Employee record name is empty.")
        return

    with memory_lock:
        if pname in allocated_processes:
            log_to_console("HR-Dept", f"Allocation failed: Record '{pname}' already exists.")
            socketio.emit('memory_status', get_memory_state())
            return

        if size <= 0 or size > 32:
            log_to_console("HR-Dept", f"Allocation failed: Invalid size request of {size} frames.")
            socketio.emit('memory_status', get_memory_state())
            return

        if method == 'contiguous':
            start_idx = -1
            consecutive_free = 0
            for i in range(32):
                if memory_blocks[i] is None:
                    if consecutive_free == 0:
                        start_idx = i
                    consecutive_free += 1
                    if consecutive_free == size:
                        break
                else:
                    consecutive_free = 0
                    start_idx = -1

            if consecutive_free == size:
                for i in range(start_idx, start_idx + size):
                    memory_blocks[i] = pname
                allocated_processes[pname] = {
                    "size": size,
                    "method": "contiguous",
                    "blocks": list(range(start_idx, start_idx + size))
                }
                log_to_console("HR-Dept", f"Contiguously allocated '{pname}' ({size} blocks) starting at frame {start_idx}.")
            else:
                log_to_console("HR-Dept", f"Contiguous allocation FAILED — external fragmentation.")
                socketio.emit('memory_error', {'msg': f"Insufficient contiguous blocks for '{pname}'."})

        else:
            free_indices = [i for i, val in enumerate(memory_blocks) if val is None]
            if len(free_indices) >= size:
                allocated_frames = free_indices[:size]
                page_table = {}
                for page_idx, frame_idx in enumerate(allocated_frames):
                    memory_blocks[frame_idx] = pname
                    page_table[page_idx] = frame_idx

                allocated_processes[pname] = {
                    "size": size,
                    "method": "paged",
                    "page_table": page_table
                }
                log_to_console("HR-Dept", f"Paged allocation of '{pname}' ({size} pages) succeeded.")
            else:
                log_to_console("HR-Dept", f"Paged allocation FAILED — insufficient frames.")
                socketio.emit('memory_error', {'msg': f"Insufficient frames available."})

        socketio.emit('memory_status', get_memory_state())

@socketio.on('deallocate_memory')
def handle_deallocate_memory(data):
    global memory_blocks, allocated_processes
    pname = data.get('process_name', '').strip()

    with memory_lock:
        if pname not in allocated_processes:
            log_to_console("HR-Dept", f"Deallocation failed: Record '{pname}' not found.")
            return

        for i in range(32):
            if memory_blocks[i] == pname:
                memory_blocks[i] = None

        del allocated_processes[pname]
        log_to_console("HR-Dept", f"Deallocated employee record '{pname}' from workspace.")
        socketio.emit('memory_status', get_memory_state())

@socketio.on('clear_memory')
def handle_clear_memory():
    global memory_blocks, allocated_processes
    with memory_lock:
        memory_blocks = [None] * 32
        allocated_processes = {}
    log_to_console("HR-Dept", "Cleared all allocated workspace memory.")
    socketio.emit('memory_status', get_memory_state())

@socketio.on('get_memory_initial')
def handle_get_memory_initial():
    socketio.emit('memory_status', get_memory_state())

@socketio.on('run_finance_sim')
def handle_run_finance_sim(data):
    global shared_balance, finance_simulation_active
    if finance_simulation_active:
        log_to_console("Finance-Dept", "Simulation already running. Wait for completion.")
        return

    mode = data.get('mode', 'managed')
    socketio.start_background_task(target=finance_simulation_thread, mode=mode)

@socketio.on('reset_finance')
def handle_reset_finance():
    global shared_balance, finance_simulation_active
    if finance_simulation_active:
        log_to_console("Finance-Dept", "Cannot reset while simulation is active.")
        return
    shared_balance = 1000
    for tx in transactions:
        tx['status'] = 'Idle'
    log_to_console("Finance-Dept", "Shared company budget reset to $1000.")
    socketio.emit('finance_tx_status', {'transactions': transactions, 'balance': shared_balance})


if __name__ == '__main__':
    # Start scheduler loops cleanly with background task context wrappers
    socketio.start_background_task(target=sjf_worker_loop)
    socketio.start_background_task(target=mlfq_worker_loop)

    print("=" * 60)
    print("  Sync-Web Simulation Server (Stable Native Mode)")
    print("  Running on http://127.0.0.1:5000/")
    print("=" * 60)
    
    socketio.run(app, host='127.0.0.1', port=5000, debug=False, allow_unsafe_werkzeug=True)