import json
import os
import threading
import uuid
from pathlib import Path
from datetime import datetime, timedelta, date

from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_from_directory
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from werkzeug.routing.exceptions import BuildError

import requests
from playwright.sync_api import sync_playwright
from playwright._impl._errors import TargetClosedError
from workers import start_worker

# --- HẰNG SỐ CỦA WORKER ---
FLOW_URL = "https://labs.google/fx/vi/tools/flow/project/447e27e0-fb0a-44b1-99f0-1624c028d6c4"
P2V_GENERATE_BTN_XPATH = '//*[@id="__next"]/div[2]/div/div/div[2]/div/div[1]/div[2]/div/div[2]/div[2]/button[2]'
PROMPT_BOX = "#PINHOLE_TEXT_AREA_ELEMENT_ID"
CHECK_STATUS_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchCheckAsyncVideoGenerationStatus" 

# --- CUSTOM JSON ENCODER để xử lý các đối tượng không phải JSON ---
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime) or isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, threading.Event):
             return "threading.Event (Removed)"
        return json.JSONEncoder.default(self, obj)

# --- KHỞI TẠO VÀ CẤU HÌNH ---
app = Flask(__name__)
app.secret_key = 'your_strong_secret_key_here' 
app.config['UPLOAD_FOLDER'] = 'storage/uploads'

# CẤU HÌNH ĐƯỜNG DẪN LƯU TRỮ VÀ TẠO THƯ MỤC
STORAGE_DIR = Path("storage")
Path(STORAGE_DIR).mkdir(exist_ok=True) 
Path(app.config['UPLOAD_FOLDER']).mkdir(parents=True, exist_ok=True) 

COOKIE_PATH = STORAGE_DIR / "cookie.json"
VIDEO_SAVE_PATH = STORAGE_DIR / "Generated_Videos"
USERS_DB_PATH = STORAGE_DIR / "users.json" 
Path(VIDEO_SAVE_PATH).mkdir(exist_ok=True) 

GLOBAL_COOKIES = None
ACTIVE_TASKS = {}  
LAST_ACTIVITY = {} 

# --- QUẢN LÝ USER DATABASE ---
def load_users():
    if not USERS_DB_PATH.exists():
        admin_data = {
            "admin": {
                "username": "admin",
                "password_hash": generate_password_hash("admin_pass123"),
                "name": "Nguyễn Đức Thắng (Admin)",
                "team": "Development",
                "is_admin": True,
                "history": {}, 
                "created_at": str(datetime.now())
            }
        }
        with open(USERS_DB_PATH, 'w', encoding='utf-8') as f:
            json.dump(admin_data, f, indent=4)
        return admin_data
    
    with open(USERS_DB_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_users(users_data):
    with open(USERS_DB_PATH, 'w', encoding='utf-8') as f:
        json.dump(users_data, f, indent=4, cls=CustomJSONEncoder)

USERS_DB = load_users()


# --- CHỨC NĂNG CHIA LỊCH THEO NGÀY ---
def get_allowed_team():
    """Xác định Team nào được phép chạy hôm nay."""
    today = date.today().day
    if today % 2 != 0:
        return "Team Lê Thắng"
    return "Team Lê Cường"

def is_team_allowed_today(user_team):
    """Kiểm tra Team User có được phép chạy hôm nay không."""
    if user_team == "Development" or user_team == "N/A": 
        return True, "Development"
        
    allowed_team = get_allowed_team()
    
    if user_team == allowed_team:
        return True, allowed_team
        
    return False, allowed_team

def update_user_history(username, task_id, status):
    """Cập nhật lịch sử chạy của user vào USERS_DB."""
    global USERS_DB
    user = USERS_DB.get(username)
    if user:
        if 'history' not in user:
            user['history'] = {}
            
        today_str = date.today().strftime("%Y-%m-%d")
        
        if today_str not in user['history']:
            user['history'][today_str] = []
            
        task_data = ACTIVE_TASKS.get(task_id, {})
        
        task_info = {
            "task_id": task_id,
            "time": datetime.now().strftime("%H:%M:%S"),
            "status": status,
            "type": task_data.get('type', 'N/A'),
            "resolution": task_data.get('resolution', 'N/A'),
            "total": task_data.get('total', 0), 
            "items": task_data.get('items', []) 
        }

        updated = False
        if today_str in user['history']:
            for item in user['history'][today_str]:
                if item['task_id'] == task_id:
                    item['status'] = status
                    item['time'] = datetime.now().strftime("%H:%M:%S")
                    updated = True
                    break
        
        if not updated or status == "Khởi tạo":
             user['history'][today_str].append(task_info)


        save_users(USERS_DB)


def verify_user(username, password):
    user = USERS_DB.get(username)
    if user and check_password_hash(user['password_hash'], password):
        return user
    return None

def get_active_users():
    """Trả về danh sách các user được coi là đang online."""
    online_users = {}
    time_limit = datetime.now() - timedelta(seconds=30)
    
    for username, last_seen in list(LAST_ACTIVITY.items()):
        if last_seen > time_limit:
            user_data = USERS_DB.get(username)
            if user_data:
                online_users[username] = {
                    "username": username,
                    "name": user_data.get('name', 'N/A'),
                    "team": user_data.get('team', 'N/A'),
                    "last_seen": last_seen.strftime("%H:%M:%S")
                }
            
    return online_users


# --- PRE-REQUEST HOOK: Cập nhật thời gian hoạt động ---
@app.before_request
def update_last_activity():
    if 'username' in session:
        LAST_ACTIVITY[session['username']] = datetime.now()
    
    global GLOBAL_COOKIES
    if GLOBAL_COOKIES is None and COOKIE_PATH.exists():
        try:
            GLOBAL_COOKIES = json.loads(COOKIE_PATH.read_text(encoding='utf-8'))
        except:
            pass


# --- KIỂM TRA TRẠNG THÁI COOKIE (Admin/User) ---
def get_auth_token_from_cookies(cookies):
    """Sử dụng Playwright để lấy Auth Token (Bearer) từ cookies."""
    if not cookies:
        return None
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, timeout=10000)
            context = browser.new_context()
            context.add_cookies(cookies)
            page = context.new_page()
            
            page.goto(FLOW_URL, timeout=15000)
            
            with page.expect_response(lambda resp: "batchAsyncGenerateVideoText" in resp.url, timeout=15000) as response_info:
                page.locator(PROMPT_BOX).fill("test")
                page.locator(f"xpath={P2V_GENERATE_BTN_XPATH}").click()
                
            request = response_info.value.request
            auth_header = request.headers.get("authorization")
            
            browser.close()
            if auth_header and auth_header.startswith("Bearer "):
                return auth_header
            return None
            
    except Exception as e:
        print(f"Lỗi khi lấy token qua Playwright: {e}")
        return None


@app.route('/api/admin/check_cookie')
def check_cookie_status():
    if not session.get('is_admin'):
        return jsonify({"status": "error", "message": "Truy cập bị từ chối"}), 403

    if not GLOBAL_COOKIES:
        return jsonify({"status": "dead", "message": "Chưa có file cookie nào được tải."})

    try:
        auth_token = get_auth_token_from_cookies(GLOBAL_COOKIES)
        
        if not auth_token:
             return jsonify({"status": "dead", "message": "Không lấy được Authorization Token từ cookies (Cookies có thể chết)."})

        headers = {"Authorization": auth_token}
        resp = requests.post(
            CHECK_STATUS_URL,
            json={"operations": [{"operation": {"name": "projects/dummy/operations/test"}}]},
            headers=headers,
            timeout=5
        )
        
        if resp.status_code == 200 or resp.status_code == 400 or resp.status_code == 404:
             return jsonify({"status": "live", "message": "Cookies đang hoạt động (Token LIVE)."}), 200
        
        if resp.status_code == 401:
             return jsonify({"status": "dead", "message": "Token bị từ chối (401 - Unauthorized)."}), 200

        return jsonify({"status": "unknown", "message": f"Lỗi không xác định: {resp.status_code}"}), 200

    except Exception as e:
        return jsonify({"status": "dead", "message": f"Lỗi kết nối/timeout: {e}"}), 200

@app.route('/api/user/check_token_status')
def user_check_token_status():
    """API kiểm tra token cho User."""
    if not session.get('username'):
        return jsonify({"status": "error", "message": "Yêu cầu bị từ chối"}), 403

    if not GLOBAL_COOKIES:
        return jsonify({"status": "dead", "message": "Chưa có file cookie nào được tải."})

    try:
        auth_token = get_auth_token_from_cookies(GLOBAL_COOKIES)
        if not auth_token:
             return jsonify({"status": "dead", "message": "Không lấy được Authorization Token từ cookies."})

        headers = {"Authorization": auth_token}
        resp = requests.post(CHECK_STATUS_URL, json={"operations": [{"operation": {"name": "projects/dummy/operations/test"}}]}, headers=headers, timeout=5)
        
        if resp.status_code in [200, 400, 404]:
             return jsonify({"status": "live", "message": "Token đang hoạt động."})
        
        if resp.status_code == 401:
             return jsonify({"status": "dead", "message": "Token đã chết (401). Vui lòng liên hệ Admin."})

        return jsonify({"status": "unknown", "message": f"Lỗi không xác định: {resp.status_code}"})

    except Exception:
        return jsonify({"status": "dead", "message": "Lỗi kết nối hoặc timeout khi kiểm tra token."})


# --- ROUTES CHÍNH ---

@app.route('/')
def index():
    if 'username' not in session:
        return redirect(url_for('login'))
    
    current_user = USERS_DB.get(session.get('username'))
    
    if current_user and current_user.get('is_admin'):
        return redirect(url_for('admin_dashboard')) 
    else:
        return redirect(url_for('user_dashboard')) 

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user = verify_user(username, password)
        
        if user:
            session['username'] = username
            session['is_admin'] = user.get('is_admin', False)
            return redirect(url_for('index'))
        
        return render_template('login.html', error="Tên đăng nhập hoặc mật khẩu không đúng.")
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    if 'username' in session:
        LAST_ACTIVITY.pop(session['username'], None)
        
    session.pop('username', None)
    session.pop('is_admin', None)
    return redirect(url_for('login'))

# --- ADMIN ROUTES ---
@app.route('/admin')
def admin_dashboard():
    current_user = USERS_DB.get(session.get('username'))
    if not current_user or not current_user.get('is_admin'):
        return "Truy cập bị từ chối", 403
        
    global GLOBAL_COOKIES
    cookie_status = "Đã tải" if GLOBAL_COOKIES else "Chưa tải"
    
    tasks_for_render = {}
    for task_id, task in ACTIVE_TASKS.items():
        task_clean = deepcopy(task) 
        task_clean.pop('stop_flag', None) 
        if 'start_time' in task_clean and isinstance(task_clean['start_time'], datetime):
             task_clean['start_time'] = str(task_clean['start_time'])
        tasks_for_render[task_id] = task_clean
    
    users_data = []
    for uname, data in USERS_DB.items():
        user_copy = data.copy()
        user_copy.pop('password_hash', None)
        users_data.append(user_copy)
        
    return render_template('admin.html', 
                           cookie_status=cookie_status, 
                           tasks=tasks_for_render, 
                           users=users_data,
                           admin_name=current_user.get('name', 'Admin'))


@app.route('/api/admin/users', methods=['POST'])
def create_or_update_user():
    if not session.get('is_admin'):
        return jsonify({"success": False, "message": "Truy cập bị từ chối"}), 403
    
    data = request.json
    username = data.get('username')
    
    if not username or not data.get('name') or not data.get('team'):
        return jsonify({"success": False, "message": "Thiếu các trường bắt buộc (username, name, team)."}), 400

    if USERS_DB.get(username) and request.method == 'POST' and not data.get('is_edit'):
        return jsonify({"success": False, "message": "Tài khoản đã tồn tại. Dùng chức năng Sửa."}), 409
        
    is_new_user = username not in USERS_DB
    
    if is_new_user and not data.get('password'):
        return jsonify({"success": False, "message": "Phải có mật khẩu cho tài khoản mới."}), 400

    if is_new_user:
        USERS_DB[username] = {
            "username": username,
            "name": data['name'],
            "team": data['team'],
            "is_admin": False,
            "history": {},
            "created_at": str(datetime.now())
        }
    
    USERS_DB[username].update({
        "name": data['name'],
        "team": data['team']
    })

    if data.get('password'):
        USERS_DB[username]['password_hash'] = generate_password_hash(data['password'])

    save_users(USERS_DB)
    return jsonify({"success": True, "message": f"Tài khoản {username} đã được {'tạo mới' if is_new_user else 'cập nhật'}."})

@app.route('/api/admin/users/<username>', methods=['DELETE'])
def delete_user(username):
    if not session.get('is_admin'):
        return jsonify({"success": False, "message": "Truy cập bị từ chối"}), 403
        
    if username == session['username']:
        return jsonify({"success": False, "message": "Không thể tự xóa tài khoản Admin."}), 403
        
    if username in USERS_DB:
        del USERS_DB[username]
        save_users(USERS_DB)
        return jsonify({"success": True, "message": f"Tài khoản {username} đã bị xóa."})
        
    return jsonify({"success": False, "message": "Không tìm thấy tài khoản."}), 404

@app.route('/api/admin/active_users')
def get_active_users_api():
    if not session.get('is_admin'):
        return jsonify([]), 403
    
    return jsonify(get_active_users())

# --- UPLOAD COOKIE TEXT (ADMIN) ---
@app.route('/admin/upload_cookie_text', methods=['POST'])
def upload_cookie_text():
    if not session.get('is_admin'):
        return jsonify({"success": False, "message": "Truy cập bị từ chối"}), 403

    try:
        # Lấy dữ liệu dạng JSON từ body request
        data = request.json.get('cookie_data')
        
        if not data:
            return jsonify({"success": False, "message": "Không tìm thấy dữ liệu cookie."})

        cookies_list = []
        # Xử lý các định dạng cookie khác nhau
        if isinstance(data, dict) and "cookies" in data and isinstance(data["cookies"], list):
            cookies_list = data["cookies"]
        elif isinstance(data, list):
            cookies_list = data
        else:
            return jsonify({"success": False, "message": "Nội dung JSON không phải là danh sách cookie hợp lệ."})
            
        global GLOBAL_COOKIES
        GLOBAL_COOKIES = cookies_list
        # Lưu vào file để sử dụng trong tương lai
        COOKIE_PATH.write_text(json.dumps(cookies_list), encoding='utf-8')
        
        return jsonify({"success": True, "message": f"Cập nhật cookie thành công. {len(cookies_list)} mục đã được lưu."})
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Lỗi xử lý JSON: {e}"})


@app.route('/api/upload_i2v', methods=['POST'])
def upload_i2v_files():
    if not session.get('username'):
        return jsonify({"success": False, "message": "Truy cập bị từ chối"}), 403

    uploaded_files = request.files.getlist('i2v_files')
    file_paths = []
    
    if not uploaded_files or uploaded_files[0].filename == '':
        return jsonify({"success": False, "message": "Không tìm thấy file nào."})

    for file in uploaded_files:
        if file and file.filename.lower().endswith(('.png', '.jpg', '.jpeg')):
            filename = secure_filename(f"{session['username']}_{uuid.uuid4().hex[:8]}_{file.filename}")
            save_path = Path(app.config['UPLOAD_FOLDER']) / filename
            file.save(save_path)
            file_paths.append(str(save_path))
        
    if not file_paths:
        return jsonify({"success": False, "message": "Không có file ảnh hợp lệ nào được upload."})

    return jsonify({"success": True, "file_paths": file_paths, "message": f"Đã upload thành công {len(file_paths)} file."})

@app.route('/api/submit_task', methods=['POST'])
def submit_task():
    username = session.get('username')
    user_data = USERS_DB.get(username)
    if not user_data:
        return jsonify({"success": False, "message": "Không tìm thấy dữ liệu người dùng."}), 404
        
    # KIỂM TRA QUYỀN CHẠY XEN KẼ
    is_allowed, allowed_team = is_team_allowed_today(user_data.get('team', 'N/A'))
    
    if not is_allowed:
        return jsonify({"success": False, "message": f"Hôm nay ({date.today().day}) thuộc Team {allowed_team} chạy tool. Vui lòng thử lại vào ngày khác."}), 403

    if not username or not GLOBAL_COOKIES:
        return jsonify({"success": False, "message": "Yêu cầu bị từ chối. Vui lòng đăng nhập hoặc liên hệ Admin."}), 403

    data = request.json
    task_type = data.get('type')

    if task_type not in ['P2V', 'I2V']:
        return jsonify({"success": False, "message": "Loại tác vụ không hợp lệ."}), 400

    task_id = str(uuid.uuid4())
    stop_event = threading.Event() 

    worker_params = {
        "type": task_type,
        "resolution": data.get('resolution', '720p'),
        "cookies": GLOBAL_COOKIES,
        "username": username,
        "save_dir": str(VIDEO_SAVE_PATH),
        "stop_flag": stop_event, 
        
        "prompts": data.get('prompts', []), 
        "tasks": data.get('tasks', []), 
    }
    
    ACTIVE_TASKS[task_id] = {
        "id": task_id,
        "user": username,
        "status": "Initializing",
        "progress": 0,
        "total": len(worker_params.get('prompts') or worker_params.get('tasks') or []),
        "completed": 0,
        "errors": 0,
        "log": [],
        "items": [], 
        "stop_flag": stop_event, 
        "type": task_type,
        "resolution": worker_params['resolution']
    }

    threading.Thread(target=start_worker, args=(task_id, ACTIVE_TASKS, worker_params), daemon=True).start()
    
    # CẬP NHẬT LỊCH SỬ CHẠY
    update_user_history(username, task_id, "Khởi tạo")
    
    return jsonify({
        "success": True, 
        "task_id": task_id, 
        "message": f"Tác vụ {task_type} đã được khởi động."
    })

@app.route('/api/get_tasks')
def get_tasks():
    if not session.get('username'):
        return jsonify([]), 403

    user_tasks = {k: v for k, v in ACTIVE_TASKS.items() if v['user'] == session['username']}
    
    tasks_to_return = json.loads(json.dumps(user_tasks, default=str)) 

    return jsonify(tasks_to_return)

@app.route('/api/stop_task/<task_id>', methods=['POST'])
def stop_task(task_id):
    if not session.get('username') or task_id not in ACTIVE_TASKS:
        return jsonify({"success": False, "message": "Không tìm thấy tác vụ."}), 404

    task = ACTIVE_TASKS[task_id]
    if task['user'] != session['username'] and not session.get('is_admin'):
         return jsonify({"success": False, "message": "Không có quyền dừng tác vụ này."}), 403
         
    if task['status'] == 'Running' or task['status'] == 'Initializing':
        if 'stop_flag' in task and isinstance(task['stop_flag'], threading.Event):
             task['stop_flag'].set()
             update_user_history(task['user'], task_id, "Đã dừng")
             return jsonify({"success": True, "message": f"Yêu cầu dừng tác vụ {task_id} đã được gửi."})

    return jsonify({"success": False, "message": "Tác vụ không ở trạng thái Running hoặc không thể dừng."})

@app.route('/downloads/<path:filename>')
def download_file(filename):
    if not session.get('username'):
        return "Truy cập bị từ chối", 403
    return send_from_directory(str(VIDEO_SAVE_PATH), filename, as_attachment=True)


@app.route('/user') 
def user_dashboard():
    if 'username' not in session:
        return redirect(url_for('login'))
    
    current_user_data = USERS_DB.get(session.get('username'))
    if not current_user_data:
        return redirect(url_for('logout'))

    global GLOBAL_COOKIES
    if not GLOBAL_COOKIES:
        pass

    user_tasks = {k: v for k, v in ACTIVE_TASKS.items() if v['user'] == session['username']}
    
    is_allowed, allowed_team = is_team_allowed_today(current_user_data.get('team', 'N/A'))

    return render_template('user.html', 
                           username=session['username'], 
                           tasks=user_tasks,
                           user_info={
                               'name': current_user_data.get('name', 'User'),
                               'team': current_user_data.get('team', 'N/A')
                           },
                           schedule={
                               'is_allowed': is_allowed,
                               'allowed_team': allowed_team,
                               'today_date': date.today().strftime("%d/%m/%Y")
                           },
                           user_history=current_user_data.get('history', {}) 
                          )

if __name__ == '__main__':
    
    app.run(host='0.0.0.0', port=8080, debug=True)
