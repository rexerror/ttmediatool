import json
import time
import requests
import re
import uuid
import threading
from pathlib import Path
from playwright.sync_api import sync_playwright
from playwright._impl._errors import TargetClosedError

# --- HẰNG SỐ CỦA WORKER ---
FLOW_URL = "https://labs.google/fx/vi/tools/flow/project/447e27e0-fb0a-44b1-99f0-1624c028d6c4"
PROMPT_BOX = "#PINHOLE_TEXT_AREA_ELEMENT_ID"
CHECK_STATUS_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchCheckAsyncVideoGenerationStatus" 
P2V_GENERATE_BTN_XPATH = '//*[@id="__next"]/div[2]/div/div/div[2]/div/div[1]/div[2]/div/div[2]/div[2]/button[2]'
I2V_SELECT_WORKFLOW_BTN = '#__next > div.sc-314021ff-1.hxuFUQ > div > div > div.sc-b0c0bd7-1.kvzLFA > div > div.sc-897c0dbb-0.eHacXb > div.sc-26b8f84c-0.fswRSj > div > div.sc-5086ca4e-0.gSrdLv > div.sc-5086ca4e-1.iuebFX > button'
I2V_SELECT_IMG2VID_BTN = 'text=Tạo video từ các khung hình'
I2V_UPLOAD_BTN = '#__next > div.sc-314021ff-1.hxuFUQ > div > div > div.sc-b0c0bd7-1.kvzLFA > div > div.sc-897c0dbb-0.eHacXb > div.sc-26b8f84c-0.fswRSj > div > div.sc-408537d4-0.eBSqXt > div:nth-child(1) > div > div:nth-child(1) > button'
I2V_SELECT_FROM_DESKTOP_BTN_XPATH = '//*[@id="radix-:rh:"]/div/div/div/div[2]/div[2]/button'
I2V_GENERATE_BTN = '#__next > div.sc-314021ff-1.hxuFUQ > div > div > div.sc-b0c0bd7-1.kvzLFA > div > div.sc-897c0dbb-0.eHacXb > div.sc-26b8f84c-0.fswRSj > div > div.sc-408537d4-0.eBSqXt > div.sc-408537d4-1.eiHkev > button'
I2V_CROP_AND_SAVE_BTN = 'button:has-text("Cắt và lưu")'
RADIX_BTN_SELECTOR = "#radix-\\3a r42\\3a" 
UPGRADE_BTN_TEXT = "Đã tăng độ phân giải (1080p)"
CHECK_UPSCALE_URL = "batchAsyncUpscaleVideo" 
LATEST_VIDEO_CARD_SELECTOR = 'div[role="list"] > div:nth-child(1) > div > div:nth-child(1)' 

def sanitize_filename(s, maxlen=50):
    s = re.sub(r'[\\/:"*?<>|]+', '', s)
    s = re.sub(r'\s+', '_', s).strip('_')
    return s[:maxlen] or "video"
    
def wait_for_generate_button(generate_locator, job_id, task_store, max_wait=60):
    task_store.log(f"⏳ [{job_id}] Đang chờ nút Generate được kích hoạt (max {max_wait}s)...")
    start_wait = time.time()
    generate_locator.wait_for(state="visible", timeout=10000)
    
    while not generate_locator.is_enabled() and time.time() - start_wait < max_wait:
        time.sleep(1)
    
    if not generate_locator.is_enabled():
          raise Exception(f"Timeout {max_wait}s: Nút Generate vẫn bị vô hiệu hóa.")
          
    task_store.log(f"✅ [{job_id}] Nút Generate đã sẵn sàng.")

# --- LỚP TASK STORE MỚI: Dùng để thay thế pyqtSignal ---
class TaskStore:
    def __init__(self, task_id, tasks_db, total_prompts, username, is_i2v, prompts_or_tasks, stop_flag_event):
        self.task_id = task_id
        self.tasks_db = tasks_db
        self.total_prompts = total_prompts
        self.username = username
        self.is_i2v = is_i2v
        self.prompts_or_tasks = prompts_or_tasks
        self.stop_flag = stop_flag_event 
        self.init_status()
        
    def init_status(self):
        initial_items = []
        for item in self.prompts_or_tasks:
            if self.is_i2v and isinstance(item, list) and len(item) == 2:
                initial_items.append({"image": item[0].split('/')[-1], "prompt": item[1], "status": "Pending", "file": ""}) # Pending
            elif not self.is_i2v and isinstance(item, str):
                initial_items.append({"prompt": item, "status": "Pending", "file": ""}) # Pending
            else:
                 initial_items.append({"prompt": "N/A (Lỗi khởi tạo item)", "status": "Error", "file": ""})

        if self.task_id in self.tasks_db:
             self.tasks_db[self.task_id].update({
                "status": "Running",
                "total": self.total_prompts,
                "items": initial_items
             })
        else:
             self.tasks_db[self.task_id] = {
                 "status": "Running", "total": self.total_prompts, "items": initial_items,
                 "user": self.username, "progress": 0, "completed": 0, "errors": 0,
                 "log": ["WARNING: TaskStore initialized late."], "stop_flag": self.stop_flag
             }
        
    def log(self, text):
        ts = time.strftime("%H:%M:%S")
        self.tasks_db[self.task_id]["log"].append(f"[{ts}] {text}")
        
    def update_item_status(self, item_index, status, file_path=""):
        if item_index < len(self.tasks_db[self.task_id]["items"]):
            # Chuẩn hóa trạng thái English cho Frontend dễ xử lý
            if "Hoàn thành" in status: status = "Finished"
            elif "Đang xử lý" in status: status = "Running"
            elif "Lỗi" in status: status = "Error"
            elif "Tạm dừng" in status: status = "Stopped"
                
            self.tasks_db[self.task_id]["items"][item_index]["status"] = status
            self.tasks_db[self.task_id]["items"][item_index]["file"] = file_path
        
    def update_progress(self, completed, errors):
        self.tasks_db[self.task_id]["completed"] = completed
        self.tasks_db[self.task_id]["errors"] = errors
        pending = self.total_prompts - completed - errors
        
        if pending < 0: pending = 0 
        
        if self.total_prompts == 0:
            progress_percent = 0
        else:
            progress_percent = int((completed / self.total_prompts) * 100)
            
        self.tasks_db[self.task_id]["progress"] = progress_percent
        
    def set_final_status(self, status):
        self.tasks_db[self.task_id]["status"] = status
        
    def stop_requested(self):
        return self.stop_flag.is_set()

# --- HÀM POLLING CHUNG (Giữ nguyên) ---
def poll_status(auth_token, operation_id, job_id, task_store):
    video_url = None
    poll_start = time.time()
    log_interval = 30
    last_log_time = time.time()
    
    while time.time() - poll_start < 300 and not task_store.stop_requested():
        try:
            headers = {"Content-Type": "application/json"}
            if auth_token:
                headers["Authorization"] = auth_token
                
            resp = requests.post(
                CHECK_STATUS_URL,
                json={"operations": [{"operation": {"name": operation_id}}]},
                headers=headers
            )
            resp.raise_for_status()
            result = resp.json()

            status = result.get("operations", [{}])[0].get("status")
            if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL":
                try:
                    video_url = result["operations"][0]["operation"]["metadata"]["video"]["fifeUrl"]
                    task_store.log(f"🎬 [{job_id}] Hoàn thành, đã có video URL.")
                    return video_url
                except KeyError as e:
                    raise Exception(f"Lỗi trích xuất URL từ JSON phản hồi: {e}")
                    
            elif status == "MEDIA_GENERATION_STATUS_FAILED":
                raise Exception("Tác vụ tạo video đã thất bại trên server.")
            
            if time.time() - last_log_time > log_interval:
                task_store.log(f"⏳ [{job_id}] Đang chờ... Status: {status}")
                last_log_time = time.time()
                
        except Exception as poll_e:
            task_store.log(f"⚠️ [{job_id}] Lỗi khi kiểm tra trạng thái: {poll_e}")
            if "401 Client Error" in str(poll_e):
                task_store.log("🚫 Lỗi 401: Token đã hết hạn. Vui lòng chạy lại ứng dụng để lấy token mới.")
            break
        time.sleep(5)
        
    if task_store.stop_requested():
        raise Exception("Tác vụ bị dừng bởi người dùng.")

    if not video_url:
        raise Exception("Timeout, không thể lấy được video URL sau 5 phút.")
    return video_url


# --- BASE WORKER (Giữ nguyên) ---
class BaseWorker(threading.Thread):
    def __init__(self, task_id, tasks_db, params, is_i2v):
        super().__init__()
        self.task_id = task_id
        self.is_i2v = is_i2v
        self.prompts_or_tasks = params['prompts'] if not is_i2v else params['tasks']
        
        self.save_dir = Path(params['save_dir']) 
        self.cookies = params['cookies']
        self.resolution = params['resolution']
        self.username = params['username']
        
        self.total_prompts = len(self.prompts_or_tasks)
        self.completed_prompts = 0
        self.error_prompts = 0
        self.pending_errors = []
        self.auth_token = None
        
        stop_event = tasks_db[task_id]['stop_flag']
        self.task_store = TaskStore(task_id, tasks_db, self.total_prompts, self.username, self.is_i2v, self.prompts_or_tasks, stop_event)
        
        
    def _upscale_and_download(self, page, original_filename_prefix, job_id, original_op_id):
        # ... (Logic _upscale_and_download giữ nguyên) ...
        video_url = None 
        
        try:
            self.task_store.log(f"🎬 [{job_id}] Video hoàn thành. Bắt đầu tăng độ phân giải...")
            
            # 1. TÌM CARD VIDEO MỚI NHẤT VÀ CLICK
            self.task_store.log(f"🔍 [{job_id}] Đang tìm card video mới nhất để kích hoạt Upscale...")
            
            video_card_locator = page.locator(LATEST_VIDEO_CARD_SELECTOR)
            video_card_locator.wait_for(timeout=30000)
            
            video_card_locator.click()
            self.task_store.log(f"▶️ [{job_id}] Đã click mở video mới nhất.")
            time.sleep(5) 
            
            # 2. Click nút có selector đặc biệt
            try:
                page.locator(RADIX_BTN_SELECTOR).wait_for(timeout=15000)
                page.locator(RADIX_BTN_SELECTOR).click()
                self.task_store.log(f"⚙️ [{job_id}] Đã click nút Tùy chỉnh ({RADIX_BTN_SELECTOR}).")
                time.sleep(3) 
            except Exception as e:
                self.task_store.log(f"⚠️ [{job_id}] Lỗi: Không tìm thấy nút Tùy chỉnh. {e}")
                raise Exception("Cannot find Radix button, aborting upscale.")
                
            # 3. Click nút tăng độ phân giải (1080p) và lắng nghe phản hồi API mới
            with page.expect_response(lambda resp: CHECK_UPSCALE_URL in resp.url, timeout=120000) as response_info:
                page.locator(f'text="{UPGRADE_BTN_TEXT}"').wait_for(timeout=10000)
                page.locator(f'text="{UPGRADE_BTN_TEXT}"').click()
                self.task_store.log(f"🚀 [{job_id}] Đã kích hoạt tăng độ phân giải (1080p).")

            time.sleep(10) 
            upscale_response = response_info.value
            upscale_data = upscale_response.json()
            
            upscale_op_id = upscale_data["operations"][0]["operation"]["name"]
            self.task_store.log(f"🔑 [{job_id}] Operation ID Upscale: {upscale_op_id}")

            # 4. Polling cho tác vụ Upscaling
            new_video_url = poll_status(self.auth_token, upscale_op_id, f"{job_id}_1080p", self.task_store)
            video_url = new_video_url
            
            # 5. Đóng giao diện xem video để chuẩn bị cho tác vụ tiếp theo (Click ESC)
            try:
                page.keyboard.press("Escape")
                time.sleep(2)
            except:
                pass
            
            filename = f"{'I2V_' if self.is_i2v else ''}1080p_{original_filename_prefix}_{job_id}.mp4"
            self.task_store.log(f"✅ [{job_id}] Upscaling hoàn thành, sẽ tải video 1080p.")

        except Exception as e:
            self.task_store.log(f"❌ [{job_id}] Lỗi Upscaling: {e}")
            raise 
            
        return video_url, filename

    def run(self):
        pass

# --- P2V WORKER ---
class P2VWorker(BaseWorker):
    def __init__(self, task_id, tasks_db, params):
        super().__init__(task_id, tasks_db, params, is_i2v=False)
        self.prompts = params['prompts']
        
    def _process_prompt(self, page, idx, prompt, job_id, is_retry=False):
        video_url = None
        original_op_id = None
        
        try:
            generate_locator = page.locator(f"xpath={P2V_GENERATE_BTN_XPATH}")
            
            self.task_store.update_item_status(idx, "Running") 
            self.task_store.log(f"📝 [{job_id}] Đã nhập prompt: {prompt[:100]}...")
            page.locator(PROMPT_BOX).fill(prompt)
            
            with page.expect_response(lambda resp: "batchAsyncGenerateVideoText" in resp.url and resp.status == 200, timeout=120000) as response_info:
                page.locator(f"xpath={P2V_GENERATE_BTN_XPATH}").click()

            time.sleep(10) 
            response = response_info.value
            data = response.json()
            
            op_data = data["operations"][0].get("operation")
            original_op_id = op_data["name"]

            # Polling trạng thái cho video gốc (720p)
            video_url_original = poll_status(self.auth_token, original_op_id, job_id, self.task_store)

            filename_prefix = sanitize_filename(prompt)
            filename = f"720p_{filename_prefix}_{job_id}.mp4" 

            if self.resolution == "1080p":
                try:
                    video_url, filename = self._upscale_and_download(
                        page, filename_prefix, job_id, original_op_id
                    )
                except Exception as upscale_e:
                    self.task_store.log(f"⚠️ [{job_id}] Lỗi Upscaling. Tải video gốc 720p...")
                    video_url = video_url_original
            else:
                video_url = video_url_original

            # Bắt đầu tải video
            filepath = self.save_dir / filename
            self.task_store.log(f"⬇️ [{job_id}] Đang tải video về: {filepath}")
            
            try:
                with requests.get(video_url, stream=True, timeout=300) as r:
                    r.raise_for_status()
                    with open(filepath, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
            except Exception as dl_e:
                raise Exception(f"Lỗi khi tải/lưu file: {dl_e}")

            self.task_store.log(f"✅ [{job_id}] Đã lưu: {filepath.name}")
            self.completed_prompts += 1
            self.task_store.update_item_status(idx, "Finished", str(filepath.name)) # Trạng thái English
            
        except Exception as e:
            self.task_store.log(f"❌ [{job_id}] Lỗi: {e}")
            if not is_retry:
                self.pending_errors.append((idx, prompt))
                self.error_prompts += 1
                self.task_store.update_item_status(idx, "Error") # Trạng thái English
            else:
                self.task_store.update_item_status(idx, "Error (Retried)") # Trạng thái English
            
        finally:
            self.task_store.update_progress(self.completed_prompts, self.error_prompts)
            try:
                page.keyboard.press("Escape")
                time.sleep(1)
                page.locator(PROMPT_BOX).fill("")
            except:
                pass
            
            self.task_store.log("⏳ Chờ 30 giây trước khi bắt đầu tác vụ tiếp theo...") 
            time.sleep(30)
            

    def run(self):
        self.task_store.log("P2V Worker started.")
        self.task_store.update_progress(self.completed_prompts, self.error_prompts)
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True) 
            context = browser.new_context()
            
            try:
                context.add_cookies(self.cookies)
            except Exception as e:
                self.task_store.log(f"⚠️ Cookie lỗi: {e}. Worker dừng.")
                self.task_store.set_final_status("Error (Cookie)")
                return
            
            page = context.new_page()
            try:
                self.task_store.log(f"🌐 Đang mở trang web: {FLOW_URL}")
                page.goto(FLOW_URL, timeout=120000)
                page.locator(f"xpath={P2V_GENERATE_BTN_XPATH}").wait_for(timeout=60000)
                self.task_store.log("✅ Trang web đã tải xong.")

                # Lấy Auth Token
                with page.expect_response(lambda resp: "batchAsyncGenerateVideoText" in resp.url, timeout=120000) as response_info:
                    page.locator(PROMPT_BOX).fill("test")
                    page.locator(f"xpath={P2V_GENERATE_BTN_XPATH}").click()
                
                request = response_info.value.request
                auth_header = request.headers.get("authorization")
                if auth_header and auth_header.startswith("Bearer "):
                    self.auth_token = auth_header
                    self.task_store.log("🔑 Đã lấy được Authorization Token.")
                
            except Exception as e:
                self.task_store.log(f"❌ Khởi tạo thất bại: {e}")
                self.task_store.set_final_status("Error (Init)")
                browser.close()
                return
            
            # --- LOGIC CHẠY VÀ XỬ LÝ TẠM DỪNG (STOPPED) ---
            for idx, prompt in enumerate(self.prompts):
                if self.task_store.stop_requested():
                    self.task_store.log("⏸️ Tác vụ bị dừng bởi người dùng. Đánh dấu các tác vụ còn lại là Tạm dừng.")
                    # Đánh dấu các tác vụ còn lại là Tạm dừng
                    for remaining_idx in range(idx, self.total_prompts):
                        # Chỉ đánh dấu nếu item chưa được xử lý (trạng thái ban đầu là Pending)
                        if self.task_store.tasks_db[self.task_id]['items'][remaining_idx]['status'] == "Pending":
                            self.task_store.update_item_status(remaining_idx, "Stopped") 
                    break
                
                job_id = f"prompt_{idx+1}_{uuid.uuid4().hex[:6]}"
                self._process_prompt(page, idx, prompt, job_id)
            # --- END LOGIC XỬ LÝ TẠM DỪNG ---

            # Logic Retry (chỉ chạy retry nếu không bị dừng bởi người dùng)
            if not self.task_store.stop_requested() and self.pending_errors:
                self.task_store.log("---")
                self.task_store.log("🔁 Bắt đầu chạy lại các tác vụ lỗi...")

                error_list = list(self.pending_errors)
                self.pending_errors.clear()

                for original_idx, prompt_text in error_list:
                    if self.task_store.stop_requested(): break
                    job_id = f"retry_{original_idx+1}_{uuid.uuid4().hex[:6]}"
                    self._process_prompt(page, original_idx, prompt_text, job_id, is_retry=True)

            browser.close()
            if self.task_store.stop_requested():
                 self.task_store.set_final_status("Stopped")
            else:
                 self.task_store.set_final_status("Finished")
            self.task_store.log("✅ Tất cả tác vụ P2V đã hoàn thành.")

# --- I2V WORKER (Cập nhật logic run tương tự) ---
class I2VWorker(BaseWorker):
    def __init__(self, task_id, tasks_db, params):
        super().__init__(task_id, tasks_db, params, is_i2v=True)
        self.tasks = params['tasks']
        
    def _process_task(self, page, idx, image_path, prompt, job_id, is_retry=False):
        # ... (Logic _process_task giữ nguyên, chỉ đảm bảo update_item_status dùng English status) ...
        image_file = str(image_path)
        video_url = None

        try:
            self.task_store.update_item_status(idx, "Running") 
            
            page.locator(I2V_UPLOAD_BTN).click()
            time.sleep(10)
            
            upload_desktop_locator = page.locator(f"xpath={I2V_SELECT_FROM_DESKTOP_BTN_XPATH}")
            upload_desktop_locator.wait_for(timeout=10000) 
            
            with page.expect_file_chooser(timeout=35000) as fc_info:
                upload_desktop_locator.click()
                
            file_chooser = fc_info.value
            file_chooser.set_files(image_file) 
            self.task_store.log(f"🖼️ [{job_id}] Đã tải lên ảnh từ Server: {Path(image_file).name}")
            
            crop_save_locator = page.locator(I2V_CROP_AND_SAVE_BTN)
            crop_save_locator.wait_for(timeout=30000) 
            crop_save_locator.click()
            
            time.sleep(20)
            self.task_store.log(f"✂️ [{job_id}] Đã click Cắt và lưu. Chờ 20s...")

            page.locator(PROMPT_BOX).wait_for(timeout=10000)
            
            page.locator(PROMPT_BOX).fill(prompt)
            self.task_store.log(f"📝 [{job_id}] Đã nhập prompt: {prompt[:100]}...")
            
            generate_locator = page.locator(I2V_GENERATE_BTN)
            wait_for_generate_button(generate_locator, job_id, self.task_store)
            
            with page.expect_response(lambda resp: "batchAsyncGenerateVideoStartImage" in resp.url and resp.status == 200, timeout=120000) as response_info:
                generate_locator.click() 
            
            time.sleep(10)
            response = response_info.value
            request = response.request
            auth_header = request.headers.get("authorization")
            if auth_header and auth_header.startswith("Bearer "):
                self.auth_token = auth_header
            
            data = response.json()
            op_data = data["operations"][0].get("operation")
            original_op_id = op_data["name"]

            self.task_store.log(f"🔑 [{job_id}] Đã lấy operation id gốc: {original_op_id}")

            video_url_original = poll_status(self.auth_token, original_op_id, job_id, self.task_store)

            filename_prefix = f"I2V_{sanitize_filename(prompt)}_{Path(image_path).stem}"
            filename = f"I2V_720p_{filename_prefix}_{job_id}.mp4" 

            if self.resolution == "1080p":
                try:
                    video_url, filename = self._upscale_and_download(
                        page, filename_prefix, job_id, original_op_id
                    )
                except Exception as upscale_e:
                    self.task_store.log(f"⚠️ [{job_id}] Lỗi Upscaling. Tải video gốc 720p...")
                    video_url = video_url_original
            else:
                video_url = video_url_original

            filepath = self.save_dir / filename
            self.task_store.log(f"⬇️ [{job_id}] Đang tải video về: {filepath}")
            
            with requests.get(video_url, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            self.task_store.log(f"✅ [{job_id}] Đã lưu: {filepath.name}")
            self.completed_prompts += 1
            self.task_store.update_item_status(idx, "Finished", str(filepath.name)) 
            
        except Exception as e:
            self.task_store.log(f"❌ [{job_id}] Lỗi I2V: {e}")
            if not is_retry:
                self.pending_errors.append((idx, image_path, prompt))
                self.error_prompts += 1
                self.task_store.update_item_status(idx, "Error") 
            else:
                self.task_store.update_item_status(idx, "Error (Retried)")
            
        finally:
            self.task_store.update_progress(self.completed_prompts, self.error_prompts)
            try:
                page.keyboard.press("Escape")
                time.sleep(1)
                page.locator(PROMPT_BOX).fill("")
            except:
                pass
            
            self.task_store.log("⏳ Chờ 30 giây trước khi bắt đầu tác vụ tiếp theo...") 
            time.sleep(30) 


    def run(self):
        self.task_store.log("I2V Worker started.")
        self.task_store.update_progress(self.completed_prompts, self.error_prompts)
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            
            try:
                context.add_cookies(self.cookies)
            except Exception as e:
                self.task_store.log(f"⚠️ Cookie lỗi: {e}. Worker dừng.")
                self.task_store.set_final_status("Error (Cookie)")
                return
            
            page = context.new_page()
            try:
                self.task_store.log(f"🌐 Đang mở trang web: {FLOW_URL}")
                page.goto(FLOW_URL, timeout=120000)
                
                page.locator(I2V_SELECT_WORKFLOW_BTN).wait_for(timeout=60000)
                page.locator(I2V_SELECT_WORKFLOW_BTN).click()
                time.sleep(10)
                
                page.locator(I2V_SELECT_IMG2VID_BTN).wait_for(timeout=10000)
                page.locator(I2V_SELECT_IMG2VID_BTN).click()
                time.sleep(10)
                self.task_store.log("✅ Đã chọn I2V Workflow.")

                page.locator(I2V_UPLOAD_BTN).wait_for(timeout=15000) 
                self.task_store.log("✅ Nút Upload đã sẵn sàng.")
                
            except Exception as e:
                self.task_store.log(f"❌ Khởi tạo thất bại: {e}")
                self.task_store.set_final_status("Error (Init)")
                browser.close()
                return

            for idx, (image_path, prompt) in enumerate(self.prompts_or_tasks):
                if self.task_store.stop_requested():
                    self.task_store.log("⏸️ Tác vụ bị dừng bởi người dùng. Đánh dấu các tác vụ còn lại là Tạm dừng.")
                    for remaining_idx in range(idx, self.total_prompts):
                        if self.task_store.tasks_db[self.task_id]['items'][remaining_idx]['status'] == "Pending":
                            self.task_store.update_item_status(remaining_idx, "Stopped") 
                    break
                    
                job_id = f"i2v_{idx+1}_{uuid.uuid4().hex[:6]}"
                self._process_task(page, idx, image_path, prompt, job_id)

            browser.close()
            if self.task_store.stop_requested():
                 self.task_store.set_final_status("Stopped")
            else:
                 self.task_store.set_final_status("Finished")
            self.task_store.log("✅ Tất cả tác vụ I2V đã hoàn thành.")


# --- HÀM KHỞI TẠO CHUNG ---
def start_worker(task_id, tasks_db, params):
    is_i2v = params.get('type') == 'I2V'
    WorkerClass = P2VWorker if not is_i2v else I2VWorker
    
    worker = WorkerClass(task_id, tasks_db, params)
    worker.start()
    return worker