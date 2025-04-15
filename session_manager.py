import requests
import json
import time
import threading
import logging
from os.path import expanduser
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    filename='wq_session.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class SessionManager:
    """管理 WorldQuant Brain API 会话的类"""
    
    def __init__(self, credentials_file='brain_credentials.txt'):
        """初始化会话管理器"""
        self.credentials_file = expanduser(credentials_file)
        self.username, self.password = self._load_credentials()
        self.session = None
        self.session_created_at = None
        self.session_valid_duration = timedelta(seconds=14380)  # 设置为比4小时稍短的时间
        self._is_refreshing = False  # 刷新中标记
        self.session_condition = threading.Condition()  # 线程锁
    
    def _load_credentials(self):
        """从文件加载凭据"""
        try:
            with open(self.credentials_file) as f:
                credentials = json.load(f)
            return credentials[0], credentials[1]
        except Exception as e:
            logging.error(f"Failed to load credentials: {str(e)}")
            raise
    
    def create_session(self):
        """创建新的会话"""
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.password)
        
        response = self.session.post('https://api.worldquantbrain.com/authentication')
        
        if response.status_code in [200,201]:
            self.session_created_at = datetime.now()
            logging.info("Session created successfully")
            print("Session created successfully")
        else:
            logging.error(f"Failed to create session: {response.status_code}, {response.text}")
            raise Exception(f"Authentication failed: {response.status_code}")
        
        return self.session


    def get_session(self):
        """获取有效的会话，如果即将过期则刷新"""
        with self.session_condition:
            while self.session is None or self._is_session_expiring():
                if not self._is_refreshing:
                    # 当前线程负责刷新
                    self._is_refreshing = True
                    try:
                        self.create_session()
                        self.session_condition.notify_all()
                        return self.session
                    finally:
                        self._is_refreshing = False
                else:
                    # 其他线程等待刷新完成
                    self.session_condition.wait()
        return self.session

    
    def _is_session_expiring(self):
        """检查会话是否即将过期"""
        if self.session_created_at is None:
            return True
        
        time_elapsed = datetime.now() - self.session_created_at
        return time_elapsed.seconds > self.session_valid_duration.seconds
    
    def post(self, url, **kwargs):
        """发送 POST 请求"""
        session = self.get_session()
        response = session.post(url, **kwargs)
        return response
    
    def get(self, url, **kwargs):
        """发送 GET 请求"""
        session = self.get_session()
        response = session.get(url, **kwargs)
        return response
    
    def patch(self, url, **kwargs):
        """发送 PATCH 请求"""
        session = self.get_session()
        response = session.patch(url, **kwargs)
        return response
    
    def put(self, url, **kwargs):
        """发送 PUT 请求"""
        session = self.get_session()
        response = session.put(url, **kwargs)
        return response
    
    def delete(self, url, **kwargs):
        """发送 DELETE 请求"""
        session = self.get_session()
        response = session.delete(url, **kwargs)
        return response
    
    def request_with_retry(self, method, url, max_retries=3, **kwargs):
        """发送请求，自动处理会话过期"""
        retries = 0
        while retries < max_retries:
            try:
                session = self.get_session()
                response = session.request(method, url, **kwargs)
                
                # 检查是否是认证错误
                if response.status_code == 401:
                    logging.warning("Session expired, creating new session")
                    time.sleep(5)  # 稍等片刻再重试
                    retries += 1
                    continue
                
                return response
            
            except Exception as e:
                logging.error(f"Request failed: {str(e)}")
                retries += 1
                if retries < max_retries:
                    logging.info(f"Retrying ({retries}/{max_retries})...")
                    time.sleep(5)  # 稍等片刻再重试
                    self.create_session()  # 创建新会话后重试
                else:
                    logging.error("Max retries reached")
                    raise