import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException

class HttpTool:
    def __init__(self, retries=3, backoff_factor=0.3, retry_status_codes=(500, 502, 503, 504), timeout=10, base_url=None):
        """
        HTTP工具类初始化
        :param retries: 最大重试次数
        :param backoff_factor: 重试等待时间因子
        :param retry_status_codes: 需要重试的状态码列表
        :param timeout: 默认超时时间（秒）
        :param base_url: 默认请求地址
        """
        self.session = requests.Session()
        self.timeout = timeout
        self.base_url = base_url
        
        # 配置重试策略
        retry = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=list(retry_status_codes),
            allowed_methods=['GET', 'POST', 'PUT', 'DELETE']  # 需要重试的HTTP方法
        )
        
        # 挂载适配器
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def _request(self, method, url, **kwargs):
        """
        通用请求方法
        :param method: HTTP方法
        :param url: 请求地址
        :param kwargs: 其他请求参数
        :return: 响应对象
        """
        # 设置默认超时
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout

        try:
            full_url = self.base_url + url if self.base_url and not url.lower().startswith(("http://", "https://")) else url
            response = self.session.request(method=method, url=full_url, **kwargs)
            response.raise_for_status()  # 如果状态码不是200，抛出HTTPError异常
            return response
        except RequestException as e:
            raise Exception(f"Request failed after multiple retries: {str(e)}")

    def get(self, url, params=None, **kwargs):
        return self._request('GET', url, params=params, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        return self._request('POST', url, data=data, json=json, **kwargs)

    def put(self, url, data=None, **kwargs):
        return self._request('PUT', url, data=data, **kwargs)

    def delete(self, url, **kwargs):
        return self._request('DELETE', url, **kwargs)

# 使用示例
if __name__ == '__main__':
    http = HttpTool(retries=3, timeout=5)
    
    try:
        # GET请求示例
        response = http.get('https://api.example.com/data', params={'key': 'value'})
        print(response.json())
        
        # POST请求示例
        post_data = {'key': 'value'}
        response = http.post('https://api.example.com/submit', json=post_data)
        print(response.status_code)
        
    except Exception as e:
        print(f"请求发生错误: {str(e)}")