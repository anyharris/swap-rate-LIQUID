import requests


class RestfulAPI():
    def _get(self, uri, headers=None, params=None):
        response = requests.get(uri, headers=headers, params=params)
        return response


class Liquid(RestfulAPI):
    API_HOST = "https://api.liquid.com"
    HEADERS = {
        'X-Quoine-API-Version': '2',
        'Accept': 'application/json'
    }

    def __init__(self, api_key, api_secret):
        self.API_KEY = api_key
        self.API_SECRET = api_secret

    def get_product(self, product_id):
        path = f'/products/{product_id}'
        uri = self.API_HOST + path
        response = self._get(uri, headers=self.HEADERS)
        return response


class Telegram(RestfulAPI):
    API_HOST = "https://api.telegram.org"

    def __init__(self, chat_id, bot_token):
        self.CHAT_ID = chat_id
        self.BOT_TOKEN = bot_token

    def send_message(self, msg):
        path = f'/bot{self.BOT_TOKEN}/sendMessage'
        uri = self.API_HOST + path
        params = {
            'chat_id': self.CHAT_ID,
            'text': msg,
            'parse_mode': 'markdown'
        }
        response = self._get(uri, params=params)
        return response
