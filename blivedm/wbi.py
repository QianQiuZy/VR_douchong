# -*- coding: utf-8 -*-
"""
WBI签名算法实现，兼容B站2025年新版getDanmuInfo接口。
"""
import hashlib
import time
import urllib.parse
import aiohttp
import re

WBI_KEYS_URL = 'https://api.bilibili.com/x/web-interface/nav'

async def get_wbi_keys(session: aiohttp.ClientSession):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    }
    async with session.get(WBI_KEYS_URL, headers=headers) as resp:
        if resp.content_type != 'application/json':
            text = await resp.text()
            raise RuntimeError(f"WBI接口返回非JSON，可能被风控或未登录，返回内容：{text[:200]}")
        data = await resp.json()
        img_url = data['data']['wbi_img']['img_url']
        sub_url = data['data']['wbi_img']['sub_url']
        img_key = img_url.split('/')[-1].split('.')[0]
        sub_key = sub_url.split('/')[-1].split('.')[0]
        return img_key, sub_key

# 见 https://github.com/SocialSisterYi/bilibili-API-collect/blob/master/docs/misc/sign/wbi.md
MIXIN_KEY_ENC_TAB = [46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52]
def get_mixin_key(orig: str) -> str:
    return ''.join([orig[t] for t in MIXIN_KEY_ENC_TAB])[:32]

def wbi_sign(params: dict, img_key: str, sub_key: str) -> dict:
    params = params.copy()
    params['wts'] = int(time.time())
    mixin_key = get_mixin_key(img_key + sub_key)
    # 过滤特殊字符
    for k, v in params.items():
        if isinstance(v, str):
            params[k] = re.sub(r"[!'()*]", '', v)
    # 按key升序
    items = sorted(params.items())
    query = urllib.parse.urlencode(items)
    w_rid = hashlib.md5((query + mixin_key).encode('utf-8')).hexdigest()
    params['w_rid'] = w_rid
    return params
