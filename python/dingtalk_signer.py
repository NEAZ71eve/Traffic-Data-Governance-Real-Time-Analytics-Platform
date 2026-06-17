#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
钉钉机器人 Webhook HMAC-SHA256 签名模块

遵循钉钉官方文档:
  签名计算: base64(hmac-sha256(timestamp + "\n" + secret))
  将签名和 timestamp 追加到 webhook URL 查询参数

用法:
    from python.dingtalk_signer import DingTalkSigner
    signer = DingTalkSigner("SECabc123...")
    signed_url = signer.sign_url("https://oapi.dingtalk.com/robot/send?access_token=xxx")
"""

import base64
import hashlib
import hmac
import time
import urllib.parse


class DingTalkSigner:
    """钉钉机器人 Webhook 签名器"""

    def __init__(self, secret: str):
        if not secret:
            raise ValueError("secret 不能为空")
        self.secret = secret

    def generate_sign(self, timestamp: int = None) -> tuple:
        """生成签名
        Returns:
            (timestamp_millis, sign_string) 元组
        """
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            self.secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code).decode("utf-8"))
        return timestamp, sign

    def sign_url(self, webhook_url: str) -> str:
        """对 Webhook URL 添加签名参数
        Returns:
            带有 timestamp 和 sign 查询参数的完整 URL
        """
        timestamp, sign = self.generate_sign()
        separator = "&" if "?" in webhook_url else "?"
        return f"{webhook_url}{separator}timestamp={timestamp}&sign={sign}"


# ============================================================
# Self-test
# ============================================================
if __name__ == "__main__":
    test_secret = "SECtest123"
    signer = DingTalkSigner(test_secret)
    timestamp, sign = signer.generate_sign()
    print(f"Secret: {test_secret}")
    print(f"Timestamp: {timestamp}")
    print(f"Sign: {sign}")

    test_url = "https://oapi.dingtalk.com/robot/send?access_token=test_token"
    signed = signer.sign_url(test_url)
    print(f"\nSigned URL: {signed}")
    assert "timestamp=" in signed, "URL 缺少 timestamp 参数"
    assert "sign=" in signed, "URL 缺少 sign 参数"
    print("\n[DingTalkSigner] 自检通过")
